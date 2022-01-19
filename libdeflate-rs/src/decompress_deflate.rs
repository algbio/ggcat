/*
 * decompress_template.h
 *
 * Copyright 2016 Eric Biggers
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/*
 * Each TABLEBITS number is the base-2 logarithm of the number of entries in the
 * main portion of the corresponding decode table.  Each number should be large
 * enough to ensure that for typical data, the vast majority of symbols can be
 * decoded by a direct lookup of the next TABLEBITS bits of compressed data.
 * However, this must be balanced against the fact that a larger table requires
 * more memory and requires more time to fill.
 *
 * Note: you cannot change a TABLEBITS number without also changing the
 * corresponding ENOUGH number!
 */

use crate::decompress_utils::*;
use crate::deflate_constants::*;
use crate::{DeflateInput, DeflateOutput, LibdeflateDecompressor, LibdeflateError};
use nightly_quirks::branch_pred::unlikely;
use std::mem::size_of;

pub const PRECODE_TABLEBITS: usize = 7;
pub const LITLEN_TABLEBITS: usize = 10;
pub const OFFSET_TABLEBITS: usize = 8;

pub struct OutStreamResult {
    pub written: usize,
    pub crc32: u32,
}

/*
 * Each ENOUGH number is the maximum number of decode table entries that may be
 * required for the corresponding Huffman code, including the main table and all
 * subtables.  Each number depends on three parameters:
 *
 *	(1) the maximum number of symbols in the code (DEFLATE_NUM_*_SYMS)
 *	(2) the number of main table bits (the TABLEBITS numbers defined above)
 *	(3) the maximum allowed codeword length (DEFLATE_MAX_*_CODEWORD_LEN)
 *
 * The ENOUGH numbers were computed using the utility program 'enough' from
 * zlib.  This program enumerates all possible relevant Huffman codes to find
 * the worst-case usage of decode table entries.
 */
pub const PRECODE_ENOUGH: usize = 128; /* enough 19 7 7	*/
pub const LITLEN_ENOUGH: usize = 1334; /* enough 288 10 15	*/
pub const OFFSET_ENOUGH: usize = 402; /* enough 32 8 15	*/

/*
 * Type for codeword lengths.
 */
pub type LenType = u8;

#[macro_export]
macro_rules! safety_check {
    ($cond:expr) => {
        if !$cond {
            return Err(LibdeflateError::BadData);
        }
    };
}

/*
 * The arrays aren't all needed at the same time.  'precode_lens' and
 * 'precode_decode_table' are unneeded after 'lens' has been filled.
 * Furthermore, 'lens' need not be retained after building the litlen
 * and offset decode tables.  In fact, 'lens' can be in union with
 * 'litlen_decode_table' provided that 'offset_decode_table' is separate
 * and is built first.
 */

#[derive(Copy, Clone)]
pub(crate) struct _DecStruct {
    pub(crate) lens:
        [LenType; DEFLATE_NUM_LITLEN_SYMS + DEFLATE_NUM_OFFSET_SYMS + DEFLATE_MAX_LENS_OVERRUN],
    pub(crate) precode_decode_table: [u32; PRECODE_ENOUGH],
}

/*
 * This is the actual DEFLATE decompression routine, lifted out of
 * deflate_decompress.c so that it can be compiled multiple times with different
 * target instruction sets.
 */

pub(crate) fn deflate_decompress_template<I: DeflateInput, O: DeflateOutput>(
    d: &mut LibdeflateDecompressor,
    in_stream: &mut I,
    out_stream: &mut O,
) -> Result<(), LibdeflateError> {
    let mut tmp_data = DecompressTempData {
        bitbuf: 0,
        bitsleft: 0,
        overrun_count: 0,
        is_final_block: false,
        block_type: 0,
        num_litlen_syms: 0,
        num_offset_syms: 0,
        input_stream: in_stream,
        output_stream: out_stream,
    };

    'block_done: loop {
        if tmp_data.is_final_block {
            break;
        }

        /* Starting to read the next block.  */

        const_assert!(can_ensure(1 + 2 + 5 + 5 + 4));
        ensure_bits(&mut tmp_data, 1 + 2 + 5 + 5 + 4);

        /* BFINAL: 1 bit  */
        tmp_data.is_final_block = pop_bits(&mut tmp_data, 1) != 0;

        /* BTYPE: 2 bits  */
        tmp_data.block_type = pop_bits(&mut tmp_data, 2);

        let skip_decode_tables;

        if tmp_data.block_type == DEFLATE_BLOCKTYPE_DYNAMIC_HUFFMAN {
            /* Dynamic Huffman block.  */

            /* The order in which precode lengths are stored.  */
            const DEFLATE_PRECODE_LENS_PERMUTATION: [u8; DEFLATE_NUM_PRECODE_SYMS] = [
                16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
            ];

            /* Read the codeword length counts.  */

            const_assert!(DEFLATE_NUM_LITLEN_SYMS == ((1 << 5) - 1) + 257);
            tmp_data.num_litlen_syms = (pop_bits(&mut tmp_data, 5) + 257) as usize;

            const_assert!(DEFLATE_NUM_OFFSET_SYMS == ((1 << 5) - 1) + 1);
            tmp_data.num_offset_syms = (pop_bits(&mut tmp_data, 5) + 1) as usize;

            const_assert!(DEFLATE_NUM_PRECODE_SYMS == ((1 << 4) - 1) + 4);
            let num_explicit_precode_lens = (pop_bits(&mut tmp_data, 4) + 4) as usize;

            d.static_codes_loaded = false;

            /* Read the precode codeword lengths.  */
            const_assert!(DEFLATE_MAX_PRE_CODEWORD_LEN == (1 << 3) - 1);
            for i in 0..num_explicit_precode_lens {
                ensure_bits(&mut tmp_data, 3);
                d.precode_lens[DEFLATE_PRECODE_LENS_PERMUTATION[i] as usize] =
                    pop_bits(&mut tmp_data, 3) as u8;
            }

            for i in num_explicit_precode_lens..DEFLATE_NUM_PRECODE_SYMS {
                d.precode_lens[DEFLATE_PRECODE_LENS_PERMUTATION[i] as usize] = 0;
            }

            /* Build the decode table for the precode.  */
            safety_check!(build_precode_decode_table(d));

            /* Expand the literal/length and offset codeword lengths.  */
            let mut i = 0;
            while i < tmp_data.num_litlen_syms + tmp_data.num_offset_syms {
                ensure_bits(&mut tmp_data, DEFLATE_MAX_PRE_CODEWORD_LEN + 7);

                /* (The code below assumes that the precode decode table
                 * does not have any subtables.)  */
                const_assert!(PRECODE_TABLEBITS == DEFLATE_MAX_PRE_CODEWORD_LEN);

                /* Read the next precode symbol.  */
                let entry = d.l.precode_decode_table
                    [bits(&mut tmp_data, DEFLATE_MAX_PRE_CODEWORD_LEN) as usize];
                remove_bits(&mut tmp_data, (entry & HUFFDEC_LENGTH_MASK) as usize);
                let presym = entry >> HUFFDEC_RESULT_SHIFT;

                if presym < 16 {
                    /* Explicit codeword length  */
                    d.l.lens[i] = presym as LenType;
                    i += 1;
                    continue;
                }

                /* Run-length encoded codeword lengths  */

                /* Note: we don't need verify that the repeat count
                 * doesn't overflow the number of elements, since we
                 * have enough extra spaces to allow for the worst-case
                 * overflow (138 zeroes when only 1 length was
                 * remaining).
                 *
                 * In the case of the small repeat counts (presyms 16
                 * and 17), it is fastest to always write the maximum
                 * number of entries.  That gets rid of branches that
                 * would otherwise be required.
                 *
                 * It is not just because of the numerical order that
                 * our checks go in the order 'presym < 16', 'presym ==
                 * 16', and 'presym == 17'.  For typical data this is
                 * ordered from most frequent to least frequent case.
                 */
                const_assert!(DEFLATE_MAX_LENS_OVERRUN == 138 - 1);

                if presym == 16 {
                    /* Repeat the previous length 3 - 6 times  */
                    safety_check!(i != 0);
                    let rep_val = d.l.lens[i - 1];
                    const_assert!(3 + ((1 << 2) - 1) == 6);
                    let rep_count = (3 + pop_bits(&mut tmp_data, 2)) as usize;
                    d.l.lens[i + 0] = rep_val;
                    d.l.lens[i + 1] = rep_val;
                    d.l.lens[i + 2] = rep_val;
                    d.l.lens[i + 3] = rep_val;
                    d.l.lens[i + 4] = rep_val;
                    d.l.lens[i + 5] = rep_val;
                    i += rep_count;
                } else if presym == 17 {
                    /* Repeat zero 3 - 10 times  */
                    const_assert!(3 + ((1 << 3) - 1) == 10);
                    let rep_count = (3 + pop_bits(&mut tmp_data, 3)) as usize;
                    d.l.lens[i + 0] = 0;
                    d.l.lens[i + 1] = 0;
                    d.l.lens[i + 2] = 0;
                    d.l.lens[i + 3] = 0;
                    d.l.lens[i + 4] = 0;
                    d.l.lens[i + 5] = 0;
                    d.l.lens[i + 6] = 0;
                    d.l.lens[i + 7] = 0;
                    d.l.lens[i + 8] = 0;
                    d.l.lens[i + 9] = 0;
                    i += rep_count;
                } else {
                    /* Repeat zero 11 - 138 times  */
                    const_assert!(11 + ((1 << 7) - 1) == 138);
                    let rep_count = (11 + pop_bits(&mut tmp_data, 7)) as usize;
                    d.l.lens[i..(i + rep_count)].fill(0);
                    i += rep_count;
                }
            }
            skip_decode_tables = false;
        } else if tmp_data.block_type == DEFLATE_BLOCKTYPE_UNCOMPRESSED {
            /* Uncompressed block: copy 'len' bytes literally from the input
             * buffer to the output buffer.  */

            align_input(&mut tmp_data)?;

            safety_check!(tmp_data.input_stream.ensure_length(4));

            let len = unsafe { read_u16(&mut tmp_data) };
            let nlen = unsafe { read_u16(&mut tmp_data) };

            safety_check!(len == !nlen);

            safety_check!(tmp_data
                .input_stream
                .read_exact_into(tmp_data.output_stream, len as usize));

            continue 'block_done;
        } else {
            safety_check!(tmp_data.block_type == DEFLATE_BLOCKTYPE_STATIC_HUFFMAN);

            /*
             * Static Huffman block: build the decode tables for the static
             * codes.  Skip doing so if the tables are already set up from
             * an earlier static block; this speeds up decompression of
             * degenerate input of many empty or very short static blocks.
             *
             * Afterwards, the remainder is the same as decompressing a
             * dynamic Huffman block.
             */

            skip_decode_tables = d.static_codes_loaded;

            if !d.static_codes_loaded {
                d.static_codes_loaded = true;

                const_assert!(DEFLATE_NUM_LITLEN_SYMS == 288);
                const_assert!(DEFLATE_NUM_OFFSET_SYMS == 32);

                for i in 0..144 {
                    d.l.lens[i] = 8;
                }
                for i in 144..256 {
                    d.l.lens[i] = 9;
                }
                for i in 256..280 {
                    d.l.lens[i] = 7;
                }
                for i in 280..288 {
                    d.l.lens[i] = 8;
                }

                for i in 288..(288 + 32) {
                    d.l.lens[i] = 5;
                }

                tmp_data.num_litlen_syms = DEFLATE_NUM_LITLEN_SYMS;
                tmp_data.num_offset_syms = DEFLATE_NUM_OFFSET_SYMS;
            }
        }

        /* Decompressing a Huffman block (either dynamic or static)  */
        if !skip_decode_tables {
            safety_check!(build_offset_decode_table(
                d,
                tmp_data.num_litlen_syms,
                tmp_data.num_offset_syms,
            ));
            safety_check!(build_litlen_decode_table(
                d,
                tmp_data.num_litlen_syms,
                tmp_data.num_offset_syms,
            ));
        }

        /* The main DEFLATE decode loop  */
        while tmp_data.overrun_count < (size_of::<usize>() / 8) {
            /* Decode a litlen symbol.  */
            ensure_bits(&mut tmp_data, DEFLATE_MAX_LITLEN_CODEWORD_LEN);
            let mut entry = d.litlen_decode_table[bits(&mut tmp_data, LITLEN_TABLEBITS) as usize];
            if (entry & HUFFDEC_SUBTABLE_POINTER) != 0 {
                /* Litlen subtable required (uncommon case)  */
                remove_bits(&mut tmp_data, LITLEN_TABLEBITS);
                entry = d.litlen_decode_table[(((entry >> HUFFDEC_RESULT_SHIFT) & 0xFFFF)
                    + bits(&mut tmp_data, (entry & HUFFDEC_LENGTH_MASK) as usize))
                    as usize];
            }
            remove_bits(&mut tmp_data, (entry & HUFFDEC_LENGTH_MASK) as usize);
            if (entry & HUFFDEC_LITERAL) != 0 {
                /* Literal  */
                if !tmp_data
                    .output_stream
                    .write(&((entry >> HUFFDEC_RESULT_SHIFT) as u8).to_ne_bytes())
                {
                    return Err(LibdeflateError::InsufficientSpace);
                }
                continue;
            }

            /* Match or end-of-block  */

            entry >>= HUFFDEC_RESULT_SHIFT;
            ensure_bits(&mut tmp_data, MAX_ENSURE);

            /* Pop the extra length bits and add them to the length base to
             * produce the full length.  */
            let length = (entry >> HUFFDEC_LENGTH_BASE_SHIFT)
                + pop_bits(
                    &mut tmp_data,
                    (entry & HUFFDEC_EXTRA_LENGTH_BITS_MASK) as usize,
                );

            /* The match destination must not end after the end of the
             * output buffer.  For efficiency, combine this check with the
             * end-of-block check.  We're using 0 for the special
             * end-of-block length, so subtract 1 and it turn it into
             * SIZE_MAX.  */
            const_assert!(HUFFDEC_END_OF_BLOCK_LENGTH == 0);
            if unlikely(length == HUFFDEC_END_OF_BLOCK_LENGTH) {
                continue 'block_done;
            }

            /* Decode the match offset.  */

            entry = d.offset_decode_table[bits(&mut tmp_data, OFFSET_TABLEBITS) as usize];
            if (entry & HUFFDEC_SUBTABLE_POINTER) != 0 {
                /* Offset subtable required (uncommon case)  */
                remove_bits(&mut tmp_data, OFFSET_TABLEBITS);
                entry = d.offset_decode_table[(((entry >> HUFFDEC_RESULT_SHIFT) & 0xFFFF)
                    + bits(&mut tmp_data, (entry & HUFFDEC_LENGTH_MASK) as usize))
                    as usize];
            }
            remove_bits(&mut tmp_data, (entry & HUFFDEC_LENGTH_MASK) as usize);
            entry >>= HUFFDEC_RESULT_SHIFT;

            const_assert!(
                can_ensure(DEFLATE_MAX_EXTRA_LENGTH_BITS + DEFLATE_MAX_OFFSET_CODEWORD_LEN)
                    && can_ensure(DEFLATE_MAX_EXTRA_OFFSET_BITS)
            );
            if !can_ensure(
                DEFLATE_MAX_EXTRA_LENGTH_BITS
                    + DEFLATE_MAX_OFFSET_CODEWORD_LEN
                    + DEFLATE_MAX_EXTRA_OFFSET_BITS,
            ) {
                ensure_bits(&mut tmp_data, DEFLATE_MAX_EXTRA_OFFSET_BITS);
            }

            /* Pop the extra offset bits and add them to the offset base to
             * produce the full offset.  */
            let offset = (entry & HUFFDEC_OFFSET_BASE_MASK)
                + pop_bits(
                    &mut tmp_data,
                    (entry >> HUFFDEC_EXTRA_OFFSET_BITS_SHIFT) as usize,
                );

            /*
             * Copy the match: 'length' bytes at 'out_next - offset' to
             * 'out_next', possibly overlapping.  If the match doesn't end
             * too close to the end of the buffer and offset >= WORDBYTES ||
             * offset == 1, take a fast path which copies a word at a time
             * -- potentially more than the length of the match, but that's
             * fine as long as we check for enough extra space.
             *
             * The remaining cases are not performance-critical so are
             * handled by a simple byte-by-byte copy.
             */

            /* The match source must not begin before the beginning of the
             * output buffer.  */

            safety_check!(tmp_data
                .output_stream
                .copy_forward(offset as usize, length as usize));
        }
    }

    /* That was the last block.  */

    /* Discard any readahead bits and check for excessive overread */
    align_input(&mut tmp_data)?;

    Ok(())
}
