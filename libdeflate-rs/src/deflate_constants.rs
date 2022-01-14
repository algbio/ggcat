/*
 * deflate_constants.h - constants for the DEFLATE compression format
 */

/* Valid block types  */
pub const DEFLATE_BLOCKTYPE_UNCOMPRESSED: u32 = 0;
pub const DEFLATE_BLOCKTYPE_STATIC_HUFFMAN: u32 = 1;
pub const DEFLATE_BLOCKTYPE_DYNAMIC_HUFFMAN: u32 = 2;

/* Minimum and maximum supported match lengths (in bytes)  */
pub const DEFLATE_MIN_MATCH_LEN: usize = 3;
pub const DEFLATE_MAX_MATCH_LEN: usize = 258;

/* Minimum and maximum supported match offsets (in bytes)  */
pub const DEFLATE_MIN_MATCH_OFFSET: usize = 1;
pub const DEFLATE_MAX_MATCH_OFFSET: usize = 32768;

pub const DEFLATE_MAX_WINDOW_SIZE: usize = 32768;

/* Number of symbols in each Huffman code.  Note: for the literal/length
 * and offset codes, these are actually the maximum values; a given block
 * might use fewer symbols.  */
pub const DEFLATE_NUM_PRECODE_SYMS: usize = 19;
pub const DEFLATE_NUM_LITLEN_SYMS: usize = 288;
pub const DEFLATE_NUM_OFFSET_SYMS: usize = 32;

/* The maximum number of symbols across all codes  */
pub const DEFLATE_MAX_NUM_SYMS: usize = 288;

/* Division of symbols in the literal/length code  */
pub const DEFLATE_NUM_LITERALS: usize = 256;
pub const DEFLATE_END_OF_BLOCK: usize = 256;
pub const DEFLATE_NUM_LEN_SYMS: usize = 31;

/* Maximum codeword length, in bits, within each Huffman code  */
pub const DEFLATE_MAX_PRE_CODEWORD_LEN: usize = 7;
pub const DEFLATE_MAX_LITLEN_CODEWORD_LEN: usize = 15;
pub const DEFLATE_MAX_OFFSET_CODEWORD_LEN: usize = 15;

/* The maximum codeword length across all codes  */
pub const DEFLATE_MAX_CODEWORD_LEN: usize = 15;

/* Maximum possible overrun when decoding codeword lengths  */
pub const DEFLATE_MAX_LENS_OVERRUN: usize = 137;

/*
 * Maximum number of extra bits that may be required to represent a match
 * length or offset.
 *
 * TODO: are we going to have full DEFLATE64 support?  If so, up to 16
 * length bits must be supported.
 */
pub const DEFLATE_MAX_EXTRA_LENGTH_BITS: usize = 5;
pub const DEFLATE_MAX_EXTRA_OFFSET_BITS: usize = 14;

/* The maximum number of bits in which a match can be represented.  This
 * is the absolute worst case, which assumes the longest possible Huffman
 * codewords and the maximum numbers of extra bits.  */
pub const DEFLATE_MAX_MATCH_BITS: usize = (DEFLATE_MAX_LITLEN_CODEWORD_LEN
    + DEFLATE_MAX_EXTRA_LENGTH_BITS
    + DEFLATE_MAX_OFFSET_CODEWORD_LEN
    + DEFLATE_MAX_EXTRA_OFFSET_BITS);
