/*
 * gzip_decompress.c - decompress with a gzip wrapper
 *
 * Originally public domain; changes after 2016-09-07 are copyrighted.
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

use crate::gzip_constants::*;
use crate::{DeflateInput, DeflateOutput, LibdeflateDecompressor, LibdeflateError, safety_check};
use crate::decompress_utils::libdeflate_deflate_decompress;

// struct flush_buffer_data {
// 	flush_buffer_func *user_func;
// 	void *user_data;
// 	int crc;
// };
//
// static int flush_buffer_checksum(void *data, void *buffer, size_t len) {
// 	struct flush_buffer_data *fdata = (struct flush_buffer_data*)data;
// 	fdata->crc = libdeflate_crc32(fdata->crc, buffer, len);
// 	return fdata->user_func(fdata->user_data, buffer, len);
// }

pub fn libdeflate_gzip_decompress<I: DeflateInput, O: DeflateOutput>(
	d: &mut LibdeflateDecompressor,
 	in_stream: &mut I,
    out_stream: &mut O,
) -> Result<(), LibdeflateError> {

	/* ID1 */
	if in_stream.read_byte() != GZIP_ID1 {
		return Err(LibdeflateError::BadData);
	}
	/* ID2 */
	if in_stream.read_byte() != GZIP_ID2 {
		return Err(LibdeflateError::BadData);
	}
	/* CM */
	if in_stream.read_byte() != GZIP_CM_DEFLATE {
		return Err(LibdeflateError::BadData);
	}
	let flg = in_stream.read_byte();

	/* MTIME */
	safety_check!(in_stream.move_stream_pos(4));
	/* XFL */
	safety_check!(in_stream.move_stream_pos(1));
	/* OS */
	safety_check!(in_stream.move_stream_pos(1));

	if (flg & GZIP_FRESERVED) != 0 {
		return Err(LibdeflateError::BadData);
	}

	/* Extra field */
	if (flg & GZIP_FEXTRA) != 0 {
		let xlen = in_stream.read_le_u16();
		safety_check!(in_stream.move_stream_pos(xlen as isize));
	}

	/* Original file name (zero terminated) */
	if (flg & GZIP_FNAME) != 0 {
		while in_stream.read_byte() != 0 {}
	}

	/* File comment (zero terminated) */
	if (flg & GZIP_FCOMMENT) != 0 {
		while in_stream.read_byte() != 0 {}
	}

	/* CRC16 for gzip header */
	if (flg & GZIP_FHCRC) != 0 {
		safety_check!(in_stream.move_stream_pos(2));
	}

	/* Compressed data  */
	libdeflate_deflate_decompress(d, in_stream, out_stream)?;

	let result = out_stream.final_flush().map_err(|_| LibdeflateError::InsufficientSpace)?;

	let gzip_crc = in_stream.read_le_u32();
	if result.crc32 != gzip_crc {
		return Err(LibdeflateError::BadData);
	}

	/* ISIZE */
	if result.written as u32 != in_stream.read_le_u32() {
		return Err(LibdeflateError::BadData);
	}

	Ok(())
}