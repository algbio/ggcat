use std::intrinsics::likely;
use crate::deflate_constants::DEFLATE_MIN_MATCH_LEN;

const WORD_BYTES: usize = std::mem::size_of::<usize>();

#[inline(always)]
unsafe fn copy_word_unaligned(src: *const u8, dst: *mut u8) {
    std::ptr::write_unaligned(dst as *mut usize, std::ptr::read_unaligned(src as *const usize));
}

#[inline(always)]
pub unsafe fn copy_rolling(
    mut dst: *mut u8,
    dst_end: *const u8,
    offset: usize,
    has_space: bool) {

    let mut src = dst.offset(-(offset as isize)) as *const u8;

    if
        /* max overrun is writing 3 words for a min length match */
        likely(has_space) {
        if offset >= WORD_BYTES {
            /* words don't overlap? */
            copy_word_unaligned(src, dst);
            src = src.add(WORD_BYTES);
            dst = dst.add(WORD_BYTES);
            copy_word_unaligned(src, dst);
            src = src.add(WORD_BYTES);
            dst = dst.add(WORD_BYTES);
            loop {
                copy_word_unaligned(src, dst);
                src = src.add(WORD_BYTES);
                dst = dst.add(WORD_BYTES);
                if dst as usize >= dst_end as usize {
                    break;
                }
            }
        } else if offset == 1 {
            /* RLE encoding of previous byte, common if the
             * data contains many repeated bytes */
            let v = usize::from_ne_bytes([*src; std::mem::size_of::<usize>()]);

            std::ptr::write_unaligned(dst as *mut usize, v);
            dst = dst.add(WORD_BYTES);
            std::ptr::write_unaligned(dst as *mut usize, v);
            dst = dst.add(WORD_BYTES);
            loop {
                std::ptr::write_unaligned(dst as *mut usize, v);
                dst = dst.add(WORD_BYTES);
                if dst as usize >= dst_end as usize {
                    break;
                }
            }
        } else {
            *dst = *src;
            src = src.add(1);
            dst = dst.add(1);

            *dst = *src;
            src = src.add(1);
            dst = dst.add(1);
            loop {
                *dst = *src;
                src = src.add(1);
                dst = dst.add(1);
                if dst as usize >= dst_end as usize {
                    break;
                }
            }
        }
    } else {
        const_assert!(DEFLATE_MIN_MATCH_LEN == 3);
        *dst = *src;
        src = src.add(1);
        dst = dst.add(1);
        loop {
            *dst = *src;
            src = src.add(1);
            dst = dst.add(1);
            if dst as usize >= dst_end as usize {
                break;
            }
        }
    }

}
