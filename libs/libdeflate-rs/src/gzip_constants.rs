#![allow(dead_code)]
/*
 * gzip_constants.h - constants for the gzip wrapper format
 */

pub const GZIP_MIN_HEADER_SIZE: usize = 10;
pub const GZIP_FOOTER_SIZE: usize = 8;
pub const GZIP_MIN_OVERHEAD: usize = GZIP_MIN_HEADER_SIZE + GZIP_FOOTER_SIZE;

pub const GZIP_ID1: u8 = 0x1F;
pub const GZIP_ID2: u8 = 0x8B;

pub const GZIP_CM_DEFLATE: u8 = 8;

pub const GZIP_FTEXT: u8 = 0x01;
pub const GZIP_FHCRC: u8 = 0x02;
pub const GZIP_FEXTRA: u8 = 0x04;
pub const GZIP_FNAME: u8 = 0x08;
pub const GZIP_FCOMMENT: u8 = 0x10;
pub const GZIP_FRESERVED: u8 = 0xE0;

pub const GZIP_MTIME_UNAVAILABLE: usize = 0;

pub const GZIP_XFL_SLOWEST_COMPRESSION: usize = 0x02;
pub const GZIP_XFL_FASTEST_COMPRESSION: usize = 0x04;

pub const GZIP_OS_FAT: usize = 0;
pub const GZIP_OS_AMIGA: usize = 1;
pub const GZIP_OS_VMS: usize = 2;
pub const GZIP_OS_UNIX: usize = 3;
pub const GZIP_OS_VM_CMS: usize = 4;
pub const GZIP_OS_ATARI_TOS: usize = 5;
pub const GZIP_OS_HPFS: usize = 6;
pub const GZIP_OS_MACINTOSH: usize = 7;
pub const GZIP_OS_Z_SYSTEM: usize = 8;
pub const GZIP_OS_CP_M: usize = 9;
pub const GZIP_OS_TOPS_20: usize = 10;
pub const GZIP_OS_NTFS: usize = 11;
pub const GZIP_OS_QDOS: usize = 12;
pub const GZIP_OS_RISCOS: usize = 13;
pub const GZIP_OS_UNKNOWN: usize = 255;
