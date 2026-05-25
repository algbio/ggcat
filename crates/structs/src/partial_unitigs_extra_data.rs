use std::ops::Range;

use byteorder::ReadBytesExt;
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use io::{
    concurrent::temp_reads::extra_data::{
        SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement,
    },
    varint::{
        VARINT_FLAGS_MAX_SIZE, VARINT_MAX_SIZE, decode_varint, decode_varint_flags, encode_varint,
        encode_varint_flags,
    },
};
use typenum::U1;

pub const INDIRECT_UNITIG_FLAG_MASK: u8 = 4;

#[derive(Clone, Debug)]
pub enum PartialUnitigMode {
    Inline,
    Indirect {
        indirection_start: usize,
        indirections_range: Range<usize>,
    },
}

#[derive(Clone, Debug)]
pub struct PartialUnitigExtraData<X: SequenceExtraDataConsecutiveCompression> {
    #[cfg(feature = "support_kmer_counters")]
    pub counters: io::concurrent::structured_sequences::SequenceAbundance,
    pub colors: X,
    pub mode: PartialUnitigMode,
}

#[derive(Clone, Copy)]
pub struct IndirectReadInfo {
    pub offset: usize,
    pub length: usize,
}

impl<X: SequenceExtraDataConsecutiveCompression> SequenceExtraDataTempBufferManagement
    for PartialUnitigExtraData<X>
{
    type TempBuffer = (X::TempBuffer, Vec<IndirectReadInfo>);

    fn new_temp_buffer() -> Self::TempBuffer {
        (
            X::new_temp_buffer(),
            Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE),
        )
    }

    fn clear_temp_buffer(buffer: &mut Self::TempBuffer) {
        X::clear_temp_buffer(&mut buffer.0);
        buffer.1.clear();
    }

    fn copy_temp_buffer(dest: &mut Self::TempBuffer, src: &Self::TempBuffer) {
        X::copy_temp_buffer(&mut dest.0, &src.0);
        dest.1.extend_from_slice(&src.1[..]);
    }

    fn copy_extra_from(extra: Self, src: &Self::TempBuffer, dst: &mut Self::TempBuffer) -> Self {
        let colors = X::copy_extra_from(extra.colors, &src.0, &mut dst.0);

        Self {
            colors,
            #[cfg(feature = "support_kmer_counters")]
            counters: extra.counters,
            mode: match extra.mode {
                PartialUnitigMode::Inline => PartialUnitigMode::Inline,
                PartialUnitigMode::Indirect {
                    indirection_start,
                    indirections_range,
                } => PartialUnitigMode::Indirect {
                    indirection_start,
                    indirections_range: {
                        let dst_range_start = dst.1.len();
                        dst.1.extend_from_slice(&src.1[indirections_range]);
                        dst_range_start..dst.1.len()
                    },
                },
            },
        }
    }
}

impl<X: SequenceExtraDataConsecutiveCompression> SequenceExtraDataConsecutiveCompression
    for PartialUnitigExtraData<X>
{
    type LastData = X::LastData;

    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        reader: &mut impl std::io::Read,
        last_data: Self::LastData,
        read_flags: u8,
    ) -> Option<Self> {
        let colors = X::decode_extended(&mut buffer.0, reader, last_data, read_flags)?;
        #[cfg(feature = "support_kmer_counters")]
        let counters = io::concurrent::structured_sequences::SequenceAbundance::decode_extended(
            &mut (),
            reader,
            (),
            read_flags,
        )?;

        Some(Self {
            colors,
            #[cfg(feature = "support_kmer_counters")]
            counters,
            mode: if read_flags & INDIRECT_UNITIG_FLAG_MASK != 0 {
                let indirection_start = decode_varint(|| reader.read_u8().ok())? as usize;
                let elcount = decode_varint(|| reader.read_u8().ok())? as usize;

                let range_start = buffer.1.len();

                for _ in 0..elcount {
                    let offset = decode_varint(|| reader.read_u8().ok())? as usize;
                    let (length, is_rc) = decode_varint_flags::<_, U1>(|| reader.read_u8().ok())?;
                    buffer.1.push(IndirectReadInfo {
                        offset,
                        length: (length << 1) as usize | is_rc as usize,
                    });
                }

                PartialUnitigMode::Indirect {
                    indirection_start,
                    indirections_range: range_start..range_start + elcount,
                }
            } else {
                PartialUnitigMode::Inline
            },
        })
    }

    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl std::io::Write,
        last_data: Self::LastData,
        sequence_length: usize,
        reverse_complement: bool,
        read_flags: u8,
    ) {
        self.colors.encode_extended(
            &buffer.0,
            writer,
            last_data,
            sequence_length,
            reverse_complement,
            read_flags,
        );
        #[cfg(feature = "support_kmer_counters")]
        self.counters.encode_extended(
            &(),
            writer,
            (),
            sequence_length,
            reverse_complement,
            read_flags,
        );

        assert_eq!(
            read_flags & INDIRECT_UNITIG_FLAG_MASK != 0,
            matches!(self.mode, PartialUnitigMode::Indirect { .. }),
        );

        match &self.mode {
            PartialUnitigMode::Indirect {
                indirection_start,
                indirections_range,
            } => {
                let elcount = indirections_range.end - indirections_range.start;

                encode_varint(
                    |b| writer.write(b).ok(),
                    if reverse_complement {
                        sequence_length - *indirection_start
                    } else {
                        *indirection_start
                    } as u64,
                )
                .unwrap();
                encode_varint(|b| writer.write(b).ok(), elcount as u64).unwrap();

                for idx in indirections_range.clone() {
                    let info = &buffer.1[idx];
                    encode_varint(|b| writer.write(b).ok(), info.offset as u64).unwrap();
                    encode_varint_flags::<_, _, U1>(
                        |b| writer.write(b).ok(),
                        (info.length >> 1) as u64,
                        (info.length & 0x1) as u8 ^ reverse_complement as u8,
                    )
                    .unwrap();
                }
            }
            PartialUnitigMode::Inline => {}
        }
    }

    fn obtain_last_data(
        &self,
        last_data: Self::LastData,
        reverse_complement: bool,
    ) -> Self::LastData {
        self.colors.obtain_last_data(last_data, reverse_complement)
    }

    fn max_size(&self) -> usize {
        self.colors.max_size()
            + match () {
                #[cfg(feature = "support_kmer_counters")]
                () => self.counters.max_size(),
                #[cfg(not(feature = "support_kmer_counters"))]
                () => 0,
            }
            + match &self.mode {
                PartialUnitigMode::Inline => 0,
                PartialUnitigMode::Indirect {
                    indirections_range, ..
                } => {
                    VARINT_MAX_SIZE * 2
                        + (VARINT_MAX_SIZE + VARINT_FLAGS_MAX_SIZE) * indirections_range.len()
                }
            }
    }
}
