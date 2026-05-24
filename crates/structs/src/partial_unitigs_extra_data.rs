use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement,
};

pub const INDIRECT_UNITIG_FLAG_MASK: u8 = 4;

#[derive(Clone, Debug)]
pub enum PartialUnitigExtraData<X: SequenceExtraDataConsecutiveCompression> {
    Indirect {
        #[cfg(feature = "support_kmer_counters")]
        counters: io::concurrent::structured_sequences::SequenceAbundance,
    },
    Inline {
        #[cfg(feature = "support_kmer_counters")]
        counters: io::concurrent::structured_sequences::SequenceAbundance,
        colors: X,
    },
}

impl<X: SequenceExtraDataConsecutiveCompression> SequenceExtraDataTempBufferManagement
    for PartialUnitigExtraData<X>
{
    type TempBuffer = X::TempBuffer;

    fn new_temp_buffer() -> Self::TempBuffer {
        X::new_temp_buffer()
    }

    fn clear_temp_buffer(buffer: &mut Self::TempBuffer) {
        X::clear_temp_buffer(buffer)
    }

    fn copy_temp_buffer(dest: &mut Self::TempBuffer, src: &Self::TempBuffer) {
        X::copy_temp_buffer(dest, src)
    }

    fn copy_extra_from(extra: Self, src: &Self::TempBuffer, dst: &mut Self::TempBuffer) -> Self {
        match extra {
            Self::Indirect {
                #[cfg(feature = "support_kmer_counters")]
                counters,
            } => {
                todo!()
            }
            Self::Inline {
                #[cfg(feature = "support_kmer_counters")]
                counters,
                colors,
            } => Self::Inline {
                #[cfg(feature = "support_kmer_counters")]
                counters: counters,
                colors: X::copy_extra_from(colors, src, dst),
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
        let color = X::decode_extended(buffer, reader, last_data, read_flags)?;

        Some(if read_flags & INDIRECT_UNITIG_FLAG_MASK != 0 {
            Self::Indirect {
                #[cfg(feature = "support_kmer_counters")]
                counters: todo!(),
            }
        } else {
            #[cfg(feature = "support_kmer_counters")]
            let counter = io::concurrent::structured_sequences::SequenceAbundance::decode_extended(
                &mut (),
                reader,
                (),
                read_flags,
            )?;
            Self::Inline {
                colors: color,
                #[cfg(feature = "support_kmer_counters")]
                counters: counter,
            }
        })
    }

    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl std::io::Write,
        last_data: Self::LastData,
        reverse_complement: bool,
        read_flags: u8,
    ) {
        if read_flags & INDIRECT_UNITIG_FLAG_MASK != 0 {
            unimplemented!()
        }

        match self {
            PartialUnitigExtraData::Indirect {
                #[cfg(feature = "support_kmer_counters")]
                counters,
            } => todo!(),
            PartialUnitigExtraData::Inline {
                colors,
                #[cfg(feature = "support_kmer_counters")]
                counters,
            } => {
                colors.encode_extended(buffer, writer, last_data, reverse_complement, read_flags);
                #[cfg(feature = "support_kmer_counters")]
                counters.encode_extended(&(), writer, (), reverse_complement, read_flags);
            }
        }
    }

    fn obtain_last_data(
        &self,
        last_data: Self::LastData,
        reverse_complement: bool,
    ) -> Self::LastData {
        match self {
            PartialUnitigExtraData::Indirect { .. } => Default::default(),
            PartialUnitigExtraData::Inline { colors, .. } => {
                colors.obtain_last_data(last_data, reverse_complement)
            }
        }
    }

    fn max_size(&self) -> usize {
        match self {
            PartialUnitigExtraData::Indirect {
                #[cfg(feature = "support_kmer_counters")]
                counters,
            } => todo!(),
            PartialUnitigExtraData::Inline {
                colors,
                #[cfg(feature = "support_kmer_counters")]
                counters,
            } => {
                colors.max_size()
                    + match () {
                        #[cfg(feature = "support_kmer_counters")]
                        () => counters.max_size(),
                        #[cfg(not(feature = "support_kmer_counters"))]
                        () => 0,
                    }
            }
        }
    }
}
