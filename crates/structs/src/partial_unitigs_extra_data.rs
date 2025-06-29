use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement,
};

#[derive(Clone, Debug)]
pub struct PartialUnitigExtraData<X: SequenceExtraDataConsecutiveCompression> {
    #[cfg(feature = "support_kmer_counters")]
    pub counters: UnitigsCounters,
    pub colors: X,
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
        Self {
            colors: X::copy_extra_from(extra.colors, src, dst),
            #[cfg(feature = "support_kmer_counters")]
            counters: extra.counters,
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
    ) -> Option<Self> {
        let color = X::decode_extended(buffer, reader, last_data)?;
        #[cfg(feature = "support_kmer_counters")]
        let counter = UnitigsCounters::decode_extended(&mut (), reader, ())?;
        Some(Self {
            colors: color,
            #[cfg(feature = "support_kmer_counters")]
            counters: counter,
        })
    }

    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl std::io::Write,
        last_data: Self::LastData,
    ) {
        self.colors.encode_extended(buffer, writer, last_data);
        #[cfg(feature = "support_kmer_counters")]
        self.counters.encode_extended(&(), writer, ());
    }

    fn obtain_last_data(&self, last_data: Self::LastData) -> Self::LastData {
        self.colors.obtain_last_data(last_data)
    }

    fn max_size(&self) -> usize {
        self.colors.max_size()
            + match () {
                #[cfg(feature = "support_kmer_counters")]
                () => self.counters.max_size(),
                #[cfg(not(feature = "support_kmer_counters"))]
                () => 0,
            }
    }
}
