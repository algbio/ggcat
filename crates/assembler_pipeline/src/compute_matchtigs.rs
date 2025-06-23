use crate::maximal_unitig_links::maximal_unitig_index::DoubleMaximalUnitigLinks;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use colors::colors_manager::{ColorsManager, ColorsMergeManager, color_types};
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use crossbeam::channel::{Receiver, Sender};
use genome_graph::bigraph::implementation::node_bigraph_wrapper::NodeBigraphWrapper;
use genome_graph::bigraph::interface::BidirectedData;
use genome_graph::bigraph::traitgraph::implementation::petgraph_impl::PetGraph;
use genome_graph::bigraph::traitgraph::interface::ImmutableGraphContainer;
use genome_graph::bigraph::traitgraph::interface::MutableGraphContainer;
use genome_graph::generic::{GenericEdge, GenericNode};
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::structured_sequences::concurrent::FastaWriterConcurrentBuffer;
use io::concurrent::structured_sequences::{
    IdentSequenceWriter, SequenceAbundanceType, StructuredSequenceBackend, StructuredSequenceWriter,
};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use libmatchtigs::{
    EulertigAlgorithm, EulertigAlgorithmConfiguration, MatchtigEdgeData, PathtigAlgorithm,
};
use libmatchtigs::{GreedytigAlgorithm, GreedytigAlgorithmConfiguration, TigAlgorithm};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::fmt::Debug;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use traitgraph_algo::dijkstra::DijkstraWeightedEdgeData;

#[cfg(feature = "support_kmer_counters")]
use io::concurrent::structured_sequences::SequenceAbundance;

const DUMMY_EDGE_VALUE: usize = usize::MAX;

#[derive(Clone)]
struct SequenceHandle<ColorInfo: IdentSequenceWriter>(
    Option<Arc<StructuredUnitigsStorage<ColorInfo>>>,
    usize,
);

impl<ColorInfo: IdentSequenceWriter> Debug for SequenceHandle<ColorInfo> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SequenceHandle").field(&self.1).finish()
    }
}

impl<ColorInfo: IdentSequenceWriter> SequenceHandle<ColorInfo> {
    fn get_sequence_handle(
        &self,
    ) -> Option<(
        &(
            CompressedReadIndipendent,
            ColorInfo,
            SequenceAbundanceType,
            DoubleMaximalUnitigLinks,
        ),
        &StructuredUnitigsStorage<ColorInfo>,
    )> {
        self.0
            .as_ref()
            .map(|s| (&s.sequences[self.1 - s.first_sequence_index], s.deref()))
    }
}

impl<ColorInfo: IdentSequenceWriter> Default for SequenceHandle<ColorInfo> {
    fn default() -> Self {
        Self(None, DUMMY_EDGE_VALUE)
    }
}

impl<ColorInfo: IdentSequenceWriter> PartialEq for SequenceHandle<ColorInfo> {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}
impl<ColorInfo: IdentSequenceWriter> Eq for SequenceHandle<ColorInfo> {}

// Declare types for the graph. It may or may not make sense to have this be the same type as the iterator outputs.
#[derive(Clone, Debug)]
struct UnitigEdgeData<ColorInfo: IdentSequenceWriter> {
    sequence_handle: SequenceHandle<ColorInfo>,
    forwards: bool,
    weight: usize,
    dummy_edge_id: usize,
}

impl<ColorInfo: IdentSequenceWriter> PartialEq for UnitigEdgeData<ColorInfo> {
    fn eq(&self, other: &Self) -> bool {
        self.sequence_handle == other.sequence_handle
            && self.forwards == other.forwards
            && self.weight == other.weight
            && self.dummy_edge_id == other.dummy_edge_id
    }
}

impl<ColorInfo: IdentSequenceWriter> Eq for UnitigEdgeData<ColorInfo> {}

impl<ColorInfo: IdentSequenceWriter> BidirectedData for UnitigEdgeData<ColorInfo> {
    fn mirror(&self) -> Self {
        Self {
            sequence_handle: self.sequence_handle.clone(),
            forwards: !self.forwards,
            weight: self.weight,
            dummy_edge_id: self.dummy_edge_id,
        }
    }
}

/*impl<ColorInfo: IdentSequenceWriter> BidirectedData for UnitigEdgeData<ColorInfo> {

}

impl<ColorInfo: IdentSequenceWriter> DijkstraWeightedEdgeData<usize> for UnitigEdgeData<ColorInfo> {

}*/

// SequenceHandle is the type that points to a sequence, e.g. just an integer.
impl<ColorInfo: IdentSequenceWriter> MatchtigEdgeData<SequenceHandle<ColorInfo>>
    for UnitigEdgeData<ColorInfo>
{
    fn is_dummy(&self) -> bool {
        self.dummy_edge_id != 0
    }

    // true if this edge represents the forwards variant of a unitig, where forwards is the direction it is stored in your sequence store/file
    fn is_forwards(&self) -> bool {
        self.forwards
    }

    fn new(
        sequence_handle: SequenceHandle<ColorInfo>,
        forwards: bool,
        weight: usize,
        dummy_edge_id: usize,
    ) -> Self {
        Self {
            sequence_handle,
            forwards,
            weight,
            dummy_edge_id,
        }
    }
}

impl<ColorInfo: IdentSequenceWriter> DijkstraWeightedEdgeData<usize> for UnitigEdgeData<ColorInfo> {
    fn weight(&self) -> usize {
        self.weight
    }
}

pub struct StructuredUnitigsStorage<ColorInfo: IdentSequenceWriter> {
    first_sequence_index: usize,
    sequences: Vec<(
        CompressedReadIndipendent,
        ColorInfo,
        SequenceAbundanceType,
        DoubleMaximalUnitigLinks,
    )>,

    sequences_buffer: Vec<u8>,
    links_buffer: <DoubleMaximalUnitigLinks as SequenceExtraDataTempBufferManagement>::TempBuffer,
    color_buffer: ColorInfo::TempBuffer,
}

impl<ColorInfo: IdentSequenceWriter> StructuredUnitigsStorage<ColorInfo> {
    fn new() -> Self {
        Self {
            first_sequence_index: usize::MAX,
            sequences: vec![],
            sequences_buffer: vec![],
            links_buffer: DoubleMaximalUnitigLinks::new_temp_buffer(),
            color_buffer: ColorInfo::new_temp_buffer(),
        }
    }
}

pub struct MatchtigsStorageBackend<ColorInfo: IdentSequenceWriter> {
    sequences_channel: (
        Sender<Arc<StructuredUnitigsStorage<ColorInfo>>>,
        Receiver<Arc<StructuredUnitigsStorage<ColorInfo>>>,
    ),
}

impl<ColorInfo: IdentSequenceWriter> MatchtigsStorageBackend<ColorInfo> {
    pub fn new() -> Self {
        Self {
            sequences_channel: crossbeam::channel::unbounded(),
        }
    }

    pub fn get_receiver(&self) -> Receiver<Arc<StructuredUnitigsStorage<ColorInfo>>> {
        self.sequences_channel.1.clone()
    }
}

impl<ColorInfo: IdentSequenceWriter> StructuredSequenceBackend<ColorInfo, DoubleMaximalUnitigLinks>
    for MatchtigsStorageBackend<ColorInfo>
{
    type SequenceTempBuffer = StructuredUnitigsStorage<ColorInfo>;

    fn alloc_temp_buffer(_: usize) -> Self::SequenceTempBuffer {
        StructuredUnitigsStorage::new()
    }

    fn write_sequence(
        _k: usize,
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: &[u8],
        color_info: ColorInfo,
        links_info: DoubleMaximalUnitigLinks,
        extra_buffers: &(
            ColorInfo::TempBuffer,
            <DoubleMaximalUnitigLinks as SequenceExtraDataTempBufferManagement>::TempBuffer,
        ),
        #[cfg(feature = "support_kmer_counters")] abundance: SequenceAbundanceType,
    ) {
        if buffer.first_sequence_index == usize::MAX {
            buffer.first_sequence_index = sequence_index as usize;
        } else {
            assert_eq!(
                buffer.first_sequence_index + buffer.sequences.len(),
                sequence_index as usize
            );
        }

        let sequence =
            CompressedReadIndipendent::from_plain(sequence, &mut buffer.sequences_buffer);
        let color_info =
            ColorInfo::copy_extra_from(color_info, &extra_buffers.0, &mut buffer.color_buffer);
        let links_info = DoubleMaximalUnitigLinks::copy_extra_from(
            links_info,
            &extra_buffers.1,
            &mut buffer.links_buffer,
        );

        buffer.sequences.push((
            sequence,
            color_info,
            match () {
                #[cfg(feature = "support_kmer_counters")]
                () => abundance,
                #[cfg(not(feature = "support_kmer_counters"))]
                () => (),
            },
            links_info,
        ));
    }

    fn get_path(&self) -> PathBuf {
        unimplemented!("In memory data structure")
    }

    fn flush_temp_buffer(&mut self, buffer: &mut Self::SequenceTempBuffer) {
        self.sequences_channel
            .0
            .send(Arc::new(std::mem::replace(
                buffer,
                StructuredUnitigsStorage::new(),
            )))
            .unwrap();
    }

    fn finalize(self) {}
}

impl<ColorInfo: IdentSequenceWriter + 'static> GenericNode for UnitigEdgeData<ColorInfo> {
    fn id(&self) -> usize {
        self.sequence_handle.1
    }

    fn is_self_complemental(&self) -> bool {
        self.sequence_handle
            .get_sequence_handle()
            .map(|s| s.0.3.is_self_complemental)
            .unwrap_or(false)
    }

    fn edges(&self) -> impl Iterator<Item = GenericEdge> {
        let links = self
            .sequence_handle
            .get_sequence_handle()
            .map(|h| h.0.3.clone())
            .unwrap_or(DoubleMaximalUnitigLinks::EMPTY);
        let storage = self.sequence_handle.0.clone();

        Box::new(
            links
                .links
                .into_iter()
                .map(move |link| {
                    let storage = storage.clone();

                    link.entries.iter().map(move |entry| {
                        let entry = &storage.as_ref().unwrap().links_buffer[entry];

                        GenericEdge {
                            from_side: !entry.flags.flip_current(),
                            to_node: entry.index() as usize,
                            to_side: !entry.flags.flip_other(),
                        }
                    })
                })
                .flatten(),
        )
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum MatchtigMode {
    FastSimpliTigs,
    FastEulerTigs,
    EulerTigs,
    GreedyTigs,
    // MatchTigs,
    PathTigs,
}

pub trait MatchtigHelperTrait {
    fn needs_simplitigs(&self) -> bool;
    fn needs_temporary_tigs(&self) -> bool;
    fn needs_matchtigs_library(&self) -> bool;
    fn get_matchtigs_mode(&self) -> Self;
}

impl MatchtigHelperTrait for Option<MatchtigMode> {
    fn needs_simplitigs(&self) -> bool {
        *self == Some(MatchtigMode::FastSimpliTigs) || *self == Some(MatchtigMode::FastEulerTigs)
    }

    fn needs_temporary_tigs(&self) -> bool {
        *self == Some(MatchtigMode::EulerTigs)
            || *self == Some(MatchtigMode::GreedyTigs)
            || *self == Some(MatchtigMode::PathTigs)
            || *self == Some(MatchtigMode::FastEulerTigs)
    }

    fn needs_matchtigs_library(&self) -> bool {
        *self == Some(MatchtigMode::EulerTigs)
            || *self == Some(MatchtigMode::GreedyTigs)
            || *self == Some(MatchtigMode::PathTigs)
    }

    fn get_matchtigs_mode(&self) -> Self {
        if self.needs_matchtigs_library() {
            *self
        } else {
            None
        }
    }
}

pub fn compute_matchtigs_thread<
    CX: ColorsManager,
    BK: StructuredSequenceBackend<PartialUnitigsColorStructure<CX>, ()>,
>(
    k: usize,
    threads_count: usize,
    input_data: Receiver<Arc<StructuredUnitigsStorage<PartialUnitigsColorStructure<CX>>>>,
    out_file: &StructuredSequenceWriter<PartialUnitigsColorStructure<CX>, (), BK>,
    mode: MatchtigMode,
) {
    let iterator = input_data
        .into_iter()
        .map(|storage| {
            (0..storage.sequences.len())
                .into_iter()
                .map(move |index| UnitigEdgeData {
                    sequence_handle: SequenceHandle(
                        Some(storage.clone()),
                        storage.first_sequence_index + index,
                    ),
                    forwards: true,
                    weight: 0,
                    dummy_edge_id: 0,
                })
        })
        .flatten();

    #[cfg(feature = "support_kmer_counters")]
    {
        if matches!(mode, MatchtigMode::GreedyTigs) {
            ggcat_logging::warn!(
                "Abundancies support with greedy matchtigs is not accurate for merged unitigs!"
            );
        }
    }

    let mut graph: NodeBigraphWrapper<PetGraph<(), UnitigEdgeData<_>>> =
        genome_graph::generic::convert_generic_node_centric_bigraph_to_edge_centric::<(), _, _ ,_ ,_>(iterator)
            .unwrap();

    let phase_name = match mode {
        MatchtigMode::EulerTigs => "eulertigs",
        MatchtigMode::GreedyTigs => "greedy matchtigs",
        MatchtigMode::PathTigs => "pathtigs",
        MatchtigMode::FastSimpliTigs => unreachable!(),
        MatchtigMode::FastEulerTigs => unreachable!(),
    };

    PHASES_TIMES_MONITOR
        .write()
        .start_phase(format!("phase: {} building [step1]", phase_name));

    /* assign weight to each edge */
    for edge_index in graph.edge_indices_copied() {
        let edge_data: &mut UnitigEdgeData<_> = graph.edge_data_mut(edge_index);

        // length (in characters) of the sequence associated with edge_data
        let sequence_length = edge_data
            .sequence_handle
            .0
            .as_ref()
            .map(|s| {
                s.sequences[edge_data.sequence_handle.1 - s.first_sequence_index]
                    .0
                    .bases_count()
            })
            .unwrap();

        let weight = sequence_length + 1 - k; // number of kmers in the sequence
        edge_data.weight = weight;
    }

    let tigs = match mode {
        MatchtigMode::GreedyTigs => GreedytigAlgorithm::compute_tigs(
            &mut graph,
            &GreedytigAlgorithmConfiguration::new(threads_count, k),
        ),
        MatchtigMode::PathTigs => PathtigAlgorithm::compute_tigs(&mut graph, &()),
        // MatchtigMode::MatchTigs => {
        //     MatchtigAlgorithm::compute_tigs(
        //         &mut graph,
        //         &MatchtigAlgorithmConfiguration::new(threads_count, k),
        //     )
        // },
        MatchtigMode::EulerTigs => {
            EulertigAlgorithm::compute_tigs(&mut graph, &EulertigAlgorithmConfiguration { k })
        }
        MatchtigMode::FastSimpliTigs => unreachable!(),
        MatchtigMode::FastEulerTigs => unreachable!(),
    };

    PHASES_TIMES_MONITOR
        .write()
        .start_phase(format!("phase: {} building [step2]", phase_name));

    let mut output_buffer =
        FastaWriterConcurrentBuffer::new(&out_file, DEFAULT_OUTPUT_BUFFER_SIZE, true, k);

    let mut read_buffer = Vec::new();

    let mut final_unitig_color =
        color_types::ColorsMergeManagerType::<CX>::alloc_unitig_color_structure();
    let mut final_color_extra_buffer =
        color_types::PartialUnitigsColorStructure::<CX>::new_temp_buffer();

    for walk in tigs.iter() {
        // Reset the colors
        color_types::ColorsMergeManagerType::<CX>::reset_unitig_color_structure(
            &mut final_unitig_color,
        );
        color_types::PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
            &mut final_color_extra_buffer,
        );

        let first_edge = *walk.first().unwrap();
        let first_data = graph.edge_data(first_edge);
        // print sequence of first edge forwards or reverse complemented, depending on first_data.is_forwards()

        let (handle, storage) = match first_data.sequence_handle.get_sequence_handle() {
            Some(handle) => handle,
            None => continue,
        };

        read_buffer.clear();

        let first_sequence = handle.0.as_reference(&storage.sequences_buffer);

        // Update the sequence and its colors
        if first_data.is_forwards() {
            read_buffer.extend(first_sequence.as_bases_iter());
            CX::ColorsMergeManagerType::join_structures::<false>(
                &mut final_unitig_color,
                &handle.1,
                &storage.color_buffer,
                0,
                None,
            );
        } else {
            read_buffer.extend(first_sequence.as_reverse_complement_bases_iter());
            CX::ColorsMergeManagerType::join_structures::<true>(
                &mut final_unitig_color,
                &handle.1,
                &storage.color_buffer,
                0,
                None,
            );
        }
        #[cfg(feature = "support_kmer_counters")]
        let mut abundance = SequenceAbundance {
            first: handle.2.first,
            sum: handle.2.sum,
            last: handle.2.last,
        };

        let mut previous_data = first_data;
        for edge in walk.iter().skip(1) {
            let edge_data = graph.edge_data(*edge);

            let extra_bases = if previous_data.is_original() {
                0
            } else {
                previous_data.weight()
            };

            previous_data = edge_data;

            let (handle, storage) = match edge_data.sequence_handle.get_sequence_handle() {
                Some(handle) => handle,
                None => {
                    // The edge is dummy

                    if CX::COLORS_ENABLED {
                        // TODO: Skip colors data
                        panic!("Matchtigs are not supported with colors");
                    }
                    assert!(edge_data.is_dummy());
                    continue;
                }
            };

            assert!(!edge_data.is_dummy());

            let kmer_offset = 0;
            let bases_offset = k - 1 - extra_bases;

            // print sequence of edge, starting from character at index offset, forwards or reverse complemented, depending on edge_data.is_forwards()
            let next_sequence = handle.0.as_reference(&storage.sequences_buffer);

            if edge_data.is_forwards() {
                read_buffer.extend(next_sequence.as_bases_iter().skip(bases_offset));
                CX::ColorsMergeManagerType::join_structures::<false>(
                    &mut final_unitig_color,
                    &handle.1,
                    &storage.color_buffer,
                    kmer_offset,
                    None,
                );
                #[cfg(feature = "support_kmer_counters")]
                {
                    abundance.sum += handle.2.sum;
                    abundance.last = handle.2.last;
                }
            } else {
                read_buffer.extend(
                    next_sequence
                        .as_reverse_complement_bases_iter()
                        .skip(bases_offset),
                );
                CX::ColorsMergeManagerType::join_structures::<true>(
                    &mut final_unitig_color,
                    &handle.1,
                    &storage.color_buffer,
                    kmer_offset,
                    None,
                );
                #[cfg(feature = "support_kmer_counters")]
                {
                    abundance.sum += handle.2.sum;
                    abundance.last = handle.2.last;
                }
            }
        }

        let writable_color = color_types::ColorsMergeManagerType::<CX>::encode_part_unitigs_colors(
            &mut final_unitig_color,
            &mut final_color_extra_buffer,
        );

        output_buffer.add_read(
            &read_buffer,
            None,
            writable_color,
            &final_color_extra_buffer,
            (),
            &(),
            #[cfg(feature = "support_kmer_counters")]
            abundance,
        );
    }
}
