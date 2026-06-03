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
use hashes::HashableSequence;
use io::compressed_read::{CompressedRead, CompressedReadIndipendent};
use io::concurrent::temp_reads::extra_data::{SequenceExtraDataTempBufferManagement, TempBuffer};
use io::concurrent_filewriter::ConcurrentFileWriter;
use io::ident_writer::IdentSequenceWriter;
use io::partial_unitigs_extra_data::PartialUnitigExtraData;
use libmatchtigs::{
    EulertigAlgorithm, EulertigAlgorithmConfiguration, MatchtigEdgeData, PathtigAlgorithm,
};
use libmatchtigs::{GreedytigAlgorithm, GreedytigAlgorithmConfiguration, TigAlgorithm};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use sequence_output::indirect_reads_extractor::ReadExtractWorkData;
use sequence_output::sequences_joiner::IndirectSequencesJoiner;
use sequence_output::structured_sequences::concurrent::FastaWriterConcurrentBuffer;
use sequence_output::structured_sequences::{StructuredSequenceBackend, StructuredSequenceWriter};
use std::fmt::Debug;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use traitgraph_algo::dijkstra::DijkstraWeightedEdgeData;

#[cfg(feature = "support_kmer_counters")]
use io::partial_unitigs_extra_data::SequenceAbundance;

const DUMMY_EDGE_VALUE: usize = usize::MAX;

#[derive(Clone)]
struct SequenceHandle<CX: ColorsManager>(Option<Arc<StructuredUnitigsStorage<CX>>>, usize);

impl<CX: ColorsManager> Debug for SequenceHandle<CX> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SequenceHandle").field(&self.1).finish()
    }
}

impl<CX: ColorsManager> SequenceHandle<CX> {
    fn get_sequence_handle(
        &self,
    ) -> Option<(
        &(
            CompressedReadIndipendent,
            PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
            DoubleMaximalUnitigLinks,
        ),
        &StructuredUnitigsStorage<CX>,
    )> {
        self.0
            .as_ref()
            .map(|s| (&s.sequences[self.1 - s.first_sequence_index], s.deref()))
    }
}

impl<CX: ColorsManager> Default for SequenceHandle<CX> {
    fn default() -> Self {
        Self(None, DUMMY_EDGE_VALUE)
    }
}

impl<CX: ColorsManager> PartialEq for SequenceHandle<CX> {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}
impl<CX: ColorsManager> Eq for SequenceHandle<CX> {}

// Declare types for the graph. It may or may not make sense to have this be the same type as the iterator outputs.
#[derive(Clone, Debug)]
struct UnitigEdgeData<CX: ColorsManager> {
    sequence_handle: SequenceHandle<CX>,
    forwards: bool,
    weight: usize,
    dummy_edge_id: usize,
}

impl<CX: ColorsManager> PartialEq for UnitigEdgeData<CX> {
    fn eq(&self, other: &Self) -> bool {
        self.sequence_handle == other.sequence_handle
            && self.forwards == other.forwards
            && self.weight == other.weight
            && self.dummy_edge_id == other.dummy_edge_id
    }
}

impl<CX: ColorsManager> Eq for UnitigEdgeData<CX> {}

impl<CX: ColorsManager> BidirectedData for UnitigEdgeData<CX> {
    fn mirror(&self) -> Self {
        Self {
            sequence_handle: self.sequence_handle.clone(),
            forwards: !self.forwards,
            weight: self.weight,
            dummy_edge_id: self.dummy_edge_id,
        }
    }
}

/*impl<CX: ColorsManager> BidirectedData for UnitigEdgeData<CX> {

}

impl<CX: ColorsManager> DijkstraWeightedEdgeData<usize> for UnitigEdgeData<CX> {

}*/
// SequenceHandle is the type that points to a sequence, e.g. just an integer.
impl<CX: ColorsManager> MatchtigEdgeData<SequenceHandle<CX>> for UnitigEdgeData<CX> {
    fn is_dummy(&self) -> bool {
        self.dummy_edge_id != 0
    }

    // true if this edge represents the forwards variant of a unitig, where forwards is the direction it is stored in your sequence store/file
    fn is_forwards(&self) -> bool {
        self.forwards
    }

    fn new(
        sequence_handle: SequenceHandle<CX>,
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

impl<CX: ColorsManager> DijkstraWeightedEdgeData<usize> for UnitigEdgeData<CX> {
    fn weight(&self) -> usize {
        self.weight
    }
}

pub struct StructuredUnitigsStorage<CX: ColorsManager> {
    first_sequence_index: usize,
    sequences: Vec<(
        CompressedReadIndipendent,
        PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
        DoubleMaximalUnitigLinks,
    )>,

    sequences_buffer: Vec<u8>,
    links_buffer: TempBuffer<DoubleMaximalUnitigLinks>,
    extra_buffer: TempBuffer<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>>,
}

impl<CX: ColorsManager> StructuredUnitigsStorage<CX> {
    fn new() -> Self {
        Self {
            first_sequence_index: usize::MAX,
            sequences: vec![],
            sequences_buffer: vec![],
            links_buffer: DoubleMaximalUnitigLinks::new_temp_buffer(),
            extra_buffer:
                PartialUnitigExtraData::<PartialUnitigsColorStructure<CX>>::new_temp_buffer(),
        }
    }
}

pub struct MatchtigsStorageBackend<CX: ColorsManager> {
    sequences_channel: (
        Sender<Arc<StructuredUnitigsStorage<CX>>>,
        Receiver<Arc<StructuredUnitigsStorage<CX>>>,
    ),
}

impl<CX: ColorsManager> MatchtigsStorageBackend<CX> {
    pub fn new() -> Self {
        Self {
            sequences_channel: crossbeam::channel::unbounded(),
        }
    }

    pub fn get_receiver(&self) -> Receiver<Arc<StructuredUnitigsStorage<CX>>> {
        self.sequences_channel.1.clone()
    }
}

impl<CX: ColorsManager> StructuredSequenceBackend<CX, DoubleMaximalUnitigLinks>
    for MatchtigsStorageBackend<CX>
{
    type SequenceTempBuffer = StructuredUnitigsStorage<CX>;

    fn alloc_temp_buffer(_: usize) -> Self::SequenceTempBuffer {
        StructuredUnitigsStorage::new()
    }

    fn write_sequence(
        _extract_workdata: &mut ReadExtractWorkData<CX>,
        _k: usize,
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: CompressedRead,
        extra_info: PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
        links_info: DoubleMaximalUnitigLinks,
        extra_buffers: &(
            TempBuffer<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>>,
            TempBuffer<DoubleMaximalUnitigLinks>,
        ),
        _indirect_file: Option<&ConcurrentFileWriter>,
        _flush_callback: impl FnMut(&mut Self::SequenceTempBuffer),
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
            CompressedReadIndipendent::from_read::<false>(&sequence, &mut buffer.sequences_buffer);
        let extra_info =
            PartialUnitigExtraData::<PartialUnitigsColorStructure<CX>>::copy_extra_from(
                extra_info,
                &extra_buffers.0,
                &mut buffer.extra_buffer,
            );
        let links_info = DoubleMaximalUnitigLinks::copy_extra_from(
            links_info,
            &extra_buffers.1,
            &mut buffer.links_buffer,
        );

        buffer.sequences.push((sequence, extra_info, links_info));
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

impl<CX: ColorsManager + 'static> GenericNode for UnitigEdgeData<CX> {
    fn id(&self) -> usize {
        self.sequence_handle.1
    }

    fn is_self_complemental(&self) -> bool {
        self.sequence_handle
            .get_sequence_handle()
            .map(|s| s.0.2.is_self_complemental)
            .unwrap_or(false)
    }

    fn edges(&self) -> impl Iterator<Item = GenericEdge> {
        let links = self
            .sequence_handle
            .get_sequence_handle()
            .map(|h| h.0.2.clone())
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

pub fn compute_matchtigs_thread<CX: ColorsManager, BK: StructuredSequenceBackend<CX, ()>>(
    k: usize,
    threads_count: usize,
    input_data: Receiver<Arc<StructuredUnitigsStorage<CX>>>,
    out_file: &StructuredSequenceWriter<CX, (), BK>,
    mode: MatchtigMode,
    indirect_file: &ConcurrentFileWriter,
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

    let mut sequence_buffer = IndirectSequencesJoiner::<CX>::new(k, indirect_file.clone());

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

        sequence_buffer.reset();

        let first_sequence = handle.0.as_reference(&storage.sequences_buffer);

        // Update the sequence and its colors
        sequence_buffer.append_sequence(
            first_sequence,
            &handle.1,
            0,
            first_data.is_forwards(),
            &storage.extra_buffer,
            None,
        );

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

            let bases_offset = k - 1 - extra_bases;

            // print sequence of edge, starting from character at index offset, forwards or reverse complemented, depending on edge_data.is_forwards()
            let next_sequence = handle.0.as_reference(&storage.sequences_buffer);

            sequence_buffer.append_sequence(
                next_sequence,
                &handle.1,
                bases_offset,
                first_data.is_forwards(),
                &storage.extra_buffer,
                None,
            );
        }

        let joined_sequence = sequence_buffer.get_sequence();

        output_buffer.add_read(
            joined_sequence.sequence,
            None,
            joined_sequence.extra,
            joined_sequence.extra_buffer,
            (),
            &(),
            Some(&indirect_file),
        );
    }

    output_buffer.finalize(Some(indirect_file));
}
