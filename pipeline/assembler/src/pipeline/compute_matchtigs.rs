use crate::pipeline::maximal_unitig_links::maximal_unitig_index::DoubleMaximalUnitigLinks;
use genome_graph::bigraph::implementation::node_bigraph_wrapper::NodeBigraphWrapper;
use genome_graph::bigraph::interface::BidirectedData;
use genome_graph::bigraph::traitgraph::implementation::petgraph_impl::PetGraph;
use genome_graph::bigraph::traitgraph::interface::ImmutableGraphContainer;
use genome_graph::generic::{GenericEdge, GenericNode};
use io::concurrent::structured_sequences::{IdentSequenceWriter, StructuredSequenceBackend};
use io::concurrent::temp_reads::extra_data::SequenceExtraData;
use libmatchtigs::MatchtigEdgeData;
use libmatchtigs::{GreedytigAlgorithm, GreedytigAlgorithmConfiguration, TigAlgorithm};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use traitgraph_algo::dijkstra::DijkstraWeightedEdgeData;

// Declare types for the graph. It may or may not make sense to have this be the same type as the iterator outputs.
#[derive(Debug, Eq, PartialEq, Clone)]
struct UnitigEdgeData<'a> {
    sequence_handle: SequenceHandle,
    self_complemental: bool,
    forwards: bool,
    weight: usize,
    dummy_edge_id: usize,
    edges_list: &'a [usize],
}

type SequenceHandle = usize;

impl<'a> BidirectedData for UnitigEdgeData<'a> {
    fn mirror(&self) -> Self {
        Self {
            sequence_handle: self.sequence_handle,
            self_complemental: self.self_complemental,
            forwards: !self.forwards,
            weight: self.weight,
            dummy_edge_id: self.dummy_edge_id,
            edges_list: self.edges_list,
        }
    }
}

// SequenceHandle is the type that points to a sequence, e.g. just an integer.
impl<'a> MatchtigEdgeData<SequenceHandle> for UnitigEdgeData<'a> {
    fn is_dummy(&self) -> bool {
        self.dummy_edge_id != 0
    }

    // true if this edge represents the forwards variant of a unitig, where forwards is the direction it is stored in your sequence store/file
    fn is_forwards(&self) -> bool {
        self.forwards
    }

    // store these values
    fn new(
        sequence_handle: SequenceHandle,
        forwards: bool,
        weight: usize,
        dummy_edge_id: usize,
    ) -> Self {
        unimplemented!()
    }
}

impl<'a> DijkstraWeightedEdgeData<usize> for UnitigEdgeData<'a> {
    fn weight(&self) -> usize {
        self.weight
    }
}

// struct

struct MatchtigsStorageBackend<ColorInfo: IdentSequenceWriter>(PhantomData<ColorInfo>);

impl<ColorInfo: IdentSequenceWriter> StructuredSequenceBackend<ColorInfo, DoubleMaximalUnitigLinks>
    for MatchtigsStorageBackend<ColorInfo>
{
    type SequenceTempBuffer = ();

    fn alloc_temp_buffer() -> Self::SequenceTempBuffer {
        todo!()
    }

    fn write_sequence(
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: &[u8],
        color_info: ColorInfo,
        links_info: DoubleMaximalUnitigLinks,
        extra_buffers: &(
            ColorInfo::TempBuffer,
            <DoubleMaximalUnitigLinks as SequenceExtraData>::TempBuffer,
        ),
    ) {
        todo!()
    }

    fn get_path(&self) -> PathBuf {
        todo!()
    }

    fn flush_temp_buffer(&mut self, buffer: &mut Self::SequenceTempBuffer) {
        todo!()
    }

    fn finalize(self) {
        todo!()
    }
}

impl<'a> GenericNode for UnitigEdgeData<'a> {
    type EdgeIterator = std::iter::Empty<GenericEdge>;

    fn id(&self) -> usize {
        self.sequence_handle
    }

    fn is_self_complemental(&self) -> bool {
        todo!()
    }

    fn edges(&self) -> Self::EdgeIterator {
        todo!()
    }
}

pub fn compute_matchtigs_thread(k: usize, threads_count: usize, output_file: impl AsRef<Path>) {
    // Generic node should be documented well enough, so check that out on how to implement it :)
    // let test = vec![];
    let iterator = (0..10).into_iter().map(|_| UnitigEdgeData { sequence_handle: 0, self_complemental: false, forwards: false, weight: 0, dummy_edge_id: 0, edges_list: &[] }) /* some iterator over something implementing genome_graph::generic::GenericNode */;

    let mut graph: NodeBigraphWrapper<PetGraph<(), UnitigEdgeData>> =
        genome_graph::generic::convert_generic_node_centric_bigraph_to_edge_centric::<(), _, _ ,_ ,_>(iterator)
            .unwrap();

    /* assign weight to each edge */
    for edge_index in graph.edge_indices() {
        let edge_data: &mut UnitigEdgeData = graph.edge_data_mut(edge_index);
        let sequence_length = 0; /* length (in characters) of the sequence associated with edge_data */
        let weight = sequence_length + 1 - k; // number of kmers in the sequence
        edge_data.weight = weight;
    }

    let greedytigs = GreedytigAlgorithm::compute_tigs(
        &mut graph,
        &GreedytigAlgorithmConfiguration::new(threads_count, k),
    );

    for walk in greedytigs.iter() {
        let first_edge = *walk.first().unwrap();
        let first_data = graph.edge_data(first_edge);
        // print sequence of first edge forwards or reverse complemented, depending on first_data.is_forwards()

        let mut previous_data = first_data;
        for edge in walk.iter().skip(1) {
            let edge_data = graph.edge_data(*edge);
            let offset = if previous_data.is_original() {
                k - 1
            } else {
                k - 1 - previous_data.weight()
            };

            // print sequence of edge, starting from character at index offset, forwards or reverse complemented, depending on edge_data.is_forwards()

            previous_data = edge_data;
        }
    }
}
