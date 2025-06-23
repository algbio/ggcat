#[derive(Clone, PartialEq, PartialOrd)]
pub enum AssemblerStartingStep {
    MinimizerBucketing = 0,
    KmersMerge = 1,
    HashesSorting = 2,
    LinksCompaction = 3,
    ReorganizeReads = 4,
    BuildUnitigs = 5,
    MaximalUnitigsLinks = 6,
}
