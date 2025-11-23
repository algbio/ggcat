#[derive(Copy, Clone, PartialEq, PartialOrd, Default)]
pub enum AssemblerPhase {
    #[default]
    MinimizerBucketing = 0,
    KmersMerge = 1,
    UnitigsExtension = 2,
    MaximalUnitigsLinks = 3,
    FinalStep = 4,
}
