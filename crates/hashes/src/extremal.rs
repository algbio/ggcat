use crate::{HashFunction, HashFunctionFactory, HashableSequence};

pub trait HashGenerator<MH: HashFunctionFactory> {
    fn get_extremal_hash<S: HashableSequence>(
        &self,
        seq: S,
        k: usize,
        beginning: bool,
    ) -> MH::HashTypeExtendable;
}

pub struct PrecomputedHash<MH: HashFunctionFactory>(pub MH::HashTypeExtendable);

impl<MH: HashFunctionFactory> HashGenerator<MH> for PrecomputedHash<MH> {
    fn get_extremal_hash<S: HashableSequence>(
        &self,
        _seq: S,
        _k: usize,
        _beginning: bool,
    ) -> MH::HashTypeExtendable {
        self.0
    }
}

pub struct DelayedHashComputation;

impl<MH: HashFunctionFactory> HashGenerator<MH> for DelayedHashComputation {
    #[inline(always)]
    fn get_extremal_hash<S: HashableSequence>(
        &self,
        seq: S,
        k: usize,
        beginning: bool,
    ) -> <MH as HashFunctionFactory>::HashTypeExtendable {
        if beginning {
            let hash = MH::new(seq, k);
            hash.iter().next().unwrap()
        } else {
            let hash = MH::new(seq.subslice(seq.bases_count() - k, seq.bases_count()), k);
            hash.iter().next().unwrap()
        }
    }
}
