set -e
mkdir -p target/checking
export CARGO_TARGET_DIR=target/checking

# cargo clean

cargo check --tests --all
cargo check
cargo check --release


export TESTABLE_FEATURES="mem-analysis no-stats process-stats tracing devel-build kmer-counters"
for feature in ${TESTABLE_FEATURES}; do
    echo "Checking with feature $feature"
    cargo check --tests --features "$feature"
    cargo check --features "$feature"
    cargo check --release --features "$feature"
done

cargo clean
