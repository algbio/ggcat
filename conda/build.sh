curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain nightly -y
git clone https://github.com/algbio/ggcat.git --recursive
pushd ggcat
    git checkout "v1.0_beta.1"
    cargo install --path crates/cmdline/ --locked --root "${PREFIX}"
popd