use std::fs::canonicalize;

fn main() {
    println!("cargo:rustc-link-search=native=./libdeflate");
    println!("cargo:rustc-link-lib=static=deflate-optmem");
    println!("cargo:rustc-link-search=native=.");
    println!("cargo:rerun-if-changed=src/");
    println!("cargo:rerun-if-changed=libdeflate/");

    make_cmd::make()
        .env("CFLAGS", "-fPIC")
        .current_dir(canonicalize("libdeflate").unwrap())
        .spawn()
        .unwrap();
}
