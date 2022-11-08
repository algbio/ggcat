fn main() {
    cxx_build::bridge("src/lib.rs")
        .flag_if_supported("-std=c++17")
        .compile("ggcat-api");

    let _ = std::fs::create_dir("clib/");
    let _ = std::fs::create_dir("include/");
    let _ = std::fs::create_dir("lib/");

    let source_header = format!(
        "{}/{}",
        std::env::var("OUT_DIR").unwrap(),
        "cxxbridge/include/ggcat-api/src/lib.rs.h"
    );

    std::fs::copy(&source_header, "cinclude/ggcat-api.h")
        .expect(&format!("Cannot copy file: '{}'", source_header));

    println!("cargo:rerun-if-changed=src/lib.rs");
}
