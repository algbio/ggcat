use std::fs::create_dir_all;

fn main() {
    cxx_build::bridge("src/lib.rs")
        .flag_if_supported("-std=c++11")
        .compile("ggcat_cxx_interop");

    std::fs::copy(
        format!(
            "{}{}",
            std::env::var("OUT_DIR").unwrap(),
            "/cxxbridge/include/ggcat-cpp-bindings/src/lib.rs.h"
        ),
        "ggcat-cpp-api/include/ggcat-cpp-bindings.hh",
    )
    .unwrap();

    let _ = create_dir_all("ggcat-cpp-api/lib");

    std::fs::copy(
        format!(
            "{}{}",
            std::env::var("OUT_DIR").unwrap(),
            "/libggcat_cxx_interop.a"
        ),
        "ggcat-cpp-api/lib/libggcat_cxx_interop.a",
    )
    .expect(&format!(
        "Cannot copy file: '{}'",
        format!(
            "{}{}",
            std::env::var("OUT_DIR").unwrap(),
            "/libggcat_cxx_interop.a"
        )
    ));

    println!("cargo:rerun-if-changed=src/lib.rs");
}
