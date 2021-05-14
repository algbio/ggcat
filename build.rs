fn main() {
    println!("cargo:rustc-link-search=native={}", "./libdeflate");
    println!("cargo:rustc-link-lib=static=deflate");
    println!("cargo:rustc-link-search=native={}", ".");
    println!("cargo:rerun-if-changed=src/");
}
