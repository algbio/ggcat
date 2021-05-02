fn main() {
    println!("cargo:rustc-link-search=native={}", "./libdeflate");
    println!("cargo:rustc-link-lib=static=deflate");
}
