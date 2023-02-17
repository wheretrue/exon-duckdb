extern crate cbindgen;

use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    // let out_dir = env::var_os("OUT_DIR").unwrap();
    let out_dir = "./../src/include/";

    // Join the out_dir with the name of the file we want to generate.wtt01_rust
    let dest_path = std::path::Path::new(&out_dir).join("wtt01_rust.hpp");

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(dest_path);
}
