use std::path::Path;

fn main() {
    match Path::new("console").exists() {
        true => println!("cargo:rerun-if-changed=console"),
        false => println!("cargo:rerun-if-changed=."),
    }
}
