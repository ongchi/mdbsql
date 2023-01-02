use version_compare::Version;

fn main() {
    let library = pkg_config::probe_library("libmdbsql").unwrap_or_else(|e| panic!("{}", e));
    let v1_0 = Version::from("1.0.0").unwrap();
    let current_version = Version::from(&library.version).unwrap();

    if current_version >= v1_0 {
        println!("cargo:rustc-cfg=LIBMDBSQL_GE_VERSION_1");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
