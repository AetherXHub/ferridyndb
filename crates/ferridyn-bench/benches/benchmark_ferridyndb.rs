use std::env::current_dir;
use std::{fs, process};

mod common;
use common::*;

fn main() {
    let _ = env_logger::try_init();
    let tmpdir = current_dir().unwrap().join(".benchmark");
    let _ = fs::remove_dir_all(&tmpdir);
    fs::create_dir(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    let results = {
        let tmpfile = tmpdir.join("ferridyndb.dat");
        let db = ferridyn_core::api::FerridynDB::create(&tmpfile).unwrap();
        let table = FerridynBenchDatabase::new(db);
        benchmark(table, &tmpfile)
    };

    fs::remove_dir_all(&tmpdir).unwrap();

    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::ASCII_MARKDOWN);
    table.set_width(80);
    table.set_header(["", "ferridyndb"]);
    for (name, result) in &results {
        table.add_row(vec![name.to_string(), result.to_string()]);
    }

    println!();
    println!("{table}");
}
