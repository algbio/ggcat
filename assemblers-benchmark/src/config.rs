use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Tool {
    pub name: String,
    pub path: PathBuf,
    pub arguments: String,

    #[serde(rename = "reads-arg-prefix")]
    pub reads_arg_prefix: Option<String>,
    #[serde(rename = "sequences-arg-prefix")]
    pub sequences_arg_prefix: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Dataset {
    pub name: String,
    pub files: Vec<PathBuf>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Benchmark {
    pub name: String,
    pub datasets: Vec<String>,
    pub tools: Vec<String>,
    #[serde(rename = "temp-dir")]
    pub temp_dir: PathBuf,
    #[serde(rename = "trim-before")]
    pub trim_before: bool,
    pub kvalues: Vec<usize>,
    pub threads: Vec<usize>,
    #[serde(rename = "max-memory")]
    pub max_memory: usize,
    #[serde(rename = "min-multiplicity")]
    pub min_multiplicity: usize,
    #[serde(rename = "size-check-time")]
    pub size_check_time: u64,
    // #[serde(rename = "output-mode")]
    // pub output_mode: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    pub tools: Vec<Tool>,
    pub datasets: Vec<Dataset>,
    pub benchmarks: Vec<Benchmark>,
}
