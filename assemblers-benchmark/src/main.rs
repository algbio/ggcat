pub mod config;
pub mod runner;

use crate::config::Config;
use crate::runner::{Parameters, RunResults, Runner};
use cgroups_rs::cgroup_builder::CgroupBuilder;
use cgroups_rs::Cgroup;
use std::ffi::CString;
use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::panic::resume_unwind;
use std::path::PathBuf;
use structopt::*;

#[derive(StructOpt)]
enum ExtendedCli {
    Start(StartOpt),
    Bench(Cli),
}

#[derive(StructOpt)]
struct StartOpt {}

#[derive(StructOpt)]
struct Cli {
    test_name: String,

    #[structopt(short, long, default_value = "bench-settings.toml")]
    settings_file: PathBuf,

    #[structopt(short, long)]
    output: PathBuf,

    #[structopt(short, long, default_value = "results-dir")]
    results_dir: PathBuf,
}

fn main() {
    let args: ExtendedCli = ExtendedCli::from_args();

    match args {
        ExtendedCli::Start(_opt) => {
            let hier = cgroups_rs::hierarchies::auto();
            let cg: Cgroup = CgroupBuilder::new("genome-benchmark-cgroup").build(hier);
            for subsys in cg.subsystems() {
                let path = subsys.to_controller().path();
                println!("Path: {}", path.display());

                let mut perms = std::fs::metadata(path).unwrap().permissions();
                perms.set_mode(0o777);
                std::fs::set_permissions(path, perms).unwrap();

                for path in std::fs::read_dir(path).unwrap() {
                    if let Ok(dir) = path {
                        let mut perms = std::fs::metadata(dir.path()).unwrap().permissions();
                        perms.set_mode(0o777);
                        std::fs::set_permissions(dir.path(), perms).unwrap();
                    }
                }
            }
        }
        ExtendedCli::Bench(args) => {
            let mut settings_file = File::open(args.settings_file).unwrap();
            let mut settings_text = String::new();

            settings_file.read_to_string(&mut settings_text).unwrap();

            let settings: Config = toml::from_str(&settings_text).unwrap();

            let experiment = {
                let mut res = None;
                let mut multiple_choices = Vec::new();

                for bench in &settings.benchmarks {
                    if bench.name == args.test_name {
                        res = Some(bench.clone());
                    } else if bench.name.starts_with(&args.test_name) {
                        if res.is_none() {
                            res = Some(bench.clone());
                        } else {
                            if res.as_ref().unwrap().name != args.test_name {
                                multiple_choices.push(bench.name.clone());
                            }
                        }
                    }
                }

                if res.is_none() {
                    println!("Cannot find a benchmark matching \"{}\"!", args.test_name);
                    println!("Available benchmarks:");
                    for bench in settings.benchmarks {
                        println!("\t{}", &bench.name);
                    }
                    return;
                } else if multiple_choices.len() > 0 {
                    println!("Multiple benchmarks matching \"{}\"!", args.test_name);
                    println!("Matching benchmarks:");
                    println!("\t{}", res.as_ref().unwrap().name);
                    for bench in multiple_choices {
                        println!("\t{}", bench);
                    }
                    return;
                }
                res.unwrap()
            };

            let datasets = experiment
                .datasets
                .iter()
                .map(|x| {
                    settings
                        .datasets
                        .iter()
                        .filter(|d| &d.name == x)
                        .next()
                        .expect(&format!("Cannot find a dataset with name '{}'", x))
                })
                .collect::<Vec<_>>();

            let tools = experiment
                .tools
                .iter()
                .map(|x| {
                    settings
                        .tools
                        .iter()
                        .filter(|t| &t.name == x)
                        .next()
                        .expect(&format!("Cannot find a tool with name '{}'", x))
                })
                .collect::<Vec<_>>();

            create_dir_all(&args.results_dir);

            for dataset in datasets {
                for kval in &experiment.kvalues {
                    for thread in &experiment.threads {
                        for tool in &tools {
                            let temp_dir = experiment.temp_dir.join(&format!(
                                "{}@K{}_{}_{}thr_temp",
                                dataset.name, kval, tool.name, thread
                            ));
                            create_dir_all(&temp_dir);

                            let results = Runner::run_tool(
                                (*tool).clone(),
                                dataset.clone(),
                                Parameters {
                                    max_threads: *thread,
                                    k: *kval,
                                    multiplicity: experiment.min_multiplicity,
                                    output_file: args
                                        .results_dir
                                        .join(&format!(
                                            "{}@K{}_{}_{}thr.fa",
                                            dataset.name, kval, tool.name, thread
                                        ))
                                        .into_os_string()
                                        .into_string()
                                        .unwrap(),
                                    temp_dir: temp_dir.into_os_string().into_string().unwrap(),
                                },
                            );

                            let results_file = args.results_dir.join(&format!(
                                "{}@K{}_{}_{}thr-info.json",
                                dataset.name, kval, tool.name, thread
                            ));

                            File::create(results_file)
                                .unwrap()
                                .write_all(
                                    serde_json::to_string_pretty(&results).unwrap().as_bytes(),
                                )
                                .unwrap();
                        }
                    }
                }
            }
        }
    }
}
