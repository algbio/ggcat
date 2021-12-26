pub mod config;
pub mod runner;

use crate::config::Config;
use crate::runner::{Parameters, RunResults, Runner};
use cgroups_rs::cgroup_builder::CgroupBuilder;
use cgroups_rs::Cgroup;
use std::ffi::CString;
use std::fs::{create_dir_all, read_dir, remove_dir_all, File};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::panic::resume_unwind;
use std::path::PathBuf;
use std::time::Duration;
use structopt::*;

#[derive(StructOpt)]
enum ExtendedCli {
    #[cfg(feature = "cpu-limit")]
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

    #[structopt(short, long = "results-dir", default_value = "results-dir")]
    results_dir: PathBuf,

    #[structopt(short, long = "outputs-dir", default_value = "outputs-dir")]
    outputs_dir: PathBuf,

    #[structopt(short, long = "logs-dir", default_value = "logs-dir")]
    logs_dir: PathBuf,
}

fn main() {
    let args: ExtendedCli = ExtendedCli::from_args();

    fdlimit::raise_fd_limit().unwrap();

    match args {
        #[cfg(feature = "cpu-limit")]
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

            std::fs::create_dir_all(&args.results_dir);
            std::fs::create_dir_all(&args.outputs_dir);
            std::fs::create_dir_all(&args.logs_dir);

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
                            let results_file = args.results_dir.join(&format!(
                                "{}@K{}_{}_{}thr-info.json",
                                dataset.name, kval, tool.name, thread
                            ));

                            if results_file.exists() {
                                println!(
                                    "File {} already exists, skipping test!",
                                    results_file.file_name().unwrap().to_str().unwrap()
                                );
                                continue;
                            }

                            let temp_dir = experiment.temp_dir.join(&format!(
                                "{}@K{}_{}_{}thr_temp",
                                dataset.name, kval, tool.name, thread
                            ));
                            let out_dir = experiment.temp_dir.join(&format!(
                                "{}@K{}_{}_{}thr_out",
                                dataset.name, kval, tool.name, thread
                            ));
                            if temp_dir.exists() && temp_dir.read_dir().unwrap().next().is_some() {
                                panic!(
                                    "Temporary directory {} not empty!, aborting (file: {})",
                                    temp_dir.display(),
                                    temp_dir
                                        .read_dir()
                                        .unwrap()
                                        .next()
                                        .unwrap()
                                        .unwrap()
                                        .file_name()
                                        .into_string()
                                        .unwrap()
                                );
                            }
                            if out_dir.exists() && out_dir.read_dir().unwrap().next().is_some() {
                                panic!(
                                    "Output directory {} not empty!, aborting",
                                    out_dir.display()
                                );
                            }
                            create_dir_all(&temp_dir);
                            create_dir_all(&out_dir);

                            let results = Runner::run_tool(
                                (*tool).clone(),
                                dataset.clone(),
                                Parameters {
                                    max_threads: *thread,
                                    k: *kval,
                                    multiplicity: experiment.min_multiplicity,
                                    output_file: out_dir
                                        .join(&format!(
                                            "{}@K{}_{}_{}thr.fa",
                                            dataset.name, kval, tool.name, thread
                                        ))
                                        .into_os_string()
                                        .into_string()
                                        .unwrap(),
                                    temp_dir: temp_dir
                                        .clone()
                                        .into_os_string()
                                        .into_string()
                                        .unwrap(),
                                    log_file: args.logs_dir.clone().join(&format!(
                                        "{}@K{}_{}_{}.log",
                                        dataset.name, kval, tool.name, thread
                                    )),
                                    size_check_time: Duration::from_millis(
                                        experiment.size_check_time,
                                    ),
                                },
                            );

                            remove_dir_all(&temp_dir);

                            let final_out_dir = args.outputs_dir.join(&format!(
                                "{}@K{}_{}_{}thr_out",
                                dataset.name, kval, tool.name, thread
                            ));
                            create_dir_all(&final_out_dir);

                            for file in read_dir(&out_dir).unwrap() {
                                let file = file.unwrap();

                                let name = file.file_name();
                                std::fs::copy(file.path(), final_out_dir.join(name));
                                std::fs::remove_file(file.path());
                            }
                            remove_dir_all(&out_dir);

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
