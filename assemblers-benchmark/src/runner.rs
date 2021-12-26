use crate::config::{Dataset, Tool};
use fork::Fork;
use rlimit::Resource;

use cgroups_rs::cgroup_builder::*;
use cgroups_rs::*;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::mem::MaybeUninit;
use std::os::raw::c_int;
use std::os::unix::raw::pid_t;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::Thread;
use std::time::{Duration, Instant};
use std::{env, io};
use walkdir::WalkDir;

pub struct Runner {}

pub struct Parameters {
    pub max_threads: usize,
    pub k: usize,
    pub multiplicity: usize,
    pub output_file: String,
    pub temp_dir: String,
    pub log_file: PathBuf,
    pub size_check_time: Duration,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunResults {
    command_line: String,
    max_memory_gb: f64,
    user_time_secs: f64,
    system_time_secs: f64,
    real_time_secs: f64,
    total_written_gb: f64,
    total_read_gb: f64,
    max_used_disk_gb: f64,
}

fn absolute_path(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let path = path.as_ref();

    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    };

    Ok(absolute_path)
}

fn get_dir_size(path: impl AsRef<Path>) -> u64 {
    let mut dir_size = 0;
    for entry in WalkDir::new(path) {
        if let Ok(file) = entry {
            dir_size += file.metadata().map(|m| m.len()).unwrap_or(0)
        }
    }
    dir_size
}

impl Runner {
    pub fn run_tool(tool: Tool, dataset: Dataset, parameters: Parameters) -> RunResults {
        // Acquire a handle for the cgroup hierarchy.
        #[cfg(feature = "cpu-limit")]
        let cg: Cgroup = {
            let hier = cgroups_rs::hierarchies::auto();

            // Use the builder pattern (see the documentation to create the control group)
            //
            // This creates a control group named "example" in the V1 hierarchy.

            let max_cores = min(num_cpus::get(), parameters.max_threads);

            CgroupBuilder::new("genome-benchmark-cgroup")
                .cpu()
                .period(100000)
                .quota(100000 * max_cores as i64)
                .cpus(format!("{}-{}", 0, max_cores - 1))
                .done()
                .build(hier)
        };

        let input_files: Vec<_> = dataset
            .files
            .iter()
            .map(|x| x.to_str().unwrap().to_string())
            .collect();

        let input_files_list_file_name =
            std::env::temp_dir().join(format!("input-files-{}.in", dataset.name));
        {
            let mut input_files_list = File::create(&input_files_list_file_name).unwrap();
            input_files_list.write_all(input_files.join("\n").as_bytes());
            input_files_list.write_all(b"\n");
        }

        let program_arguments: HashMap<&str, Vec<String>> = [
            ("<THREADS>", vec![parameters.max_threads.to_string()]),
            ("<KVALUE>", vec![parameters.k.to_string()]),
            ("<MULTIPLICITY>", vec![parameters.multiplicity.to_string()]),
            ("<INPUT_FILES>", input_files.clone()),
            (
                "<INPUT_FILES_LIST>",
                vec![input_files_list_file_name.to_str().unwrap().to_string()],
            ),
            ("<INPUT_FILES_READS>", {
                if let Some(reads_prefix) = tool.reads_arg_prefix {
                    if parameters.multiplicity == 1 {
                        input_files
                            .iter()
                            .map(|x| vec![reads_prefix.clone(), x.clone()])
                            .flatten()
                            .collect()
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            }),
            ("<INPUT_FILES_SEQUENCES>", {
                if let Some(sequences_prefix) = tool.sequences_arg_prefix {
                    if parameters.multiplicity >= 2 {
                        input_files
                            .iter()
                            .map(|x| vec![sequences_prefix.clone(), x.clone()])
                            .flatten()
                            .collect()
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            }),
            (
                "<OUTPUT_FILE>",
                vec![absolute_path(&parameters.output_file)
                    .unwrap()
                    .into_os_string()
                    .into_string()
                    .unwrap()],
            ),
            (
                "<TEMP_DIR>",
                vec![absolute_path(&parameters.temp_dir)
                    .unwrap()
                    .into_os_string()
                    .into_string()
                    .unwrap()],
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let mut arguments = tool.arguments.split(" ").collect::<Vec<_>>();

        let mut i = 0;
        while i < arguments.len() {
            if program_arguments.contains_key(arguments[i]) {
                let args = &program_arguments[arguments[i]];
                arguments.remove(i);
                for (j, arg) in args.iter().enumerate() {
                    arguments.insert(i + j, arg);
                }
            } else {
                i += 1;
            }
        }

        let start_time = Instant::now();

        println!(
            "Running tool {} with dataset {} K = {} threads = {}",
            &tool.name, &dataset.name, parameters.k, parameters.max_threads
        );
        eprintln!("{} {}", tool.path.display(), arguments.join(" "));

        let mut command = std::process::Command::new(&tool.path)
            .args(arguments.as_slice())
            .stdout(File::create(&parameters.log_file).unwrap())
            .stderr(File::create(parameters.log_file.with_extension("stderr")).unwrap())
            .spawn()
            .unwrap();

        let is_finished = Arc::new(AtomicBool::new(false));

        let is_finished_thr = is_finished.clone();
        let temp_dir_thr = parameters.temp_dir.clone();
        let out_dir_thr = PathBuf::from(parameters.output_file)
            .parent()
            .unwrap()
            .to_path_buf();

        let maximum_disk_usage = Arc::new(AtomicU64::new(0));

        let maximum_disk_usage_thr = maximum_disk_usage.clone();
        let maximum_disk_usage_thread = std::thread::spawn(move || {
            while !is_finished_thr.load(Ordering::Relaxed) {
                maximum_disk_usage_thr.fetch_max(
                    get_dir_size(&temp_dir_thr) + get_dir_size(&out_dir_thr),
                    Ordering::Relaxed,
                );
                std::thread::sleep(parameters.size_check_time);
            }
        });

        #[cfg(feature = "cpu-limit")]
        cg.add_task(CgroupPid::from(&command)).expect(
            "Cannot set correct cgroup, please initialize as root with the start subcommand",
        );

        let mut rusage: libc::rusage;
        unsafe {
            let mut status = 0;
            rusage = MaybeUninit::zeroed().assume_init();
            libc::wait4(
                command.id() as pid_t,
                &mut status as *mut c_int,
                0,
                &mut rusage as *mut libc::rusage,
            );
        }

        is_finished.store(true, Ordering::Relaxed);
        maximum_disk_usage_thread.join();

        RunResults {
            command_line: format!("{} {}", tool.path.display(), arguments.join(" ")),
            max_memory_gb: rusage.ru_maxrss as f64 / (1024.0 * 1024.0),
            user_time_secs: rusage.ru_utime.tv_sec as f64
                + (rusage.ru_utime.tv_usec as f64 / 1000000.0),
            system_time_secs: rusage.ru_stime.tv_sec as f64
                + (rusage.ru_stime.tv_usec as f64 / 1000000.0),
            real_time_secs: start_time.elapsed().as_secs_f64(),
            total_written_gb: rusage.ru_oublock as f64 / 2048.0 / 1024.0,
            total_read_gb: rusage.ru_inblock as f64 / 2048.0 / 1024.0,
            max_used_disk_gb: maximum_disk_usage.load(Ordering::Relaxed) as f64
                / (1024.0 * 1024.0 * 1024.0),
        }
    }
}
