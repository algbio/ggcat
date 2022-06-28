use crate::{Error, ProcessStats};
use procfs::process::Stat;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use std::time::Duration;

pub fn get_info() -> Result<ProcessStats, Error> {
    let bytes_per_page = procfs::page_size().map_err(Error::SystemCall)?;
    let ticks_per_second = procfs::ticks_per_second().map_err(Error::SystemCall)?;

    let path = Path::new("/proc/self/stat");
    let mut file_contents = Vec::new();
    File::open(path)
        .map_err(|e| Error::FileRead(path.to_path_buf(), e))?
        .read_to_end(&mut file_contents)
        .map_err(|e| Error::FileRead(path.to_path_buf(), e))?;

    let readable_string = Cursor::new(file_contents);
    let stat_file =
        Stat::from_reader(readable_string).map_err(|_e| Error::FileContentsMalformed)?;

    let memory_usage_bytes = (stat_file.rss as u64) * (bytes_per_page as u64);
    let user_mode_seconds = (stat_file.utime as f64) / (ticks_per_second as f64);
    let kernel_mode_seconds = (stat_file.stime as f64) / (ticks_per_second as f64);

    Ok(ProcessStats {
        cpu_time_user: Duration::from_secs_f64(user_mode_seconds),
        cpu_time_kernel: Duration::from_secs_f64(kernel_mode_seconds),
        memory_usage_bytes,
    })
}

#[cfg(test)]
pub mod tests {
    use crate::linux;

    #[tokio::test]
    pub async fn test_no_error() {
        #[no_mangle]
        fn spin_for_a_bit() {
            let mut _a = 0;
            for _i in 0..9999999 {
                _a += 1;
            }
        }

        // to get some nonzero number for cpu_time_user
        spin_for_a_bit();

        dbg!(linux::get_info().await.unwrap());
    }
}
