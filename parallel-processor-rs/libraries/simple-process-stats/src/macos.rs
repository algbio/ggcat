use crate::{Error, ProcessStats};
use std::time::Duration;

pub fn get_info() -> Result<ProcessStats, Error> {
    let pid = unsafe { libc::getpid() };
    let proc_info = darwin_libproc::task_info(pid).map_err(Error::SystemCall)?;

    Ok(ProcessStats {
        memory_usage_bytes: proc_info.pti_resident_size,
        cpu_time_user: Duration::from_nanos(proc_info.pti_total_user),
        cpu_time_kernel: Duration::from_nanos(proc_info.pti_total_system),
    })
}

#[cfg(test)]
pub mod tests {
    use crate::macos;

    #[test]
    pub fn test_no_error() {
        #[no_mangle]
        fn spin_for_a_bit() {
            let mut _a = 0;
            for _i in 0..9999999 {
                _a += 1;
            }
        }

        // to get some nonzero number for cpu_time_user
        spin_for_a_bit();

        dbg!(macos::get_info().unwrap());
    }
}
