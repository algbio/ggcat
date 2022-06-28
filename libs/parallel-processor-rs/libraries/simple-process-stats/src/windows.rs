use crate::{Error, ProcessStats};
use std::mem;
use std::time::Duration;
use winapi::shared::minwindef::FILETIME;
use winapi::um::processthreadsapi::GetCurrentProcess;
use winapi::um::processthreadsapi::GetProcessTimes;
use winapi::um::psapi::K32GetProcessMemoryInfo;
use winapi::um::psapi::PROCESS_MEMORY_COUNTERS;
use winapi::um::winnt::ULARGE_INTEGER;

fn empty_filetime() -> FILETIME {
    FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    }
}

// FILETIME: Contains a 64-bit value representing the number of 100-nanosecond intervals since January 1, 1601 (UTC).
// note because we are parsing a duration the offset is of no interest to us
fn filetime_to_u64(f: &FILETIME) -> u64 {
    unsafe {
        let mut v: ULARGE_INTEGER = mem::zeroed();
        v.s_mut().LowPart = f.dwLowDateTime;
        v.s_mut().HighPart = f.dwHighDateTime;
        *v.QuadPart()
    }
}

fn filetime_to_duration(f: &FILETIME) -> Duration {
    let hundred_nanos = filetime_to_u64(f);

    // 1 second is 10^9 nanoseconds,
    // so 1 second is 10^7 * (100 nanoseconds).
    let seconds = hundred_nanos / u64::pow(10, 7);
    // 1 second is 10^9 nanos which always fits in a u32.
    let nanos = ((hundred_nanos % u64::pow(10, 7)) * 100) as u32;

    Duration::new(seconds, nanos)
}

pub fn get_info() -> Result<ProcessStats, Error> {
    let current_process = unsafe { GetCurrentProcess() };

    // cpu info
    let mut lp_creation_time = empty_filetime();
    let mut lp_exit_time = empty_filetime();
    let mut lp_kernel_time = empty_filetime();
    let mut lp_user_time = empty_filetime();
    let return_code = unsafe {
        GetProcessTimes(
            current_process,
            &mut lp_creation_time,
            &mut lp_exit_time,
            &mut lp_kernel_time,
            &mut lp_user_time,
        )
    };
    if return_code == 0 {
        return Err(Error::SystemCall(std::io::Error::last_os_error()));
    }

    // memory info
    let mut proc_mem_counters = PROCESS_MEMORY_COUNTERS {
        cb: 0,
        PageFaultCount: 0,
        PeakWorkingSetSize: 0,
        WorkingSetSize: 0,
        QuotaPeakPagedPoolUsage: 0,
        QuotaPagedPoolUsage: 0,
        QuotaPeakNonPagedPoolUsage: 0,
        QuotaNonPagedPoolUsage: 0,
        PagefileUsage: 0,
        PeakPagefileUsage: 0,
    };
    let cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
    let return_code =
        unsafe { K32GetProcessMemoryInfo(current_process, &mut proc_mem_counters, cb) };
    if return_code == 0 {
        return Err(Error::SystemCall(std::io::Error::last_os_error()));
    }

    Ok(ProcessStats {
        cpu_time_user: filetime_to_duration(&lp_user_time),
        cpu_time_kernel: filetime_to_duration(&lp_kernel_time),
        memory_usage_bytes: proc_mem_counters.WorkingSetSize as u64,
    })
}

#[cfg(test)]
mod tests {
    use crate::windows::{self, filetime_to_duration};
    use std::time::Duration;
    use winapi::shared::minwindef::FILETIME;

    #[test]
    fn test_no_error() {
        #[no_mangle]
        fn spin_for_a_bit() {
            let mut _a = 0;
            for _i in 0..9999999 {
                _a += 1;
            }
        }

        // to get some nonzero number for cpu_time_user
        spin_for_a_bit();

        dbg!(windows::get_info().unwrap());
    }

    #[test]
    fn test_time_conversion_zero() {
        assert_eq!(
            Duration::from_nanos(0),
            filetime_to_duration(&FILETIME {
                dwLowDateTime: 0,
                dwHighDateTime: 0
            })
        )
    }

    #[test]
    fn test_time_conversion_nonzero() {
        assert_eq!(
            Duration::from_nanos(500),
            filetime_to_duration(&FILETIME {
                dwLowDateTime: 5,
                dwHighDateTime: 0
            })
        )
    }

    #[test]
    fn test_time_conversion_nonzero_with_seconds() {
        assert_eq!(
            Duration::new(5, 500), // 5 seconds 500 nanoseconds
            filetime_to_duration(&FILETIME {
                dwLowDateTime: 5 * u32::pow(10, 7) + 5, // 5 * 10^7 + 5
                dwHighDateTime: 0
            })
        )
    }
}
