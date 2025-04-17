use super::StatId;

#[macro_export]
macro_rules! stats {
    (stats.assembler.$name:ident = $($items:tt)*) => {
        {
            use $crate::generate_stat_id;
            let value = $($items)*;
            let mut stats = $crate::stats::STATS.lock();
            stats.assembler.$name = value;
        }
    };
    (stats.assembler.$name:ident = $($items:tt)*) => {
        {
            use $crate::generate_stat_id;
            let mut stats = $crate::stats::STATS.lock();
            stats.assembler.$name = $($items)*;
        }
    };
    (stats.$name:ident = $($items:tt)*) => {
        {
            use $crate::generate_stat_id;
            let value = ($($items)*);
            let mut stats = $crate::stats::STATS.lock();
            stats.$name = value;
        }
    };
    (stats.$name:ident.$($items:tt)*) => {
        {
            use $crate::generate_stat_id;
            let mut stats = $crate::stats::STATS.lock();
            stats.$name.$($items)*;
        }
    };
    (let stats.$name:ident = $($items:tt)*) => {
        {
            use $crate::generate_stat_id;
            let value = Some($($items)*);
            let mut stats = $crate::stats::STATS.lock();
            stats.$name = value;
        }
    };
    ($($items:tt)*) => {
        $($items)*
    };
}

pub fn generate_stat_id() -> StatId {
    use std::sync::atomic::{AtomicU64, Ordering};
    static BLOCK_ID: AtomicU64 = AtomicU64::new(1);
    StatId(BLOCK_ID.fetch_add(1, Ordering::SeqCst))
}

#[macro_export]
macro_rules! generate_stat_id {
    () => {
        $crate::stats::macros::generate_stat_id()
    };
}

#[macro_export]
macro_rules! get_stat_opt {
    (stats.$name:ident) => {
        $crate::stats::STATS.lock().$name.unwrap()
    };
}

#[macro_export]
macro_rules! get_stat {
    (stats.$name:ident) => {
        $crate::stats::STATS.lock().$name
    };
}
