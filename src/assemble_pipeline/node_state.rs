use crate::utils::Utils;
use core::fmt::{Debug, Formatter};
use std::num::NonZeroU8;

const STATE_EMPTY: u8 = 0x00;

const FORWARD_OFFSET: u8 = 4;
const BACKWARD_OFFSET: u8 = 0;

const STATE_MASK: u8 = 0b111;

// Relative difference states transitions
const STATES_DIFF_MAP: [[u8; 4]; 6] = [
    // A, C, T, G
    [1, 2, 3, 4], // Empty, 0
    [0, 4, 4, 4], // A, 1
    [3, 0, 3, 3], // C, 2
    [2, 2, 0, 2], // T, 3
    [1, 1, 1, 0], // G, 4
    [0, 0, 0, 0], // Multi, 5
];

#[inline(always)]
fn has_single_edge(state: u8) -> bool {
    match state {
        1 | 2 | 3 | 4 => true,
        _ => false,
    }
}

pub struct NodeState(u8);

impl NodeState {
    pub const fn new() -> NodeState {
        NodeState(STATE_EMPTY)
    }

    pub fn update(&mut self, forward: bool, cbase: u8) {
        let offset = if forward {
            FORWARD_OFFSET
        } else {
            BACKWARD_OFFSET
        };
        let state = ((self.0) >> offset) & STATE_MASK;
        let state_diff = STATES_DIFF_MAP[state as usize][cbase as usize];
        self.0 += state_diff << offset;
    }

    pub fn is_extendable(&self) -> bool {
        // Checks if we are not in the empty state or multi state
        let fwd_state = (self.0 >> FORWARD_OFFSET) & STATE_MASK;
        let bkw_state = (self.0 >> BACKWARD_OFFSET) & STATE_MASK;
        has_single_edge(fwd_state) && has_single_edge(bkw_state)
    }

    pub fn get_base(&self, forward: bool) -> u8 {
        let offset = if forward {
            FORWARD_OFFSET
        } else {
            BACKWARD_OFFSET
        };
        let state = (self.0 >> offset) & STATE_MASK;
        state - 1
    }

    fn debug_get_state(&self, offset: u8) -> &'static str {
        let state = (self.0 >> offset) & STATE_MASK;
        match state {
            0 => "Empty",
            1 => "A",
            2 => "C",
            3 => "G",
            4 => "T",
            5 => "Multi",
            _ => "Unknown",
        }
    }
}

impl Debug for NodeState {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.write_fmt(format_args!(
            "({}, {})",
            self.debug_get_state(BACKWARD_OFFSET),
            self.debug_get_state(FORWARD_OFFSET)
        ))
    }
}
