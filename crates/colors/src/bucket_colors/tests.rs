use super::*;
use rand::{Rng, SeedableRng, rngs::StdRng};

/// One handle is stored inline per superkmer the compactor holds, so its size
/// is a direct per-superkmer memory and I/O cost.
#[test]
fn handle_stays_two_words() {
    assert_eq!(std::mem::size_of::<ColorHandle>(), 16);
}

/// A set of one run is the common case and must not touch the slab at all.
#[test]
fn a_single_run_needs_no_slab() {
    let mut arena = ColorArena::default();
    let singleton = arena.singleton(u32::MAX);
    assert_eq!(arena.used_capacity(), 0);
    assert_eq!(collect(&arena, &singleton), [u32::MAX]);

    let range = arena.from_colors(&(0..1_000_000).collect::<Vec<_>>());
    assert_eq!(arena.runs(&range).len(), 1);
    assert_eq!(arena.used_capacity(), 0);
}

fn collect(arena: &ColorArena, handle: &ColorHandle) -> Vec<ColorIndexType> {
    arena.colors(handle).collect()
}

fn round_trip(arena: &ColorArena, handle: &ColorHandle) -> Vec<ColorIndexType> {
    let mut bytes = Vec::new();
    arena.write_to(handle, &mut bytes);
    let mut decoded = ColorArena::default();
    let handle = decoded.decode(&mut bytes.as_slice()).unwrap();
    collect(&decoded, &handle)
}

#[test]
fn runs_pack_and_unpack_at_the_boundaries() {
    let whole = ColorRun::new(0, 1u64 << 32);
    assert_eq!(whole.start(), 0);
    assert_eq!(whole.len(), 1u64 << 32);
    assert_eq!(whole.end(), 1u64 << 32);
    assert_eq!(whole.last(), u32::MAX);

    let top = ColorRun::single(u32::MAX);
    assert_eq!(top.start(), u32::MAX);
    assert_eq!(top.end(), 1u64 << 32);
    assert_eq!(top.last(), u32::MAX);

    // A first color at or above 2^31 used to collide with the allocator's heap
    // marker; the packing puts it in the high half, so this is the case that
    // proves the marker really moved.
    let high = ColorRun::new(1 << 31, 4);
    assert_eq!(high.start(), 1 << 31);
    assert_eq!(high.len(), 4);
}

/// Sorting an array of packed runs must order them by first color, with no
/// comparator. That property is the reason for the packing.
#[test]
fn packed_runs_sort_by_first_color() {
    let mut runs = vec![
        ColorRun::new(100, 2),
        ColorRun::new(5, 50),
        ColorRun::new(5, 1),
        ColorRun::single(0),
    ];
    runs.sort_unstable();
    assert_eq!(
        runs.iter().map(|run| run.start()).collect::<Vec<_>>(),
        [0, 5, 5, 100]
    );
    // Equal first colors put the shorter run first, which is what forces the
    // merge below to take a maximum rather than extend by the length.
    assert_eq!(runs[1].len(), 1);
    assert_eq!(runs[2].len(), 50);
}

/// A run nested inside another must not grow it. Extending the tail by the
/// incoming length instead of taking the further end would invent colors that
/// were never in either set.
#[test]
fn canonicalize_merges_nested_and_adjacent_runs() {
    let mut nested = [ColorRun::new(10, 100), ColorRun::new(10, 1)];
    assert_eq!(canonicalize(&mut nested), [ColorRun::new(10, 100)]);

    let mut contained = [ColorRun::new(10, 100), ColorRun::new(20, 5)];
    assert_eq!(canonicalize(&mut contained), [ColorRun::new(10, 100)]);

    let mut adjacent = [ColorRun::new(0, 4), ColorRun::new(4, 4)];
    assert_eq!(canonicalize(&mut adjacent), [ColorRun::new(0, 8)]);

    let mut overlapping = [ColorRun::new(0, 6), ColorRun::new(4, 4)];
    assert_eq!(canonicalize(&mut overlapping), [ColorRun::new(0, 8)]);

    let mut disjoint = [ColorRun::new(0, 2), ColorRun::new(3, 2)];
    assert_eq!(
        canonicalize(&mut disjoint),
        [ColorRun::new(0, 2), ColorRun::new(3, 2)]
    );

    let mut boundary = [ColorRun::single(u32::MAX), ColorRun::new(u32::MAX - 1, 2)];
    assert_eq!(
        canonicalize(&mut boundary),
        [ColorRun::new(u32::MAX - 1, 2)]
    );
}

#[test]
fn prepare_sorts_deduplicates_and_round_trips() {
    let mut arena = ColorArena::default();
    let single = arena.singleton(7);
    assert_eq!(arena.unique_color(&single), 7);
    assert_eq!(round_trip(&arena, &single), [7]);

    let mut set = arena.from_colors(&[4, 5, 6, 9]);
    arena.extend_colors(&mut set, &[4, 5]);
    arena.prepare(&mut set);
    assert_eq!(collect(&arena, &set), [4, 5, 6, 9]);
    assert_eq!(round_trip(&arena, &set), [4, 5, 6, 9]);
}

#[test]
fn merging_across_arenas_unions_the_colors() {
    let mut source = ColorArena::default();
    let left = source.from_colors(&[4, 6]);
    let right = source.from_colors(&[5, 6, 7]);

    let mut output = ColorArena::default();
    let mut merged = output.copy_entry(&source, &left);
    output.append_from(&mut merged, &source, &right);
    output.prepare(&mut merged);
    assert_eq!(collect(&output, &merged), [4, 5, 6, 7]);
    // Four consecutive colors are one run, whichever order they arrived in.
    assert_eq!(output.runs(&merged), [ColorRun::new(4, 4)]);
    assert_eq!(round_trip(&output, &merged), [4, 5, 6, 7]);
}

/// The ascending feed the compactor usually sees must never make a set hold
/// more than the one run it canonically is, at any moment.
#[test]
fn an_ascending_feed_never_grows_past_one_run() {
    let mut arena = ColorArena::default();
    let mut set = arena.singleton(0);
    for color in 1..100_000u32 {
        arena.extend(&mut set, &[ColorRun::single(color)]);
        assert_eq!(set.runs_count(), 1);
    }
    assert_eq!(arena.used_capacity(), 0);
    assert_eq!(arena.runs(&set), [ColorRun::new(0, 100_000)]);
}

/// The compactor folds chunks newest first, so blocks of ascending runs arrive
/// descending. Appending without compacting would hold one record per append
/// until the single `prepare` before serialization, which is twice the memory of
/// storing the colors outright -- the opposite of the point.
#[test]
fn a_descending_feed_stays_bounded() {
    const COUNT: u32 = 100_000;
    let mut arena = ColorArena::default();
    let mut set = arena.singleton(COUNT);
    for color in (0..COUNT).rev() {
        arena.extend(&mut set, &[ColorRun::single(color)]);
        assert!(
            set.runs_count() <= 2 * COMPACT_FLOOR,
            "held {} records for a set that is one run",
            set.runs_count()
        );
    }
    arena.prepare(&mut set);
    assert_eq!(arena.runs(&set), [ColorRun::new(0, COUNT as u64 + 1)]);
}

/// Interleaved sparse colors really are the worst case, and they must still stay
/// within a constant factor of the canonical record count.
#[test]
fn an_out_of_order_sparse_feed_stays_within_twice_its_runs() {
    let mut arena = ColorArena::default();
    let mut set = arena.singleton(1);
    let mut expected = vec![1u32];
    for step in 0..20_000u32 {
        // Alternating high and low keeps every append out of order.
        let color = if step % 2 == 0 {
            1_000_000 - step
        } else {
            3 + step
        };
        arena.extend(&mut set, &[ColorRun::single(color)]);
        expected.push(color);
    }
    let canonical = {
        expected.sort_unstable();
        expected.dedup();
        let mut runs = Vec::new();
        runs_from_colors(&expected, &mut runs);
        runs
    };
    assert!(
        set.runs_count() <= 2 * canonical.len().max(COMPACT_FLOOR),
        "held {} records against {} canonical runs",
        set.runs_count(),
        canonical.len()
    );
    arena.prepare(&mut set);
    assert_eq!(arena.runs(&set), canonical.as_slice());
}

#[test]
fn long_runs_stay_compact_and_boundaries_round_trip() {
    let mut arena = ColorArena::default();
    let run = arena.from_colors(&(0..1_000_000).collect::<Vec<_>>());
    let mut bytes = Vec::new();
    arena.write_to(&run, &mut bytes);
    // Run count, a shifted gap of zero with the length flag, and the length.
    assert_eq!(bytes.len(), 5);

    let boundary = arena.from_colors(&[u32::MAX - 1, u32::MAX]);
    assert_eq!(round_trip(&arena, &boundary), [u32::MAX - 1, u32::MAX]);

    let top = arena.from_colors(&[u32::MAX]);
    assert_eq!(round_trip(&arena, &top), [u32::MAX]);
}

/// A one-color set is what most superkmers carry, so its size is the one that
/// matters most. Shifting the gap up by a bit to carry the length flag costs a
/// byte where it pushes a value over a seven-bit boundary; on the captured
/// corpus the runs it buys more than repay that, at 12.9% fewer bytes overall
/// than the format it replaces.
#[test]
fn a_one_color_set_costs_two_bytes() {
    let mut arena = ColorArena::default();
    for color in [0u32, 1, 7, 63] {
        let handle = arena.singleton(color);
        let mut bytes = Vec::new();
        arena.write_to(&handle, &mut bytes);
        assert_eq!(bytes.len(), 2, "color {color}");
    }
    // Past the boundary the shifted gap needs a second byte, and no more.
    let handle = arena.singleton(64);
    let mut bytes = Vec::new();
    arena.write_to(&handle, &mut bytes);
    assert_eq!(bytes.len(), 3);
}

#[test]
fn rejects_truncated_and_overflowing_streams() {
    let mut arena = ColorArena::default();
    assert!(arena.decode(&mut &[5u8, 1, 0][..]).is_none());
    assert!(arena.decode(&mut &[][..]).is_none());
    assert!(arena.decode_from_slice(&[5u8, 1, 0]).is_none());
    assert!(arena.decode_from_slice(&[]).is_none());
}

/// A record that runs past the color space is rejected even though the decoder
/// tests the position once per run rather than once per field.
#[test]
fn rejects_colors_past_the_color_space() {
    let mut bytes = Vec::new();
    encode_varint(|b| bytes.extend_from_slice(b), 0);
    // A gap of u32::MAX with a length flag, then a length that leaves the space.
    encode_varint(|b| bytes.extend_from_slice(b), ((u32::MAX as u64) << 1) | 1);
    encode_varint(|b| bytes.extend_from_slice(b), 1);
    let mut arena = ColorArena::default();
    assert!(arena.decode_from_slice(&bytes).is_none());
    assert!(arena.decode(&mut bytes.as_slice()).is_none());

    // The same run one color shorter fits exactly, so the rejection above is the
    // bounds check and not a short read.
    let mut bytes = Vec::new();
    encode_varint(|b| bytes.extend_from_slice(b), 0);
    encode_varint(
        |b| bytes.extend_from_slice(b),
        (((u32::MAX - 3) as u64) << 1) | 1,
    );
    encode_varint(|b| bytes.extend_from_slice(b), 2);
    let (handle, consumed) = arena.decode_from_slice(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    assert_eq!(arena.colors(&handle).last(), Some(u32::MAX));
}

/// Every decoder must agree, and the slice decoder must report exactly the bytes
/// it consumed so a caller can walk to the next record. The sets are built to
/// straddle the eight-byte look-ahead: multi-byte values near the end of a
/// buffer take the byte-at-a-time fallback instead.
#[test]
fn decoders_agree_and_report_the_consumed_length() {
    let mut arena = ColorArena::default();
    let cases: Vec<Vec<ColorIndexType>> = vec![
        vec![0],
        vec![u32::MAX],
        vec![1, 2, 3, 4, 7, 8, 9, 10, 12, 13],
        vec![0, 1 << 7, 1 << 14, 1 << 21, 1 << 28],
        vec![1 << 28, (1 << 28) + 1, (1 << 28) + 2, (1 << 28) + 3],
        vec![1 << 31, (1 << 31) + 1, u32::MAX],
        (0..300).map(|index| index * 1_000_003).collect(),
        (0..300).collect(),
    ];
    for colors in cases {
        let handle = arena.from_colors(&colors);
        let mut bytes = Vec::new();
        arena.write_to(&handle, &mut bytes);
        assert!(bytes.len() <= handle.encoded_len(), "{colors:?}");

        let mut vec_bytes = Vec::new();
        arena.write_to_vec(&handle, &mut vec_bytes);
        assert_eq!(vec_bytes, bytes, "{colors:?}");

        let mut decoded = ColorArena::default();
        let (slice_handle, consumed) = decoded.decode_from_slice(&bytes).unwrap();
        assert_eq!(collect(&decoded, &slice_handle), colors, "{colors:?}");
        assert_eq!(consumed, bytes.len(), "{colors:?}");

        let stream_handle = decoded.decode(&mut bytes.as_slice()).unwrap();
        assert_eq!(collect(&decoded, &stream_handle), colors, "{colors:?}");

        let pointer_handle = unsafe { decoded.decode_from_pointer(bytes.as_ptr()) }.unwrap();
        assert_eq!(collect(&decoded, &pointer_handle), colors, "{colors:?}");
    }
}

/// `encoded_len` is what the bucket dispatcher reserves, so a set that encodes
/// larger than its bound would overrun a bucket buffer.
#[test]
fn encoded_length_bounds_the_worst_case() {
    let mut arena = ColorArena::default();
    // Sparse colors are the worst case: every one of them is its own run and
    // shares in nothing.
    for count in [1usize, 2, 3, 4, 5, 64, 1000] {
        let colors: Vec<ColorIndexType> = (0..count as u32)
            .map(|index| index.wrapping_mul(0x0010_0001) | 0x8000_0000)
            .collect();
        let mut sorted = colors.clone();
        sorted.sort_unstable();
        sorted.dedup();
        let handle = arena.from_colors(&sorted);
        let mut bytes = Vec::new();
        arena.write_to(&handle, &mut bytes);
        assert!(
            bytes.len() <= handle.encoded_len(),
            "{} colors encoded to {} over a bound of {}",
            sorted.len(),
            bytes.len(),
            handle.encoded_len()
        );
    }
}

/// However a set is assembled, the canonical form must be the same array of
/// runs: the global color table interns by hashing exactly these bytes, so two
/// spellings of one set would mint two subset identifiers.
#[test]
fn canonical_form_is_independent_of_assembly_order() {
    let mut rng = StdRng::seed_from_u64(0x5eed);
    for _ in 0..200 {
        let mut colors: Vec<ColorIndexType> = (0..rng.gen_range(1..80))
            .map(|_| rng.gen_range(0..200u32))
            .collect();
        colors.sort_unstable();
        colors.dedup();

        let mut reference = Vec::new();
        runs_from_colors(&colors, &mut reference);

        // Feed the same set in a different order every time, in pieces.
        for attempt in 0..8 {
            let mut shuffled = colors.clone();
            for index in (1..shuffled.len()).rev() {
                let other = rng.gen_range(0..=index);
                shuffled.swap(index, other);
            }

            let mut arena = ColorArena::default();
            let mut handle = ColorHandle::default();
            let chunk = 1 + attempt % 5;
            for piece in shuffled.chunks(chunk) {
                let mut sorted = piece.to_vec();
                sorted.sort_unstable();
                let mut runs = Vec::new();
                runs_from_colors(&sorted, &mut runs);
                arena.extend(&mut handle, &runs);
            }
            arena.prepare(&mut handle);
            assert_eq!(arena.runs(&handle), reference.as_slice(), "{colors:?}");

            // And through the accumulator, which interns by the same route.
            let mut accumulator = ColorAccumulator::default();
            for piece in shuffled.chunks(chunk) {
                let mut sorted = piece.to_vec();
                sorted.sort_unstable();
                accumulator.append_colors(&sorted);
            }
            assert_eq!(accumulator.finish(), reference.as_slice(), "{colors:?}");
        }
    }
}

/// Branching copies the set rather than sharing it, and the copy must be usable
/// on its own.
#[test]
fn branching_leaves_the_original_intact() {
    let mut arena = ColorArena::default();
    let original = arena.from_colors(&[1, 2, 3, 10]);
    let mut branched = arena.branch_extended(&original, &[ColorRun::new(20, 3)]);
    arena.prepare(&mut branched);

    assert_eq!(collect(&arena, &original), [1, 2, 3, 10]);
    assert_eq!(collect(&arena, &branched), [1, 2, 3, 10, 20, 21, 22]);
}

/// Freeing must return the block the set actually owns even after compaction
/// has shrunk it, or the slab grows without bound across a bucket.
#[test]
fn freeing_compacted_sets_recycles_the_slab() {
    let mut arena = ColorArena::default();
    let mut high = 0;
    for round in 0..200 {
        let mut handle = ColorHandle::default();
        for color in (0..2_000u32).rev() {
            arena.extend(&mut handle, &[ColorRun::single(color)]);
        }
        arena.prepare(&mut handle);
        assert_eq!(arena.runs(&handle), [ColorRun::new(0, 2_000)]);
        arena.free(&mut handle);

        if round == 0 {
            high = arena.used_capacity();
        } else {
            assert_eq!(
                arena.used_capacity(),
                high,
                "slab grew on round {round}, so a freed block was stranded"
            );
        }
    }
}

/// Reports what the arena holds for the captured bucket color sets, against
/// what the same sets cost as one `u32` per color.
///
/// Point it at a fixture of little-endian `u32` lengths, each followed by that
/// many sorted unique colors, as
/// `benchmark-results/canonical-color-sets/make_fixture.py` writes:
///
/// ```sh
/// GGCAT_COLOR_RLE_BENCH_INPUT=/tmp/ggcat-rle-color-sets.bin \
///   cargo test --release -p ggcat_colors --lib measure_captured_sets -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn measure_captured_sets() {
    let Ok(path) = std::env::var("GGCAT_COLOR_RLE_BENCH_INPUT") else {
        eprintln!("set GGCAT_COLOR_RLE_BENCH_INPUT to a captured color-set fixture");
        return;
    };
    let raw = std::fs::read(&path).unwrap();

    let mut arena = ColorArena::default();
    let mut handles = Vec::new();
    let mut encoded = Vec::new();
    let mut colors_total = 0usize;
    let mut runs_total = 0usize;
    let mut expanded_slab = 0usize;

    let mut offset = 0;
    while offset + 4 <= raw.len() {
        let count = u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let colors: Vec<ColorIndexType> = raw[offset..offset + count * 4]
            .chunks_exact(4)
            .map(|word| u32::from_le_bytes(word.try_into().unwrap()))
            .collect();
        offset += count * 4;

        let handle = arena.from_colors(&colors);
        arena.write_to_vec(&handle, &mut encoded);
        colors_total += count;
        runs_total += handle.runs_count();
        // What the same set cost as expanded colors: one inline slot, else a
        // power-of-two slab block of four-byte colors.
        expanded_slab += if count <= 1 {
            0
        } else {
            4 * count.next_power_of_two()
        };
        handles.push(handle);
    }

    let run_slab = arena.used_capacity() * std::mem::size_of::<ColorRun>();
    let handle_bytes = handles.len() * std::mem::size_of::<ColorHandle>();
    let inline = handles.iter().filter(|h| h.runs_count() <= 1).count();

    println!(
        "sets {} colors {} runs {} ({:.2} runs/set, {:.1} colors/run)",
        handles.len(),
        colors_total,
        runs_total,
        runs_total as f64 / handles.len() as f64,
        colors_total as f64 / runs_total as f64,
    );
    println!(
        "slab as runs      {:>12} bytes\nslab as colors    {:>12} bytes  ({:.1}x)",
        run_slab,
        expanded_slab,
        expanded_slab as f64 / run_slab as f64,
    );
    println!(
        "handles           {:>12} bytes  ({} of {} sets need no slab at all)",
        handle_bytes,
        inline,
        handles.len()
    );
    println!("encoded           {:>12} bytes", encoded.len());
}
