use super::*;
use crate::bucket_colors::runs_from_colors;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::io::Cursor;

fn store(colors: &[u32], buffer: &mut ColorArena) -> MinBkMultipleColors {
    let mut handle = buffer.from_colors(colors);
    buffer.prepare(&mut handle);
    MinBkMultipleColors(handle)
}

fn encode(colors: &[u32], buffer: &mut ColorArena) -> Vec<u8> {
    buffer.reset();
    assert!(
        colors.windows(2).all(|p| p[0] < p[1]),
        "invalid input: {colors:?}"
    );
    let extra = store(colors, buffer);
    let mut bytes = Vec::new();
    extra.encode_extended(buffer, &mut bytes, (), 31, false, 0);
    assert!(bytes.len() <= extra.max_size());
    bytes
}

fn decoded(buffer: &ColorArena, extra: &MinBkMultipleColors) -> Vec<u32> {
    buffer.colors(&extra.0).collect()
}

/// The size of the plain delta encoding these records replace: a count and one
/// varint per color, with no run compression at all.
fn plain_delta_len(colors: &[u32]) -> usize {
    let mut bytes = Vec::new();
    encode_varint(|b| bytes.extend_from_slice(b), (colors.len() - 1) as u64);
    let mut last = 0;
    for &color in colors {
        encode_varint(|b| bytes.extend_from_slice(b), (color - last) as u64);
        last = color;
    }
    bytes.len()
}

fn runs_count(colors: &[u32]) -> usize {
    let mut runs = Vec::new();
    runs_from_colors(colors, &mut runs);
    runs.len()
}

fn roundtrip(colors: &[u32], bytes: &[u8], buffer: &mut ColorArena) {
    buffer.reset();
    let mut cursor = Cursor::new(bytes);
    let extra = MinBkMultipleColors::decode_extended(buffer, &mut cursor, (), 0).unwrap();
    assert_eq!(decoded(buffer, &extra), colors);
    assert_eq!(cursor.position(), bytes.len() as u64);
}

#[test]
fn records_frame_themselves_and_keep_one_color_at_two_bytes() {
    let mut buffer = ColorArena::default();
    for (colors, expected) in [
        // One run, gap 0, no length: the case most superkmers carry.
        (vec![0], vec![0, 0]),
        // One run of two: the gap is shifted up and the freed bit says a length
        // follows, biased by two.
        (vec![1, 2], vec![0, 3, 0]),
        (vec![1, 2, 3], vec![0, 3, 1]),
        (vec![1, 2, 3, 4], vec![0, 3, 2]),
        // Three runs: count minus one, then a gap measured from the previous
        // run's last color.
        (
            vec![1, 2, 3, 4, 7, 8, 9, 10, 12, 13],
            vec![2, 3, 2, 7, 2, 5, 0],
        ),
        // Sparse colors pay only a shifted gap each; 99 shifts to 198, which
        // needs a second varint byte.
        (vec![0, 99], vec![1, 0, 198, 1]),
    ] {
        let bytes = encode(&colors, &mut buffer);
        assert_eq!(bytes, expected, "{colors:?}");
        roundtrip(&colors, &bytes, &mut buffer);
        let extra =
            MinBkMultipleColors::decode_from_slice_extended(&mut buffer, &bytes, (), 0).unwrap();
        assert_eq!(decoded(&buffer, &extra), colors);
        let extra = unsafe {
            MinBkMultipleColors::decode_from_pointer_extended(&mut buffer, bytes.as_ptr(), (), 0)
        }
        .unwrap();
        assert_eq!(decoded(&buffer, &extra), colors);
    }
}

/// Against the plain delta encoding, a record can only lose the one bit the gap
/// is shifted by, and never more than a byte per run: a run's length varint is
/// always paid for by the unit deltas it replaces.
#[test]
fn boundaries_and_random_sets_stay_within_a_byte_per_run() {
    let mut buffer = ColorArena::default();
    for len in [1, 2, 3, 4, 5, 127, 128, 131, 132, 16387, 16388] {
        let upper = u32::MAX;
        for start in [0, 63, 127, 16383, upper - len + 1] {
            let colors: Vec<_> = (0..len).map(|offset| start + offset).collect();
            let bytes = encode(&colors, &mut buffer);
            assert!(bytes.len() <= plain_delta_len(&colors) + runs_count(&colors));
            roundtrip(&colors, &bytes, &mut buffer);
        }
    }
    let mut rng = StdRng::seed_from_u64(9710);
    for _ in 0..2000 {
        let mut colors = vec![rng.gen_range(0..100000)];
        for _ in 0..rng.gen_range(0..512) {
            let gap = if rng.gen_bool(0.7) {
                1
            } else {
                rng.gen_range(2..100000)
            };
            colors.push(colors.last().unwrap() + gap);
        }
        let bytes = encode(&colors, &mut buffer);
        assert!(
            bytes.len() <= plain_delta_len(&colors) + runs_count(&colors),
            "{} bytes for {} colors in {} runs",
            bytes.len(),
            colors.len(),
            runs_count(&colors)
        );
        roundtrip(&colors, &bytes, &mut buffer);
    }
    let colors: Vec<_> = (0..1_000_000).collect();
    let bytes = encode(&colors, &mut buffer);
    assert_eq!(bytes.len(), 5);
    roundtrip(&colors, &bytes, &mut buffer);
}

#[test]
fn prepare_deduplicates_before_encoding_and_records_stay_separate() {
    let mut buffer = ColorArena::default();
    let mut extra = store(&[4, 2, 3, 1, 3, 2, 4], &mut buffer);
    extra.prepare_for_serialization(&mut buffer);
    let mut bytes = Vec::new();
    extra.encode_extended(&buffer, &mut bytes, (), 31, false, 0);
    assert_eq!(bytes, [0, 3, 2]);

    let second_extra = store(&[0, 99], &mut buffer);
    second_extra.encode_extended(&buffer, &mut bytes, (), 31, false, 0);

    let mut cursor = Cursor::new(&bytes);
    let first = MinBkMultipleColors::decode_extended(&mut buffer, &mut cursor, (), 0).unwrap();
    assert_eq!(decoded(&buffer, &first), [1, 2, 3, 4]);
    let second = MinBkMultipleColors::decode_extended(&mut buffer, &mut cursor, (), 0).unwrap();
    assert_eq!(decoded(&buffer, &second), [0, 99]);
    assert_eq!(cursor.position(), bytes.len() as u64);
}

#[test]
fn rejects_truncated_records_and_color_overflow() {
    let mut buffer = ColorArena::default();

    let varint = |value: u64| {
        let mut bytes = Vec::new();
        encode_varint(|b| bytes.extend_from_slice(b), value);
        bytes
    };

    // A count with no body, and a record whose length varint never arrives.
    let mut header_only = varint(0);
    let mut missing_length = varint(0);
    missing_length.extend_from_slice(&varint((1 << 1) | 1));

    // Four records promised, one delivered.
    let mut short_count = varint(3);
    short_count.extend_from_slice(&varint(2));

    // A run that starts at the top of the space and runs off the end.
    let mut overflow = varint(0);
    overflow.extend_from_slice(&varint(((u32::MAX as u64) << 1) | 1));
    overflow.extend_from_slice(&varint(1));

    // A second record whose gap leaves the space: the first run ends at 1, so a
    // gap of the whole space puts the second run one past the last color.
    let mut gap_overflow = varint(1);
    gap_overflow.extend_from_slice(&varint(1 << 1));
    gap_overflow.extend_from_slice(&varint((u32::MAX as u64) << 1));

    for bytes in [
        std::mem::take(&mut header_only),
        missing_length,
        short_count,
        overflow,
        gap_overflow,
        vec![],
    ] {
        assert!(
            MinBkMultipleColors::decode_from_slice_extended(&mut buffer, &bytes, (), 0).is_none(),
            "{bytes:?}"
        );
        buffer.reset();
    }
}
