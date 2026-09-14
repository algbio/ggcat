use super::*;
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

fn legacy_encode(colors: &[u32]) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_varint(|b| bytes.extend_from_slice(b), (colors.len() - 1) as u64);
    let mut last = 0;
    for &color in colors {
        encode_varint(|b| bytes.extend_from_slice(b), (color - last) as u64);
        last = color;
    }
    bytes
}

fn roundtrip(colors: &[u32], bytes: &[u8], buffer: &mut ColorArena) {
    buffer.reset();
    let mut cursor = Cursor::new(bytes);
    let extra = MinBkMultipleColors::decode_extended(buffer, &mut cursor, (), 0).unwrap();
    assert_eq!(buffer.colors(&extra.0).to_vec(), colors);
    assert_eq!(cursor.position(), bytes.len() as u64);
}

#[test]
fn threshold_and_multiple_runs() {
    let mut buffer = ColorArena::default();
    for (colors, expected) in [
        (vec![0], vec![0, 0]),
        (vec![1, 2], vec![1, 1, 1]),
        (vec![1, 2, 3], vec![2, 1, 1, 1]),
        (vec![1, 2, 3, 4], vec![3, 1, 0, 0]),
        (
            vec![1, 2, 3, 4, 7, 8, 9, 10, 12, 13],
            vec![9, 1, 0, 0, 3, 0, 0, 2, 1],
        ),
    ] {
        let bytes = encode(&colors, &mut buffer);
        assert_eq!(bytes, expected);
        roundtrip(&colors, &bytes, &mut buffer);
        // The new decoder also accepts the old all-positive-delta format.
        roundtrip(&colors, &legacy_encode(&colors), &mut buffer);
        let extra =
            MinBkMultipleColors::decode_from_slice_extended(&mut buffer, &bytes, (), 0).unwrap();
        assert_eq!(buffer.colors(&extra.0).to_vec(), colors);
        let extra = unsafe {
            MinBkMultipleColors::decode_from_pointer_extended(&mut buffer, bytes.as_ptr(), (), 0)
        }
        .unwrap();
        assert_eq!(buffer.colors(&extra.0).to_vec(), colors);
    }
}

#[test]
fn boundaries_and_random_sets_never_expand_serialized_size() {
    let mut buffer = ColorArena::default();
    for len in [1, 2, 3, 4, 5, 127, 128, 131, 132, 16387, 16388] {
        let upper = u32::MAX;
        for start in [0, 63, 127, 16383, upper - len + 1] {
            let colors: Vec<_> = (0..len).map(|offset| start + offset).collect();
            let bytes = encode(&colors, &mut buffer);
            assert!(bytes.len() <= legacy_encode(&colors).len());
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
        assert!(bytes.len() <= legacy_encode(&colors).len());
        roundtrip(&colors, &bytes, &mut buffer);
    }
    let colors: Vec<_> = (0..1_000_000).collect();
    let bytes = encode(&colors, &mut buffer);
    assert_eq!(bytes.len(), 8);
    roundtrip(&colors, &bytes, &mut buffer);
}

#[test]
fn prepare_deduplicates_before_range_encoding_and_records_stay_separate() {
    let mut buffer = ColorArena::default();
    let mut extra = store(&[4, 2, 3, 1, 3, 2, 4], &mut buffer);
    extra.prepare_for_serialization(&mut buffer);
    let mut bytes = Vec::new();
    extra.encode_extended(&buffer, &mut bytes, (), 31, false, 0);
    assert_eq!(bytes, [3, 1, 0, 0]);
    bytes.extend_from_slice(&legacy_encode(&[0, 99]));
    let mut cursor = Cursor::new(&bytes);
    let first = MinBkMultipleColors::decode_extended(&mut buffer, &mut cursor, (), 0).unwrap();
    assert_eq!(buffer.colors(&first.0).to_vec(), [1, 2, 3, 4]);
    let second = MinBkMultipleColors::decode_extended(&mut buffer, &mut cursor, (), 0).unwrap();
    assert_eq!(buffer.colors(&second.0).to_vec(), [0, 99]);
    assert_eq!(cursor.position(), bytes.len() as u64);
}

#[test]
fn rejects_truncated_ranges_invalid_lengths_and_color_overflow() {
    let mut buffer = ColorArena::default();
    let mut overflow = legacy_encode(&[u32::MAX]);
    overflow[0] = 3;
    overflow.extend_from_slice(&[0, 0]);
    let mut delta_overflow = legacy_encode(&[u32::MAX]);
    delta_overflow[0] = 1;
    delta_overflow.push(1);
    for bytes in [
        vec![3, 1, 0],
        vec![2, 1, 0, 0],
        vec![3, 1, 0, 128],
        overflow,
        delta_overflow,
    ] {
        assert!(
            MinBkMultipleColors::decode_from_slice_extended(&mut buffer, &bytes, (), 0).is_none()
        );
        buffer.reset();
    }
}

// Dataset benchmarks live in color_set::tests.
