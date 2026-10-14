use super::*;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::io::Cursor;

fn store(colors: &[u32], buffer: &mut AllocatorU32) -> MinBkMultipleColors {
    let mut vec = buffer.new_vec(colors.len());
    buffer.slice_vec_mut(&mut vec).copy_from_slice(colors);
    MinBkMultipleColors(vec)
}

fn encode(colors: &[u32], buffer: &mut AllocatorU32) -> Vec<u8> {
    buffer.reset();
    assert!(
        colors.windows(2).all(|p| p[0] < p[1]),
        "invalid input: {colors:?}"
    );
    let extra = store(colors, buffer);
    assert_eq!(buffer.slice_vec(&extra.0), colors);
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

fn roundtrip(colors: &[u32], bytes: &[u8], buffer: &mut AllocatorU32) {
    buffer.reset();
    let mut cursor = Cursor::new(bytes);
    let extra = MinBkMultipleColors::decode_extended(buffer, &mut cursor, (), 0).unwrap();
    assert_eq!(buffer.slice_vec(&extra.0), colors);
    assert_eq!(cursor.position(), bytes.len() as u64);
}

#[test]
fn threshold_and_multiple_runs() {
    let mut buffer = AllocatorU32::default();
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
        assert_eq!(buffer.slice_vec(&extra.0), colors);
        let extra = unsafe {
            MinBkMultipleColors::decode_from_pointer_extended(&mut buffer, bytes.as_ptr(), (), 0)
        }
        .unwrap();
        assert_eq!(buffer.slice_vec(&extra.0), colors);
    }
}

#[test]
fn boundaries_and_random_sets_never_expand_serialized_size() {
    let mut buffer = AllocatorU32::default();
    for len in [1, 2, 3, 4, 5, 127, 128, 131, 132, 16387, 16388] {
        // The existing inline allocator reserves the high bit of its second
        // inline color as a pointer tag; use its supported limit for pairs.
        let upper = if len == 2 { i32::MAX as u32 } else { u32::MAX };
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
    let mut buffer = AllocatorU32::default();
    let mut extra = store(&[4, 2, 3, 1, 3, 2, 4], &mut buffer);
    extra.prepare_for_serialization(&mut buffer);
    let mut bytes = Vec::new();
    extra.encode_extended(&buffer, &mut bytes, (), 31, false, 0);
    assert_eq!(bytes, [3, 1, 0, 0]);
    bytes.extend_from_slice(&legacy_encode(&[0, 99]));
    let mut cursor = Cursor::new(&bytes);
    let first = MinBkMultipleColors::decode_extended(&mut buffer, &mut cursor, (), 0).unwrap();
    assert_eq!(buffer.slice_vec(&first.0), [1, 2, 3, 4]);
    let second = MinBkMultipleColors::decode_extended(&mut buffer, &mut cursor, (), 0).unwrap();
    assert_eq!(buffer.slice_vec(&second.0), [0, 99]);
    assert_eq!(cursor.position(), bytes.len() as u64);
}

#[test]
fn rejects_truncated_ranges_invalid_lengths_and_color_overflow() {
    let mut buffer = AllocatorU32::default();
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

// Input: repeated little-endian u32 lengths followed by that many u32 colors.
// Kept out of the normal test suite because it requires captured dataset colors.
#[test]
#[ignore = "set GGCAT_COLOR_RLE_BENCH_INPUT to a color-set fixture"]
fn benchmark_captured_color_sets() {
    use byteorder::LittleEndian;
    use std::{hint::black_box, time::Instant};
    let data = std::fs::read(std::env::var_os("GGCAT_COLOR_RLE_BENCH_INPUT").unwrap()).unwrap();
    let mut reader = Cursor::new(&data);
    let mut buffer = AllocatorU32::default();
    let mut sets = Vec::new();
    while reader.position() < data.len() as u64 {
        let len = reader.read_u32::<LittleEndian>().unwrap() as usize;
        let mut vec = buffer.new_vec(len);
        for color in buffer.slice_vec_mut(&mut vec) {
            *color = reader.read_u32::<LittleEndian>().unwrap();
        }
        sets.push(MinBkMultipleColors(vec));
    }
    let mut timings = Vec::new();
    for compressed in [false, true] {
        let mut bytes = Vec::with_capacity(data.len());
        let start = Instant::now();
        for _ in 0..10 {
            bytes.clear();
            for extra in &sets {
                if compressed {
                    extra.encode_extended(&buffer, &mut bytes, (), 31, false, 0);
                } else {
                    let colors = buffer.slice_vec(&extra.0);
                    encode_varint(|b| bytes.extend_from_slice(b), (colors.len() - 1) as u64);
                    let mut last = 0;
                    for &color in colors {
                        encode_varint(|b| bytes.extend_from_slice(b), (color - last) as u64);
                        last = color;
                    }
                }
            }
            black_box(&bytes);
        }
        let encode_time = start.elapsed() / 10;
        let mut decoded_buffer = AllocatorU32::default();
        // Verify every set, including record boundaries, against the source.
        let mut reader = Cursor::new(&bytes);
        for extra in &sets {
            let decoded =
                MinBkMultipleColors::decode_extended(&mut decoded_buffer, &mut reader, (), 0)
                    .unwrap();
            assert_eq!(
                decoded_buffer.slice_vec(&decoded.0),
                buffer.slice_vec(&extra.0)
            );
            decoded_buffer.reset();
        }
        assert_eq!(reader.position(), bytes.len() as u64);
        let start = Instant::now();
        for _ in 0..10 {
            let mut reader = Cursor::new(&bytes);
            for _ in &sets {
                decoded_buffer.reset();
                let decoded = if compressed {
                    MinBkMultipleColors::decode_extended(&mut decoded_buffer, &mut reader, (), 0)
                        .unwrap()
                } else {
                    // Original delta-only decoder, for the timing baseline.
                    let len = decode_varint(|| reader.read_u8().ok()).unwrap() as usize + 1;
                    let mut vec = decoded_buffer.new_vec(len);
                    let colors = decoded_buffer.slice_vec_mut(&mut vec);
                    colors[0] = decode_varint(|| reader.read_u8().ok()).unwrap() as u32;
                    for i in 1..len {
                        colors[i] =
                            colors[i - 1] + decode_varint(|| reader.read_u8().ok()).unwrap() as u32;
                    }
                    MinBkMultipleColors(vec)
                };
                black_box(decoded_buffer.slice_vec(&decoded.0));
            }
            assert_eq!(reader.position(), bytes.len() as u64);
        }
        let decode_time = start.elapsed() / 10;
        timings.push((compressed, bytes.len(), encode_time, decode_time));
    }
    assert!(timings[1].1 <= timings[0].1);
    println!(
        "{} real color sets: (RLE, serialized bytes, mean encode time, mean decode time) {:?}",
        sets.len(),
        timings
    );
}
