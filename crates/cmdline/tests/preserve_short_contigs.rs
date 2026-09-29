use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn build(dir: &Path, inputs: &[&Path], output: &Path, flags: &[&str]) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ggcat"));
    command.current_dir(dir).arg("build");
    for input in inputs {
        command.arg(input);
    }
    let result = command
        .args([
            "-k",
            "7",
            "--minimizer-length",
            "3",
            "-s",
            "1",
            "-j",
            "2",
            "-o",
        ])
        .arg(output)
        .args(flags)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn short_contigs_reach_fasta_gfa_and_colored_outputs() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "ggcat-short-contigs-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&dir).unwrap();
    let first = dir.join("first.fa");
    let second = dir.join("second.fa");
    fs::write(&first, b">one\nACG\n>two\nTTA\n").unwrap();
    fs::write(&second, b">three\nGGT\n").unwrap();

    let ordinary = dir.join("ordinary.fa");
    build(&dir, &[&first], &ordinary, &[]);
    assert!(fs::read(&ordinary).unwrap().is_empty());

    let fasta = dir.join("preserved.fa");
    build(&dir, &[&first], &fasta, &["--preserve-short-contigs"]);
    let result = fs::read_to_string(&fasta).unwrap();
    assert!(result.contains("\nACG\n"));
    assert!(result.contains("\nTTA\n"));

    let gfa = dir.join("preserved.gfa");
    build(
        &dir,
        &[&first],
        &gfa,
        &["--preserve-short-contigs", "--gfa-v1"],
    );
    let result = fs::read_to_string(&gfa).unwrap();
    assert!(result.contains("\tACG\tLN:i:3\n"));
    assert!(result.contains("\tTTA\tLN:i:3\n"));

    let gfa_v2 = dir.join("preserved-v2.gfa");
    build(
        &dir,
        &[&first],
        &gfa_v2,
        &["--preserve-short-contigs", "--gfa-v2"],
    );
    let result = fs::read_to_string(&gfa_v2).unwrap();
    assert!(result.contains("\t3\tACG\n"));
    assert!(result.contains("\t3\tTTA\n"));

    let linked = dir.join("linked.fa");
    build(
        &dir,
        &[&first],
        &linked,
        &[
            "--preserve-short-contigs",
            "--generate-maximal-unitigs-links",
        ],
    );
    let result = fs::read_to_string(&linked).unwrap();
    assert!(result.contains("\nACG\n"));
    assert!(result.contains("\nTTA\n"));

    let colored = dir.join("colored.fa");
    build(
        &dir,
        &[&first, &second],
        &colored,
        &["--preserve-short-contigs", "--colors"],
    );
    let result = fs::read_to_string(&colored).unwrap();
    assert!(result.contains("\nACG\n"));
    assert!(result.contains("\nTTA\n"));
    assert!(result.contains("\nGGT\n"));
    assert!(result.lines().filter(|line| line.contains(" C:")).count() == 3);

    let mixed = dir.join("mixed.fa");
    fs::write(
        &mixed,
        b">short\nACGTAC\n>ambiguous\nAN\n>exactly_k\nTTTTTTT\n",
    )
    .unwrap();
    let compressed = dir.join("mixed.fa.lz4");
    build(&dir, &[&mixed], &compressed, &["--preserve-short-contigs"]);
    let mut decoded = String::new();
    lz4::Decoder::new(fs::File::open(compressed).unwrap())
        .unwrap()
        .read_to_string(&mut decoded)
        .unwrap();
    assert!(decoded.contains("LN:i:6\nACGTAC\n"));
    assert!(decoded.contains("LN:i:2\nAN\n"));
    assert!(decoded.contains("LN:i:7\nAAAAAAA\n") || decoded.contains("LN:i:7\nTTTTTTT\n"));
    fs::remove_dir_all(dir).unwrap();
}
