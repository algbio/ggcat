# BiLoki

BiLoki is a tool for building compacted De Brujin graphs from raw sequencing data or for merging multiple existing cDBG in a single graph. It has a speedup from x8 to x32 wrt. other publicly available tools for cDBG construction.

## Tool usage

After building and installing the tool following the below instructions, launch it with ```--help``` to display the command line options:
```
BiLoki 0.1.0

USAGE:
    BiLoki [FLAGS] [OPTIONS] <input>...

FLAGS:
    -d                       
    -h, --help               Prints help information
        --keep-temp-files    Keep intermediate temporary files for debugging purposes
    -V, --version            Prints version information

OPTIONS:
    -k <klen>                                      Specifies the k-mers length [default: 32]
    -s, --min-multiplicity <min-multiplicity>      Minimum multiplicity required to keep a kmer [default: 2]
    -m <mlen>
            Specifies the m-mers (minimizers) length, defaults to min(12, ceil(K / 2))

    -n, --number <number>                           [default: 0]
    -o, --output-file <output-file>                 [default: output.fasta.lz4]
    -q, --quality-threshold <quality-threshold>
            Minimum correctness probability for each kmer (using fastq quality checks)

    -x, --step <step>                               [default: MinimizerBucketing]
    -t, --temp-dir <temp-dir>
            Directory for temporary files (default .temp_files) [default: .temp_files]

    -j, --threads-count <threads-count>             [default: 16]

ARGS:
    <input>...    The input files

```

## Installation
At the moment building from source is the only option to install the tool.
To build the tool the Rust nightly toolchain is required, and can be downloaded with the following commands:

### Linux/Mac

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install nightly

```

### Windows
Follow the instructions at the site:
https://rustup.rs/

The building process was not tested on windows, but it should work with minor tweaks.

### Building

Then the tool can be installed with the commands:
```
git clone https://github.com/Guilucand/biloki --recursive
cd biloki/
cargo install --path .
```
the binary is automatically copied to ```$HOME/.cargo/bin```

To launch the tool directly from the command line, the above directory should be added to the ```$PATH``` variable. 
