[![BioConda Install](https://img.shields.io/conda/dn/bioconda/ggcat.svg?style=flag&label=BioConda%20install)](https://anaconda.org/bioconda/ggcat)
[![License](https://img.shields.io/github/license/algbio/ggcat)](https://mit-license.org/)
[![GitHub release (latest by date)](https://img.shields.io/github/v/release/algbio/ggcat)](https://github.com/algbio/ggcat/releases/)
[![GitHub Downloads](https://img.shields.io/github/downloads/algbio/ggcat/total.svg?style=social&logo=github&label=Download)](https://github.com/algbio/ggcat/releases)

# GGCAT - compacted and colored de Bruijn graph construction and querying

GGCAT is a tool for building compacted (and optionally colored) de Bruijn graphs from raw sequencing data or for merging multiple existing cDBG in a single graph. It also supports sequence queryies to either a colored or non-colored graph (i.e. number/percentage of present kmers).

## Install

GGCAT can be downloaded from https://github.com/algbio/ggcat/releases or installed via conda:

```
conda install  -c conda-forge -c bioconda ggcat
```

## Tool usage

### Build a new graph

To build a new graph with a specified k of some input files, run:

```
ggcat build -k <k_value> -j <threads_count> <input_files> -o <output_file>
```

Or if you have a file with a list of input files:

```
ggcat build -k <k_value> -j <threads_count> -l <input_files_list> -o <output_file>
```

#### Building a colored graph

To build a colored graph, add the `-c` flag to the above commands

By default the color name is equal to the file name, this behavior can be overridden
by specifying color names with associated input files in a separate file, and by passing it to ggcat with the `-d` flag. The color and file in each line should be separated by one `<TAB>` character.

Example `color_mapping.in`:

```
color1	file1.fa
color2	file2.fa
color2	file3.fa
color1	dir/file4.fa
color3	dir2/file5.fa
```

Then the graph can be built with the command:

```
ggcat build -k <k_value> -j <threads_count> -c -d color_mapping.in -o <output_file>
```

#### Building links

To build links between maximal unitigs in BCALM2 like format, use the `-e` flag

#### Building minimum-plain text representations of kmer sets

Unitigs are a plain-text representation of the set of kmers in the input reads / genomes, but not of minimum size. GGCAT integrates the [matchtigs & eulertigs](https://github.com/algbio/matchtigs) libraries. These libraries assume a set of maximal unitigs as input, and compute such minimum representations, allowing or forbidding repetitions of kmers, respectively. To build greedy matchtigs, use the `-g` flag; to build eulertigs, use the `--eulertigs` flag; to build a greedy version of eulertigs, use the `--pathtigs` flag.

Here are all listed the available options for graph building:

```
> ggcat build --help
USAGE:
    ggcat build [FLAGS] [OPTIONS] --kmer-length <kmer-length> [--] [input]...

FLAGS:
    -c, --colors                            Enable colors
        --disable-disk-optimization         Disables the disk compaction optimization
        --eulertigs                         Generate eulertigs instead of maximal unitigs
        --fast-eulertigs                    Generate eulertigs instead of maximal unitigs, faster version
        --fast-simplitigs                   Generate simplitigs instead of maximal unitigs, faster version
    -f, --forward-only                      Treats reverse complementary kmers as different
    -e, --generate-maximal-unitigs-links    Generate maximal unitigs connections references, in BCALM2 format
                                            L:<+/->:<other id>:<+/->
        --gfa-v1                            Output the graph in GFA format v1
        --gfa-v2                            Output the graph in GFA format v2
    -g, --greedy-matchtigs                  Generate greedy matchtigs instead of maximal unitigs
    -h, --help                              Prints help information
        --keep-temp-files                   Keep intermediate temporary files for debugging purposes
        --pathtigs                          Generate pathtigs instead of maximal unitigs
    -p, --prefer-memory                     Use all the given memory before writing to disk
    -V, --version                           Prints version information

OPTIONS:
    -b, --buckets-count-log <buckets-count-log>                              The log2 of the number of buckets
    -d, --colored-input-lists <colored-input-lists>...
            The lists of input files with colors in format <COLOR_NAME><TAB><FILE_PATH>

    -w, --hash-type <hash-type>
            Hash type used to identify kmers [default: Auto]

    -l, --input-lists <input-lists>...                                       The lists of input files
        --intermediate-compression-level <intermediate-compression-level>
            The level of lz4 compression to be used for the intermediate files

    -k, --kmer-length <kmer-length>                                          Specifies the k-mers length
        --last-step <last-step>                                               [default: BuildUnitigs]
    -m, --memory <memory>
            Maximum suggested memory usage (GB) The tool will try use only up to this GB of memory to store temporary
            files without writing to disk. This usage does not include the needed memory for the processing steps. GGCAT
            can allocate extra memory for files if the current memory is not enough to complete the current operation
            [default: 2]
    -s, --min-multiplicity <min-multiplicity>
            Minimum multiplicity required to keep a kmer [default: 2]

        --minimizer-length <minimizer-length>
            Overrides the default m-mers (minimizers) length

    -o, --output-file <output-file>                                           [default: output.fasta.lz4]
        --step <step>                                                         [default: MinimizerBucketing]
    -t, --temp-dir <temp-dir>
            Directory for temporary files (default .temp_files) [default: .temp_files]

    -j, --threads-count <threads-count>                                       [default: 16]

ARGS:
    <input>...    The input files
```

### Querying a graph

To query an uncolored graph use the command:

```
ggcat query -k <k_value> -j <threads_count> <input-graph> <input-query>
```

The provided k value must match the one used for graph construction.
To query a colored graph use the command:

```
ggcat query --colors -k <k_value> -j <threads_count> <input-graph> <input-query>
```

The tool automatically searches for the colormap file associated with the
input graph, that must have the same name as the graph with extension '.colors.dat'

The colors in the output are by default represented by an integer, to recover a mapping between the integers
and the color filenames, use the command `ggcat dump-colors <colormap> <output_file>`.

If you instead want the color file names to be written directly in the query output (leading to a potentially much bigger output file),
pass the option `-f JsonLinesWithNames`.

Here are listed all the available options for graph querying:

```
> ggcat query --help
USAGE:
    ggcat query [FLAGS] [OPTIONS] <input-graph> <input-query> --kmer-length <kmer-length>

FLAGS:
    -c, --colors             Enable colors
    -f, --forward-only       Treats reverse complementary kmers as different
    -h, --help               Prints help information
        --keep-temp-files    Keep intermediate temporary files for debugging purposes
    -p, --prefer-memory      Use all the given memory before writing to disk
    -V, --version            Prints version information

OPTIONS:
    -b, --buckets-count-log <buckets-count-log>                              The log2 of the number of buckets
        --colored-query-output-format <colored-query-output-format>
    -w, --hash-type <hash-type>
            Hash type used to identify kmers [default: Auto]

        --intermediate-compression-level <intermediate-compression-level>
            The level of lz4 compression to be used for the intermediate files

    -k, --kmer-length <kmer-length>                                          Specifies the k-mers length
    -m, --memory <memory>
            Maximum suggested memory usage (GB) The tool will try use only up to this GB of memory to store temporary
            files without writing to disk. This usage does not include the needed memory for the processing steps. GGCAT
            can allocate extra memory for files if the current memory is not enough to complete the current operation
            [default: 2]
        --minimizer-length <minimizer-length>
            Overrides the default m-mers (minimizers) length

    -o, --output-file-prefix <output-file-prefix>                             [default: output]
    -x, --step <step>                                                         [default: MinimizerBucketing]
    -t, --temp-dir <temp-dir>
            Directory for temporary files (default .temp_files) [default: .temp_files]

    -j, --threads-count <threads-count>                                       [default: 16]

ARGS:
    <input-graph>    The input graph
    <input-query>    The input query as a .fasta file
```

## Building from source

To build the tool the Rust stable (>= 1.75) toolchain is required, and can be downloaded with the following commands:

### Linux/Mac

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install stable

```

### Windows

Follow the instructions at the site:
https://rustup.rs/

### Additional opt-in features

Additional features can be enabled by specifying them in the command line while building/installing GGCAT (ex. --features "feature1,feature2"):

- **kmer-counters**: Adds kmer abundance for each unitig, in a BCALM2 compatible format. If enabled GGCAT uses more memory while building colored graphs

### Building

Then the tool can be installed with the commands:

```
git clone https://github.com/algbio/ggcat
cd ggcat/
cargo install --path crates/cmdline/ --locked
```

the binary is automatically copied to `$HOME/.cargo/bin`

To launch the tool directly from the command line, the above directory should be added to the `$PATH` variable.

## API usage

GGCAT has an API for both Rust and C++.

### Rust

Add a dependency to the crates/api/ crate to use it in your project.
Check crates/api/example for usage examples.

### C++

Run the makefile inside crates/capi/ggcat-cpp-api to build the library.
Check crates/capi/ggcat-cpp-api/example for usage examples.

## Citing

If you use GGCAT in your research, please cite the following article:

### [GGCAT](https://doi.org/10.1101/2022.10.24.513174)

> Extremely-fast construction and querying of compacted and colored de Bruijn graphs with GGCAT
> Andrea Cracco, Alexandru I. Tomescu
> Genome Research gr.277615.122; doi: https://doi.org/10.1101/gr.277615.122

If you use a matchtigs/eulertigs output, please also cite the following articles:

#### [Matchtigs](https://doi.org/10.1101/2021.12.15.472871)

> Matchtigs: minimum plain text representation of kmer sets
> Sebastian Schmidt, Shahbaz Khan, Jarno Alanko, Alexandru I. Tomescu
> bioRxiv 2021.12.15.472871; doi: https://doi.org/10.1101/2021.12.15.472871

#### [Eulertigs](https://doi.org/10.4230/LIPIcs.WABI.2022.2)

> Eulertigs: minimum plain text representation of k-mer sets without repetitions in linear time
> Sebastian Schmidt and Jarno N. Alanko
> WABI 2022. [10.4230/LIPIcs.WABI.2022.2](https://doi.org/10.4230/LIPIcs.WABI.2022.2).
