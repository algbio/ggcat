# GGCAT - compacted and colored de Bruijn graph construction and querying

GGCAT is a tool for building compacted (and optinally colored) de Bruijn graphs from raw sequencing data or for merging multiple existing cDBG in a single graph. It also supports sequence queryies to either a colored or non-colored graph (i.e. number/percentage of present kmers).

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
Here are all listed the available options for graph building:
```
> ggcat build --help
USAGE:
    ggcat build [FLAGS] [OPTIONS] [--] [input]...

FLAGS:
    -c, --colors                            Enable colors
    -f, --forward-only                      Treats reverse complementary kmers as different
    -e, --generate-maximal-unitigs-links    Generate maximal unitigs connections references, in BCALM2 format
                                            L:<+/->:<other id>:<+/->
    -h, --help                              Prints help information
        --keep-temp-files                   Keep intermediate temporary files for debugging purposes
    -p, --prefer-memory                     Use all the given memory before writing to disk
    -V, --version                           Prints version information

OPTIONS:
    -b, --buckets-count-log <buckets-count-log>                              The log2 of the number of buckets
    -w, --hash-type <hash-type>
            Hash type used to identify kmers [default: Auto]

    -l, --input-lists <input-lists>...                                       The lists of input files
        --intermediate-compression-level <intermediate-compression-level>
            The level of lz4 compression to be used for the intermediate files

    -k <klen>                                                                Specifies the k-mers length [default: 32]
        --last-step <last-step>                                               [default: BuildUnitigs]
    -m, --memory <memory>                                                    Maximum memory usage (GB) [default: 2]
    -s, --min-multiplicity <min-multiplicity>
            Minimum multiplicity required to keep a kmer [default: 2]

        --mlen <mlen>
            Overrides the default m-mers (minimizers) length

    -n, --number <number>                                                     [default: 0]
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
Here are listed all the available options for graph querying:
```
> ggcat query --help
USAGE:
    ggcat query [FLAGS] [OPTIONS] <input-graph> <input-query>

FLAGS:
    -c, --colors             Enable colors
    -f, --forward-only       Treats reverse complementary kmers as different
    -h, --help               Prints help information
        --keep-temp-files    Keep intermediate temporary files for debugging purposes
    -p, --prefer-memory      Use all the given memory before writing to disk
    -V, --version            Prints version information

OPTIONS:
    -b, --buckets-count-log <buckets-count-log>                              The log2 of the number of buckets
    -w, --hash-type <hash-type>
            Hash type used to identify kmers [default: Auto]

        --intermediate-compression-level <intermediate-compression-level>
            The level of lz4 compression to be used for the intermediate files

    -k <klen>                                                                Specifies the k-mers length [default: 32]
    -m, --memory <memory>                                                    Maximum memory usage (GB) [default: 2]
        --mlen <mlen>
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
git clone https://github.com/Guilucand/ggcat --recursive
cd ggcat/
cargo install --path .
```
the binary is automatically copied to ```$HOME/.cargo/bin```

To launch the tool directly from the command line, the above directory should be added to the ```$PATH``` variable. 
