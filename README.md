# GGCAT

GGCAT is a tool for building compacted De Bruijn graphs from raw sequencing data or for merging multiple existing cDBG in a single graph. It has a speedup from x8 to x32 wrt. other publicly available tools for cDBG construction.

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
To see all the available options for graph building run:
```
ggcat build --help
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
