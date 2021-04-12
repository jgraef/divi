![Maintenance](https://img.shields.io/badge/maintenance-experimental-blue.svg)

# divi-tool

Command-line tool to sync the DIVI register[1]. This crawls the archived
data[2], downloads the CSV files, and normalized them to JSON.

Please read [1] in regards to copyright and further information about the
data.

- [1] https://www.divi.de/register/tagesreport
- [2] https://www.divi.de/divi-intensivregister-tagesreport-archiv

## Installation

You don't need to install the program, to use it. `cargo install` will compile and install the binary user-locally.

```sh
cargo install --path .
```

## Usage

If you haven't installed the program, use `cargo run --` instead of `divi-tool`.

### Syncing archived data

```sh
divi-tool sync -d data
```

This will sync the archived DIVI data to the directory `data` (`./data` is the default, if the `-d` option is omitted).

Otherwise run `divi-tool --help` to show the program usage:

```plain
divi-tool 0.1.0

USAGE:
divi <SUBCOMMAND>

FLAGS:
-h, --help
        Prints help information

-V, --version
        Prints version information


SUBCOMMANDS:
help     Prints this message or the help of the given subcommand(s)
sync     Synchronize DIVI register's archived data. The data will be normalized and stored as JSON files
today    Fetch the daily report for today
```


## License

Licensed under MIT license ([LICENSE](LICENSE) or https://opensource.org/licenses/MIT)
