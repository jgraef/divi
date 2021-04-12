//! Command-line tool to sync the DIVI register[1]. This crawls the archived
//! data[2], downloads the CSV files, and normalized them to JSON.
//!
//! Please read [1] in regards to copyright and further information about the
//! data.
//! 
//! - [1] https://www.divi.de/register/tagesreport
//! - [2] https://www.divi.de/divi-intensivregister-tagesreport-archiv
//! 
//! # Installation
//! 
//! You don't need to install the program, to use it. `cargo install` will compile and install the binary user-locally.
//! 
//! ```sh
//! cargo install --path .
//! ```
//! 
//! # Usage
//! 
//! If you haven't installed the program, use `cargo run --` instead of `divi-tool`.
//! 
//! ## Syncing archived data
//! 
//! ```sh
//! divi-tool sync -d data
//! ```
//! 
//! This will sync the archived DIVI data to the directory `data` (`./data` is the default, if the `-d` option is omitted).
//! 
//! Otherwise run `divi-tool --help` to show the program usage:
//! 
//! ```plain
//! divi-tool 0.1.0
//! 
//! USAGE:
//! divi <SUBCOMMAND>
//! 
//! FLAGS:
//! -h, --help
//!         Prints help information
//! 
//! -V, --version
//!         Prints version information
//! 
//! 
//! SUBCOMMANDS:
//! help     Prints this message or the help of the given subcommand(s)
//! sync     Synchronize DIVI register's archived data. The data will be normalized and stored as JSON files
//! today    Fetch the daily report for today
//! ``` 
//! 

mod divi;
mod store;

use std::path::PathBuf;

use color_eyre::eyre::Error;
use futures::stream::StreamExt;
use structopt::StructOpt;

use store::Store;

#[derive(Clone, Debug, StructOpt)]
enum Args {
    /// Synchronize DIVI register's archived data. The data will be normalized
    /// and stored as JSON files.
    ///
    /// Please read [1] for copyright information and further information about
    /// the data.assert_eq!
    ///
    /// [1] https://www.divi.de/register/tagesreport
    Sync {
        /// Directory in which the normalized DIVI data is stored.
        #[structopt(short = "d", long = "data", default_value = "./data")]
        data_dir: PathBuf,

        /// Don't stop when an already synced file is encountered, but check all
        /// files.
        #[structopt(long, short = "a")]
        check_all: bool,

        /// Ignore already downloaded files and sync everything.
        #[structopt(long, short = "A")]
        resync_all: bool,
    },
    /// Fetch the daily report for today.
    Today,
}

impl Args {
    pub async fn run(self) -> Result<(), Error> {
        match self {
            Args::Sync {
                data_dir,
                check_all,
                resync_all,
            } => {
                let mut store = Store::new(&data_dir)?;

                let api = divi::Api::default();
                let mut archived = api.list_archived()?;

                while let Some(result) = archived.next().await {
                    let url = result?;

                    if !resync_all && store.contains_dataset_source_url(&url) {
                        if check_all {
                            tracing::info!("Skipping {}", url);
                        } else {
                            tracing::info!("Stopping, already known: {}", url);
                            break;
                        }
                    } else {
                        tracing::info!("Downloading {}", url);
                        let dataset = api.get_archived(url).await?;
                        store.put_dataset(&dataset)?;
                    }
                }
            }
            Args::Today => {
                let api = divi::Api::default();
                let today = api.get_current().await?;

                //for entry in today {}

                println!("{:#?}", today);
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    color_eyre::install()?;
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let args = Args::from_args();
    args.run().await?;

    Ok(())
}
