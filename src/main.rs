mod divi;
mod store;

use std::path::PathBuf;

use color_eyre::eyre::Error;
use futures::stream::StreamExt;
use structopt::StructOpt;

use store::Store;

#[derive(Clone, Debug, StructOpt)]
enum Args {
    /// Synchronize DIVI register's archived data. The data will be normalized and stored as JSON files.
    ///
    /// Please read [1] for copyright information and further information about the data.assert_eq!
    ///
    /// [1] https://www.divi.de/register/tagesreport
    ///
    Sync {
        /// Directory in which the normalized DIVI data is stored.
        #[structopt(short = "d", long = "data", default_value = "./data")]
        data_dir: PathBuf,

        /// Don't stop when an already synced file is encountered, but check all files.
        #[structopt(long, short = "a")]
        check_all: bool,

        /// Ignore already downloaded files and sync everything.
        #[structopt(long, short = "A")]
        resync_all: bool,
    },
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
