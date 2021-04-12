use std::{
    collections::HashSet,
    fs::{create_dir_all, OpenOptions},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use color_eyre::eyre::Error;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::divi::DataSet;

#[derive(Debug)]
pub struct Store {
    path: PathBuf,
    info: Info,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Info {
    urls_synced: HashSet<Url>,
}

impl Store {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();

        if !path.exists() {
            create_dir_all(&path)?;
        }

        let info_path = path.join("info.json");
        let info = if !info_path.exists() {
            Info::default()
        } else {
            let opt = OpenOptions::new().read(true).open(info_path)?;
            serde_json::from_reader(BufReader::new(opt))?
        };

        Ok(Self { path: path.to_owned(), info })
    }

    fn rows_path(&self, date: &NaiveDate) -> PathBuf {
        self.path.join(format!("{}.json", date.format("%Y-%m-%d")))
    }

    pub fn get_dataset(&self, date: &NaiveDate) -> Result<DataSet, Error> {
        let file = OpenOptions::new().read(true).open(self.rows_path(date))?;

        Ok(serde_json::from_reader(BufReader::new(file))?)
    }

    pub fn put_dataset(&mut self, dataset: &DataSet) -> Result<(), Error> {
        let file = OpenOptions::new().write(true).create(true).truncate(true).open(self.rows_path(&dataset.date))?;

        serde_json::to_writer_pretty(BufWriter::new(file), dataset)?;
        self.info.urls_synced.insert(dataset.source_url.clone());

        self.save_info()?;

        Ok(())
    }

    pub fn contains_dataset_source_url(&self, source_url: &Url) -> bool {
        self.info.urls_synced.contains(source_url)
    }

    pub fn save_info(&self) -> Result<(), Error> {
        let info_path = self.path.join("info.json");
        let file = OpenOptions::new().write(true).create(true).truncate(true).open(info_path)?;
        serde_json::to_writer_pretty(BufWriter::new(file), &self.info)?;
        Ok(())
    }
}
