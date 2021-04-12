use std::{
    io::Cursor,
    pin::Pin,
    task::{Context, Poll},
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use csv::Reader as CsvReader;
use futures::{
    future::{BoxFuture, Future, FutureExt},
    ready,
    stream::Stream,
};
use pin_project::pin_project;
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use soup::{NodeExt, QueryBuilderExt, Soup};
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Parse error: {0}")]
    ParseError(&'static str),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

    #[error("URL error: {0}")]
    Url(#[from] url::ParseError),

    #[error("The dataset is empty")]
    EmptyDataSet,

    #[error("Invalid row")]
    InvalidRow,

    #[error("Chrono: {0}")]
    Chrono(#[from] chrono::ParseError),

    #[error("Parse int: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("Could not determine timestamp for dataset")]
    MissingTimestamp,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GeoPosition {
    pub latitude: f32,
    pub longitude: f32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HospitalLocation {
    pub id: String,
    #[serde(rename = "bezeichnung")]
    pub name: String,
    #[serde(rename = "strasse")]
    pub address_street: String,
    #[serde(rename = "hausnummer")]
    pub address_house_number: String,
    #[serde(rename = "plz")]
    pub address_postcode: String,
    #[serde(rename = "ort")]
    pub address_city: String,
    #[serde(rename = "bundesland")]
    pub state: String,
    #[serde(rename = "ikNummer")]
    pub ik_number: String,
    pub position: GeoPosition,
    #[serde(rename = "gemeindeschluessel")]
    pub community_key: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReportArea {
    #[serde(rename = "meldebereichId")]
    pub id: String,
    #[serde(rename = "ardsNetzwerkMitglied")]
    pub ards_network_member: String,
    #[serde(rename = "meldebereichBezeichnung")]
    pub name: String,
    #[serde(rename = "behandlungsSchwerpunktL1")]
    pub treatment_focus_l1: Option<String>,
    #[serde(rename = "behandlungsSchwerpunktL2")]
    pub treatment_focus_l2: Option<String>,
    #[serde(rename = "behandlungsSchwerpunktL3")]
    pub treatment_focus_l3: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Entry {
    #[serde(rename = "krankenhausStandort")]
    pub hospital_location: HospitalLocation,
    #[serde(rename = "letzteMeldezeitpunkt")]
    pub last_report_time: DateTime<Utc>,
    #[serde(rename = "oldestMeldezeitpunkt")]
    pub oldest_report_time: DateTime<Utc>,
    #[serde(rename = "meldebereiche")]
    pub report_areas: Vec<ReportArea>,
    #[serde(rename = "maxBettenStatusEinschaetzungEcmo")]
    pub max_beds_status_estimate_ecmo: String,
    #[serde(rename = "maxBettenStatusEinschaetzungHighCare")]
    pub max_beds_status_estimate_high_care: String,
    #[serde(rename = "maxBettenStatusEinschaetzungLowCare")]
    pub max_beds_status_estimate_low_care: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SumEntry {
    #[serde(rename = "letzteMeldezeitpunkt")]
    pub last_report_time: DateTime<Utc>,
    #[serde(rename = "oldestMeldezeitpunkt")]
    pub oldest_report_time: DateTime<Utc>,
    #[serde(rename = "maxBettenStatusEinschaetzungEcmo")]
    pub max_beds_status_estimate_ecmo: String,
    #[serde(rename = "maxBettenStatusEinschaetzungHighCare")]
    pub max_beds_status_estimate_high_care: String,
    #[serde(rename = "maxBettenStatusEinschaetzungLowCare")]
    pub max_beds_status_estimate_low_care: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentResponse {
    pub row_count: usize,
    pub data: Vec<Entry>,
    pub sum: SumEntry,
}

#[derive(Clone, Debug, Deserialize)]
struct RawRow {
    #[serde(rename = "")]
    pub idx: Option<usize>,
    pub bundesland: Option<u8>,
    pub gemeindeschluessel: Option<String>,
    pub kreis: Option<String>,
    pub anzahl_meldebereiche: Option<usize>,
    pub faelle_covid_aktuell: Option<usize>,
    pub faelle_covid_aktuell_invasiv_beatmet: Option<usize>,
    pub faelle_covid_aktuell_beatmet: Option<usize>,
    pub faelle_cobid_aktuell_im_bundesland: Option<usize>,
    pub anzahl_standorte: usize,
    pub betten_frei: String,
    pub betten_belegt: String,
    pub daten_stand: Option<String>, // German time zone
    pub betten_belegt_nur_erwachsen: Option<usize>,
    pub betten_frei_nur_erwachsen: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    // TODO: Drop state? It's contained in the Gemeindeschluessel (AGS) anyway.
    state: u8,
    ags: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_report_areas: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cases_current: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cases_ventilated: Option<usize>,
    num_locations: usize,
    beds_available: usize,
    beds_occupied: usize,
    timestamp: NaiveDateTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    beds_occupied_adults: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    beds_available_adults: Option<usize>,
}

fn parse_decimal_to_int(s: &str) -> Result<usize, Error> {
    let mut parts = s.split('.');
    Ok(parts
        .next()
        .ok_or_else(|| {
            tracing::error!("Tried to parse integer with decimal point, but failed: {}", s);
            Error::InvalidRow
        })?
        .parse()?)
}

impl Row {
    fn from_raw(raw: RawRow, timestamp_hint: &Option<NaiveDateTime>) -> Result<Self, Error> {
        let timestamp = match (&raw.daten_stand, timestamp_hint) {
            (Some(daten_stand), _) => NaiveDateTime::parse_from_str(daten_stand, "%Y-%m-%d %H:%M:%S")?,
            (None, Some(hint)) => hint.clone(),
            _ => return Err(Error::MissingTimestamp),
        };

        let ags = raw.gemeindeschluessel.or(raw.kreis).ok_or_else(|| {
            tracing::error!("Missing Gemeindeschluessel");
            Error::InvalidRow
        })?;

        let cases_ventilated = raw.faelle_covid_aktuell_invasiv_beatmet.or(raw.faelle_covid_aktuell_beatmet);

        let state = if let Some(bundesland) = raw.bundesland {
            bundesland
        } else {
            ags[0..2].parse()?
        };

        Ok(Self {
            state,
            ags,
            num_report_areas: raw.anzahl_meldebereiche,
            cases_current: raw.faelle_covid_aktuell,
            cases_ventilated,
            num_locations: raw.anzahl_standorte,
            beds_available: parse_decimal_to_int(&raw.betten_frei)?,
            beds_occupied: parse_decimal_to_int(&raw.betten_belegt)?,
            timestamp,
            beds_occupied_adults: raw.betten_belegt_nur_erwachsen,
            beds_available_adults: raw.betten_frei_nur_erwachsen,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataSet {
    pub date: NaiveDate,
    pub source_url: Url,
    pub rows: Vec<Row>,
}

impl DataSet {
    pub fn new(source_url: Url, rows: Vec<Row>) -> Result<Self, Error> {
        let date = rows.get(0).ok_or_else(|| Error::EmptyDataSet)?.timestamp.date();

        Ok(Self { date, source_url, rows })
    }
}

#[derive(Debug, Default)]
pub struct Api {
    client: Client,
}

impl Api {
    pub async fn get_current(&self) -> Result<CurrentResponse, Error> {
        Ok(self
            .client
            .get("https://www.intensivregister.de/api/public/intensivregister")
            .send()
            .await?
            .json()
            .await?)
    }

    pub async fn get_archived(&self, url: Url) -> Result<DataSet, Error> {
        let data = self.client.get(url.clone()).send().await?.bytes().await?;

        let mut reader = CsvReader::from_reader(Cursor::new(data));

        let timestamp_hint = timestamp_hint_from_url(&url);

        let rows = reader
            .deserialize::<RawRow>()
            .map(|r| Ok(Row::from_raw(r?, &timestamp_hint)?))
            .collect::<Result<Vec<Row>, Error>>()?;

        Ok(DataSet::new(url, rows)?)
    }

    pub fn list_archived<'a>(&'a self) -> Result<ArchiveStream<'a>, Error> {
        ArchiveStream::new(self)
    }
}

fn timestamp_hint_from_url(url: &Url) -> Option<NaiveDateTime> {
    let regex = Regex::new(r"(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})").unwrap();

    let captures = regex.captures(url.path())?;
    let hint = NaiveDate::from_ymd(
        captures.get(1)?.as_str().parse().ok()?,
        captures.get(2)?.as_str().parse().ok()?,
        captures.get(3)?.as_str().parse().ok()?,
    )
    .and_hms(captures.get(4)?.as_str().parse().ok()?, captures.get(5)?.as_str().parse().ok()?, 0);

    Some(hint)
}

async fn parse_archive_page(client: &Client, url: Url, base_url: Url) -> Result<(Vec<Url>, Option<Url>), Error> {
    let html = client.get(url).send().await?.text().await?;

    let soup = Soup::new(&html);

    let table = soup.attr("id", "table-document").find().ok_or_else(|| Error::ParseError("Can't find table"))?;

    let mut urls = vec![];
    for a in table.tag("a").find_all() {
        let url = base_url.join(&a.get("href").ok_or_else(|| Error::ParseError("Missing href in <a> tag"))?)?;
        tracing::debug!("Found URL: {}", url);
        urls.push(url);
    }

    let next_url = if let Some(a_next) = soup.tag("a").attr("title", "Weiter").find() {
        let next_url = base_url.join(&a_next.get("href").ok_or_else(|| Error::ParseError("Missing href in <a> tag"))?)?;
        tracing::debug!("Next url: {}", next_url);
        Some(next_url)
    } else {
        None
    };

    Ok((urls, next_url))
}

struct ArchiveStreamInner<'a> {
    client: &'a Client,
    base_url: Url,
    urls: Vec<Url>,
}

#[pin_project]
pub struct ArchiveStream<'a> {
    //#[pin]
    request_future: Option<BoxFuture<'a, Result<(Vec<Url>, Option<Url>), Error>>>,
    inner: ArchiveStreamInner<'a>,
}

impl<'a> ArchiveStream<'a> {
    pub fn new(api: &'a Api) -> Result<Self, Error> {
        let base_url: Url = "https://www.divi.de".parse()?;
        let next_url = base_url.join("divi-intensivregister-tagesreport-archiv-csv")?;
        let request_future = parse_archive_page(&api.client, next_url, base_url.clone()).boxed();

        Ok(Self {
            request_future: Some(request_future),
            inner: ArchiveStreamInner {
                client: &api.client,
                base_url,
                urls: vec![],
            },
        })
    }
}

impl<'a> Stream for ArchiveStream<'a> {
    type Item = Result<Url, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            // If we have URLs buffered, yield them
            if let Some(url) = this.inner.urls.pop() {
                return Poll::Ready(Some(Ok(url)));
            }

            if let Some(request_future) = &mut this.request_future {
                // Poll the future
                match ready!(Pin::new(request_future).poll(cx)) {
                    Ok((urls, next_url)) => {
                        this.inner.urls = urls;
                        this.inner.urls.reverse();
                        if let Some(next_url) = next_url {
                            *this.request_future = Some(parse_archive_page(&this.inner.client, next_url, this.inner.base_url.clone()).boxed())
                        } else {
                            *this.request_future = None;
                        }
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                }
            } else {
                // No new future was set, thus we're done
                return Poll::Ready(None);
            }
        }
    }
}
