use serde_json::Value;
use wide::f64x4;

use crate::record::Record;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeSeriesFilter {
    pub start_ts: i64,
    pub end_ts: i64,
    pub metric: Option<String>,
    pub device_id: Option<String>,
}

impl TimeSeriesFilter {
    pub fn new(start_ts: i64, end_ts: i64) -> Self {
        Self {
            start_ts,
            end_ts,
            metric: None,
            device_id: None,
        }
    }

    pub fn metric(mut self, metric: impl Into<String>) -> Self {
        self.metric = Some(metric.into());
        self
    }

    pub fn device_id(mut self, device_id: impl Into<String>) -> Self {
        self.device_id = Some(device_id.into());
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NumericSummary {
    pub count: usize,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub avg: Option<f64>,
}

pub fn aggregate<'a, I>(records: I, filter: &TimeSeriesFilter) -> NumericSummary
where
    I: IntoIterator<Item = &'a Record>,
{
    let mut accumulator = NumericAccumulator::default();

    for record in records {
        if !matches_filter(record, filter) {
            continue;
        }

        if let Some(value) = numeric_value(record) {
            accumulator.push(value);
        }
    }

    accumulator.finish()
}

pub fn summarize_values(values: &[f64]) -> NumericSummary {
    if values.is_empty() {
        return NumericSummary::default();
    }

    let mut sum_acc = f64x4::from([0.0; 4]);
    let mut min_acc = f64x4::from([f64::INFINITY; 4]);
    let mut max_acc = f64x4::from([f64::NEG_INFINITY; 4]);
    let mut chunks = values.chunks_exact(4);

    for chunk in chunks.by_ref() {
        let values = f64x4::from([chunk[0], chunk[1], chunk[2], chunk[3]]);
        sum_acc += values;
        min_acc = min_acc.min(values);
        max_acc = max_acc.max(values);
    }

    let mut summary = NumericAccumulator {
        count: values.len() - chunks.remainder().len(),
        min: min_acc.to_array().into_iter().fold(f64::INFINITY, f64::min),
        max: max_acc
            .to_array()
            .into_iter()
            .fold(f64::NEG_INFINITY, f64::max),
        sum: sum_acc.reduce_add(),
    };

    for value in chunks.remainder() {
        summary.push(*value);
    }

    summary.finish()
}

pub fn matches_filter(record: &Record, filter: &TimeSeriesFilter) -> bool {
    let Some(timestamp) = record.timestamp else {
        return false;
    };

    if timestamp < filter.start_ts || timestamp > filter.end_ts {
        return false;
    }

    if let Some(metric) = &filter.metric {
        if metadata_string(&record.metadata, "metric") != Some(metric.as_str()) {
            return false;
        }
    }

    if let Some(device_id) = &filter.device_id {
        if metadata_string(&record.metadata, "device_id") != Some(device_id.as_str()) {
            return false;
        }
    }

    true
}

#[derive(Clone, Debug)]
struct NumericAccumulator {
    count: usize,
    min: f64,
    max: f64,
    sum: f64,
}

impl Default for NumericAccumulator {
    fn default() -> Self {
        Self {
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
        }
    }
}

impl NumericAccumulator {
    fn push(&mut self, value: f64) {
        self.count += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
    }

    fn finish(self) -> NumericSummary {
        if self.count == 0 {
            return NumericSummary::default();
        }

        NumericSummary {
            count: self.count,
            min: Some(self.min),
            max: Some(self.max),
            avg: Some(self.sum / self.count as f64),
        }
    }
}

fn numeric_value(record: &Record) -> Option<f64> {
    record.metadata.get("value").and_then(Value::as_f64)
}

fn metadata_string<'a>(metadata: &'a Value, key: &str) -> Option<&'a str> {
    metadata.as_object()?.get(key)?.as_str()
}
