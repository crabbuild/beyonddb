use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{Error, MAX_ENCODED_KEY_BYTES, Result, encoding::successor};

/// A bounded ordered key or synthetic byte fence. Never truncate or hash it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>", into = "Vec<u8>")]
pub struct RangeKey(std::sync::Arc<[u8]>);

impl RangeKey {
    /// Synthetic fence before every encoded user key (whose tag is >= 0x10).
    pub fn minimum() -> Self {
        Self(std::sync::Arc::from([0]))
    }

    pub fn new(bytes: Vec<u8>) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_ENCODED_KEY_BYTES {
            return Err(Error::KeySize);
        }
        Ok(Self(bytes.into()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::ops::Deref for RangeKey {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for RangeKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for RangeKey {
    type Error = Error;
    fn try_from(bytes: Vec<u8>) -> Result<Self> {
        Self::new(bytes)
    }
}

impl From<RangeKey> for Vec<u8> {
    fn from(key: RangeKey) -> Self {
        key.0.to_vec()
    }
}

/// `[lower, upper)`. An absent lower/upper means negative/positive infinity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SpanWire", into = "SpanWire")]
pub struct KeySpan {
    lower: Option<RangeKey>,
    upper: Option<RangeKey>,
}

#[derive(Serialize, Deserialize)]
struct SpanWire {
    lower: Option<RangeKey>,
    upper: Option<RangeKey>,
}

impl KeySpan {
    pub fn new(lower: Option<RangeKey>, upper: Option<RangeKey>) -> Result<Self> {
        if lower
            .as_ref()
            .zip(upper.as_ref())
            .is_some_and(|(a, b)| a >= b)
        {
            return Err(Error::Range);
        }
        Ok(Self { lower, upper })
    }

    pub fn lower(&self) -> Option<&RangeKey> {
        self.lower.as_ref()
    }

    pub fn upper(&self) -> Option<&RangeKey> {
        self.upper.as_ref()
    }

    pub fn contains(&self, key: &RangeKey) -> bool {
        self.lower.as_ref().is_none_or(|lower| key >= lower)
            && self.upper.as_ref().is_none_or(|upper| key < upper)
    }

    pub fn intersection(&self, other: &Self) -> Option<Self> {
        let lower = self.lower.clone().max(other.lower.clone());
        let upper = match (&self.upper, &other.upper) {
            (Some(a), Some(b)) => Some(a.min(b).clone()),
            (a, b) => a.as_ref().or(b.as_ref()).cloned(),
        };
        Self::new(lower, upper).ok()
    }

    /// Resume from an API key against current topology, never from a cell ID.
    /// A reverse query excludes the resume key using an exclusive upper bound.
    pub fn resume(&self, last_evaluated: &RangeKey, forward: bool) -> Option<Self> {
        if forward {
            let next = RangeKey::new(successor(last_evaluated.as_bytes())?).ok()?;
            self.intersection(&Self {
                lower: Some(next),
                upper: None,
            })
        } else {
            self.intersection(&Self {
                lower: None,
                upper: Some(last_evaluated.clone()),
            })
        }
    }
}

impl TryFrom<SpanWire> for KeySpan {
    type Error = Error;
    fn try_from(value: SpanWire) -> Result<Self> {
        Self::new(value.lower, value.upper)
    }
}

impl From<KeySpan> for SpanWire {
    fn from(value: KeySpan) -> Self {
        Self {
            lower: value.lower,
            upper: value.upper,
        }
    }
}

/// Logical data cell and its application epoch; placement is Cellar's concern.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route {
    pub cell_id: [u8; 16],
    pub epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellRange {
    pub span: KeySpan,
    pub route: Route,
}

/// A validated directory snapshot for one immutable table generation.
/// Construction rejects the legacy one-cell table layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Directory {
    epoch: u64,
    ranges: Vec<CellRange>,
}

impl Directory {
    /// Explicit ordered fences create multiple cells before table activation.
    /// Fences are schema/workload policy, never evenly spaced hash values.
    pub fn initial(fences: Vec<RangeKey>, cells: Vec<[u8; 16]>) -> Result<Self> {
        if fences.is_empty() || cells.len() != fences.len() + 1 {
            return Err(Error::Directory);
        }
        let mut lower = None;
        let mut ranges = Vec::with_capacity(cells.len());
        for (index, cell_id) in cells.into_iter().enumerate() {
            let upper = fences.get(index).cloned();
            ranges.push(CellRange {
                span: KeySpan::new(lower, upper.clone())?,
                route: Route { cell_id, epoch: 1 },
            });
            lower = upper;
        }
        Self::new(1, ranges)
    }

    pub fn new(epoch: u64, ranges: Vec<CellRange>) -> Result<Self> {
        if epoch == 0 || ranges.len() < 2 || ranges[0].span.lower.is_some() {
            return Err(Error::Directory);
        }
        let mut ids = HashSet::new();
        let mut previous_upper = None;
        for (index, range) in ranges.iter().enumerate() {
            if range.route.epoch == 0
                || range.route.epoch > epoch
                || !ids.insert(range.route.cell_id)
                || range.span.lower != previous_upper
                || (index + 1 < ranges.len() && range.span.upper.is_none())
            {
                return Err(Error::Directory);
            }
            previous_upper = range.span.upper.clone();
        }
        if previous_upper.is_some() {
            return Err(Error::Directory);
        }
        Ok(Self { epoch, ranges })
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn ranges(&self) -> &[CellRange] {
        &self.ranges
    }

    pub fn point(&self, key: &RangeKey) -> &CellRange {
        let index = self
            .ranges
            .partition_point(|range| range.span.upper.as_ref().is_some_and(|upper| upper <= key));
        // Full coverage was validated by new(). The final upper is unbounded.
        &self.ranges[index]
    }

    /// Ordered, clipped requests. Base-table Query can concatenate their pages;
    /// LSI queries need a separate merge in LSI sort order if a PK is divided.
    pub fn query(&self, query: &KeySpan, forward: bool) -> Vec<CellRange> {
        let start = self.ranges.partition_point(|range| {
            range
                .span
                .upper
                .as_ref()
                .zip(query.lower.as_ref())
                .is_some_and(|(upper, lower)| upper <= lower)
        });
        let mut output = Vec::new();
        for range in &self.ranges[start..] {
            if range
                .span
                .lower
                .as_ref()
                .zip(query.upper.as_ref())
                .is_some_and(|(lower, upper)| lower >= upper)
            {
                break;
            }
            if let Some(span) = range.span.intersection(query) {
                output.push(CellRange {
                    span,
                    route: range.route,
                });
            }
        }
        if !forward {
            output.reverse();
        }
        output
    }

    pub fn split(
        &self,
        expected_epoch: u64,
        source: Route,
        boundary: RangeKey,
        children: [[u8; 16]; 2],
    ) -> Result<Split> {
        if expected_epoch != self.epoch {
            return Err(Error::StaleRoute);
        }
        let range = self
            .ranges
            .iter()
            .find(|range| range.route == source)
            .ok_or(Error::StaleRoute)?;
        if !range.span.contains(&boundary)
            || range.span.lower.as_ref() == Some(&boundary)
            || children[0] == children[1]
            || self
                .ranges
                .iter()
                .any(|range| children.contains(&range.route.cell_id))
        {
            return Err(Error::Split);
        }
        let next = source.epoch.checked_add(1).ok_or(Error::Split)?;
        Ok(Split {
            directory_epoch: expected_epoch,
            source: range.clone(),
            children: [
                CellRange {
                    span: KeySpan::new(range.span.lower.clone(), Some(boundary.clone()))?,
                    route: Route {
                        cell_id: children[0],
                        epoch: next,
                    },
                },
                CellRange {
                    span: KeySpan::new(Some(boundary), range.span.upper.clone())?,
                    route: Route {
                        cell_id: children[1],
                        epoch: next,
                    },
                },
            ],
        })
    }

    /// Pure CAS result only. The server must verify durable child-copy receipts
    /// and source fencing before committing this replacement in a directory cell.
    pub fn replace(&self, split: &Split) -> Result<Self> {
        let boundary = split.children[1].span.lower.clone().ok_or(Error::Split)?;
        let checked = self.split(
            split.directory_epoch,
            split.source.route,
            boundary,
            [
                split.children[0].route.cell_id,
                split.children[1].route.cell_id,
            ],
        )?;
        if checked != *split {
            return Err(Error::Split);
        }
        let mut ranges = Vec::with_capacity(self.ranges.len() + 1);
        for range in &self.ranges {
            if range.route == split.source.route {
                ranges.extend(split.children.clone());
            } else {
                ranges.push(range.clone());
            }
        }
        Self::new(self.epoch.checked_add(1).ok_or(Error::Split)?, ranges)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Split {
    pub directory_epoch: u64,
    pub source: CellRange,
    pub children: [CellRange; 2],
}

impl TryFrom<&[u8]> for RangeKey {
    type Error = Error;
    fn try_from(bytes: &[u8]) -> Result<Self> {
        Self::new(bytes.to_vec())
    }
}
