//! Split at an observed distinct key, never an arithmetic midpoint.
use super::*;

pub struct PartitionSplitPoint;
impl Query for PartitionSplitPoint {
    const MODULE: &'static str = DATA_MODULE;
    const ID: u32 = 21;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<()>;
    type Output = Json<Option<RangeKey>>;
    fn execute(context: &mut QueryContext<'_>, _: Self::Input) -> Result<Self::Output> {
        let rows = context.sql(&statement(
            "SELECT spec FROM ddb_partition WHERE singleton = 1",
            vec![],
        ))?;
        let Some(spec) = decode_spec(&rows[0])? else {
            return Ok(Json(None));
        };
        let collections = !spec.table.local_secondary_indexes.is_empty();
        let source = if collections {
            "SELECT DISTINCT partition_key AS k FROM ddb_partition_items"
        } else {
            "SELECT ordered_key AS k FROM ddb_partition_items"
        };
        let rows = context.sql(&statement(&format!(
            "WITH keys AS ({source}), n AS (SELECT COUNT(*) AS count FROM keys) SELECT k FROM keys WHERE (SELECT count FROM n) >= 2 ORDER BY k LIMIT 1 OFFSET (SELECT count / 2 FROM n)"
        ), vec![]))?;
        let key = match rows[0].rows.first().map(Vec::as_slice) {
            None => None,
            Some([SqlValue::Blob(key)]) => Some(
                RangeKey::try_from(key.as_slice())
                    .map_err(|_| Error::Command("invalid split point"))?,
            ),
            _ => return Err(Error::Command("invalid split point row")),
        };
        Ok(Json(key.filter(|key| {
            spec.lower.as_ref().is_none_or(|low| key > low)
                && spec.upper.as_ref().is_none_or(|high| key < high)
        })))
    }
}
