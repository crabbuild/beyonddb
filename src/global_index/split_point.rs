//! Include retained tombstones when choosing an observed index split key.
use super::*;
pub struct GlobalIndexSplitPoint;
impl Query for GlobalIndexSplitPoint {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<()>;
    type Output = Json<Option<RangeKey>>;
    fn execute(context: &mut QueryContext<'_>, _: Self::Input) -> Result<Self::Output> {
        let Some(spec) = read_spec(|batch| context.sql(batch))? else {
            return Ok(Json(None));
        };
        let rows = context.sql(&statement("SELECT ordered_key FROM ddb_global_index_items WHERE (SELECT COUNT(*) FROM ddb_global_index_items) >= 2 ORDER BY ordered_key LIMIT 1 OFFSET (SELECT COUNT(*) / 2 FROM ddb_global_index_items)", vec![]))?;
        let key = match rows[0].rows.first().map(Vec::as_slice) {
            None => None,
            Some([SqlValue::Blob(key)]) => Some(
                RangeKey::try_from(key.as_slice())
                    .map_err(|_| Error::Command("invalid index split point"))?,
            ),
            _ => return Err(Error::Command("invalid index split point row")),
        };
        Ok(Json(key.filter(|key| {
            spec.lower.as_ref().is_none_or(|low| key > low)
                && spec.upper.as_ref().is_none_or(|high| key < high)
        })))
    }
}
