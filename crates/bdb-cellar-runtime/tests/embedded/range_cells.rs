//! A qualification fixture, not the production DynamoDB implementation.
//! Shares the real compiled gateway and Cellar host with the adapter test.

use super::*;
use bdb_keyspace::{
    CellRange, Component as C, Directory, KeyKind, KeySchema, KeySpan, RangeKey, SortCondition,
};
use serde::{Deserialize, Serialize};

const NAME: &str = "BdbRangeTest";
const NS: NamespaceId = NamespaceId::from_bytes([2; 16]);
const SQL: &str = "
CREATE TABLE range_config (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), record BLOB NOT NULL);
CREATE TABLE range_items (key BLOB PRIMARY KEY, item BLOB NOT NULL) WITHOUT ROWID;
";
static OPS: [OperationDescriptor; 2] = [op(1), op(2)];
static READS: [OperationDescriptor; 1] = [op(1)];
const fn op(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        input_limit: 16_384,
        output_limit: 16_384,
        ..operation(id)
    }
}

pub(super) struct RangeModule;
impl CellModule for RangeModule {
    const NAME: &'static str = NAME;
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: NAME,
            source_digest: Digest::from_bytes(
                *blake3::hash(include_bytes!("range_cells.rs")).as_bytes(),
            ),
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: Box::leak(Box::new([MigrationDescriptor {
                version: 1,
                sql: SQL,
                digest: Digest::from_bytes(*blake3::hash(SQL.as_bytes()).as_bytes()),
            }])),
            commands: &OPS,
            queries: &READS,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: NS,
                name: NAME,
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }
    fn register(self, builder: &mut RegistryBuilder) -> Result<()> {
        builder.bind_command::<Install>()?;
        builder.bind_command::<Put>()?;
        builder.bind_query::<Select>()
    }
}

fn invalid() -> bdb_cellar_runtime::Error {
    bdb_cellar_runtime::Error::Command("invalid or stale range request")
}

struct Install;
impl Command for Install {
    const MODULE: &'static str = NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Vec<u8>;
    type Output = ();
    fn execute(context: &mut CommandContext<'_, '_>, input: Vec<u8>) -> Result<CommandResult<()>> {
        let range: CellRange = serde_json::from_slice(&input)?;
        if context.target().partition() != range.route.cell_id || range.route.epoch == 0 {
            return Err(invalid());
        }
        context.sql(&statement(
            "INSERT INTO range_config VALUES(1, ?1)",
            vec![SqlValue::Blob(input)],
        ))?;
        Ok(CommandResult::Success(()))
    }
}

#[derive(Serialize, Deserialize)]
struct WriteInput {
    epoch: u64,
    key: RangeKey,
    item: Vec<u8>,
}

struct Put;
impl Command for Put {
    const MODULE: &'static str = NAME;
    const ID: u32 = 2;
    const CODEC_VERSION: u32 = 1;
    type Input = Vec<u8>;
    type Output = ();
    fn execute(context: &mut CommandContext<'_, '_>, input: Vec<u8>) -> Result<CommandResult<()>> {
        let input: WriteInput = serde_json::from_slice(&input)?;
        let rows = context.sql(&statement(
            "SELECT record FROM range_config WHERE singleton=1",
            vec![],
        ))?;
        let [SqlValue::Blob(record)] = rows[0].rows.first().ok_or_else(invalid)?.as_slice() else {
            return Err(invalid());
        };
        let range: CellRange = serde_json::from_slice(record)?;
        if range.route.epoch != input.epoch || !range.span.contains(&input.key) {
            return Err(invalid());
        }
        context.sql(&statement("INSERT INTO range_items VALUES(?1, ?2) ON CONFLICT(key) DO UPDATE SET item=excluded.item", vec![SqlValue::Blob(input.key.as_bytes().to_vec()), SqlValue::Blob(input.item)]))?;
        Ok(CommandResult::Success(()))
    }
}

#[derive(Serialize, Deserialize)]
struct ReadInput {
    epoch: u64,
    span: KeySpan,
    forward: bool,
}

struct Select;
impl Query for Select {
    const MODULE: &'static str = NAME;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    fn execute(context: &mut QueryContext<'_>, input: Vec<u8>) -> Result<Vec<u8>> {
        let input: ReadInput = serde_json::from_slice(&input)?;
        let rows = context.sql(&statement(
            "SELECT record FROM range_config WHERE singleton=1",
            vec![],
        ))?;
        let [SqlValue::Blob(record)] = rows[0].rows.first().ok_or_else(invalid)?.as_slice() else {
            return Err(invalid());
        };
        let range: CellRange = serde_json::from_slice(record)?;
        if range.route.epoch != input.epoch
            || range.span.intersection(&input.span).as_ref() != Some(&input.span)
        {
            return Err(invalid());
        }
        let lower = input.span.lower().map_or(SqlValue::Null, |key| {
            SqlValue::Blob(key.as_bytes().to_vec())
        });
        let upper = input.span.upper().map_or(SqlValue::Null, |key| {
            SqlValue::Blob(key.as_bytes().to_vec())
        });
        let order = if input.forward { "ASC" } else { "DESC" };
        let rows = context.sql(&statement(&format!("SELECT item FROM range_items WHERE (?1 IS NULL OR key>=?1) AND (?2 IS NULL OR key<?2) ORDER BY key {order} LIMIT 32"), vec![lower, upper]))?;
        let mut values = Vec::<Vec<u8>>::new();
        for row in &rows[0].rows {
            let [SqlValue::Blob(item)] = row.as_slice() else {
                return Err(invalid());
            };
            values.push(item.clone());
        }
        Ok(serde_json::to_vec(&values)?)
    }
}

fn key(value: u8) -> RangeKey {
    schema()
        .encode(C::String("one-hot-pk"), Some(C::Number(&value.to_string())))
        .unwrap()
}
fn schema() -> KeySchema {
    KeySchema {
        partition: KeyKind::String,
        sort: Some(KeyKind::Number),
    }
}
fn target(id: [u8; 16]) -> Result<CellTarget> {
    CellTarget::new(
        TenantId::from_bytes([3; 16]),
        ApplicationId::from_bytes([2; 16]),
        NS,
        &id,
    )
}

async fn query(
    client: &CellClient,
    directory: &Directory,
    forward: bool,
) -> anyhow::Result<Vec<u8>> {
    let span = schema().query(
        C::String("one-hot-pk"),
        Some(SortCondition::Between(C::Number("7"), C::Number("24"))),
    )?;
    let mut values = Vec::new();
    for part in directory.query(&span, forward) {
        let result = client
            .query::<Select>(
                &target(part.route.cell_id)?,
                None,
                serde_json::to_vec(&ReadInput {
                    epoch: part.route.epoch,
                    span: part.span,
                    forward,
                })?,
            )
            .await?;
        let items: Vec<Vec<u8>> = serde_json::from_slice(&result.output)?;
        values.extend(items.into_iter().map(|item| item[0]));
    }
    Ok(values)
}

#[test]
fn ordered_table_uses_three_real_cells_and_survives_restart() -> anyhow::Result<()> {
    let data = tempfile::tempdir()?;
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    host.block_on(async {
        let registry = registry()?;
        let directory =
            Directory::initial(vec![key(10), key(20)], vec![[1; 16], [2; 16], [3; 16]])?;
        let (node, client) =
            start(data.path(), registry.clone(), host.handle(), core.handle()).await?;
        for part in directory.ranges() {
            client
                .command::<Install>(
                    &target(part.route.cell_id)?,
                    mutation(100),
                    serde_json::to_vec(part)?,
                )
                .await?;
        }
        for value in 1..=30 {
            let key = key(value);
            let route = directory.point(&key).route;
            client
                .command::<Put>(
                    &target(route.cell_id)?,
                    mutation(value),
                    serde_json::to_vec(&WriteInput {
                        epoch: route.epoch,
                        key,
                        item: vec![value],
                    })?,
                )
                .await?;
        }
        assert_eq!(
            query(&client, &directory, true).await?,
            (7..=24).collect::<Vec<u8>>()
        );
        assert_eq!(
            query(&client, &directory, false).await?,
            (7..=24).rev().collect::<Vec<u8>>()
        );
        assert!(
            client
                .command::<Put>(
                    &target([1; 16])?,
                    mutation(101),
                    serde_json::to_vec(&WriteInput {
                        epoch: 1,
                        key: key(20),
                        item: vec![200]
                    })?
                )
                .await
                .is_err()
        );
        assert!(
            client
                .command::<Put>(
                    &target([3; 16])?,
                    mutation(102),
                    serde_json::to_vec(&WriteInput {
                        epoch: 2,
                        key: key(20),
                        item: vec![200]
                    })?
                )
                .await
                .is_err()
        );
        stop(node).await?;
        drop(client);
        let (node, client) = start(data.path(), registry, host.handle(), core.handle()).await?;
        assert_eq!(
            query(&client, &directory, true).await?,
            (7..=24).collect::<Vec<u8>>()
        );
        assert_eq!(
            query(&client, &directory, false).await?,
            (7..=24).rev().collect::<Vec<u8>>()
        );
        stop(node).await
    })
}
