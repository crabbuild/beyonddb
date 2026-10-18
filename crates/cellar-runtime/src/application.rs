//! Cellar application and cell factories for compiled BeyondDB operations.

use std::sync::Arc;

use cellar::sdk::{
    self, Application, Cell, Context, HandlerFuture, Invocation, Output, Storage, http::Response,
};

use crate::{
    Error, Result,
    identity::{CellTarget, NamespaceId, encode_hex},
    registry::{CommandContext, EncodedResult, QueryContext, Registry},
    wire::{ENVELOPE_LIMIT, Envelope, Reply},
};

pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

const STATE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS bdb_runtime_state (
 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
 schema_digest BLOB NOT NULL,
 sequence INTEGER NOT NULL,
 now_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS bdb_runtime_receipts (
 request_id BLOB PRIMARY KEY,
 fingerprint BLOB NOT NULL,
 sequence INTEGER NOT NULL,
 rejected INTEGER NOT NULL,
 result BLOB NOT NULL,
 retire_after_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS bdb_runtime_receipt_expiry ON bdb_runtime_receipts(retire_after_ms);
";

pub fn binding(namespace: NamespaceId) -> String {
    format!("BDB_{}", encode_hex(namespace.as_bytes()))
}

pub fn cell_name(target: &CellTarget) -> String {
    format!("bdb-v1-{}", encode_hex(target.cell_id().as_bytes()))
}

impl Registry {
    /// A static deployment matching exactly the compiled module inventory.
    pub fn deployment(&self) -> sdk::DeploymentGraph {
        sdk::DeploymentGraph::single(cellar::fleet::LoadedDeployment {
            event_streams: Vec::new(),
            native: None,
            script_name: "beyonddb".into(),
            version: "v1".into(),
            prefix: "beyonddb/v1".into(),
            options: cellar::engine_api::WorkerConfigOptions {
                stream_bindings: Vec::new(),
                src: String::new(),
                script_name: "beyonddb".into(),
                do_classes: self.classes(),
                bindings: self.bindings(),
                sql_bindings: Vec::new(),
                kv_bindings: Vec::new(),
                blob_bindings: Vec::new(),
                queue_bindings: Vec::new(),
                queue_consumers: Vec::new(),
                workflow_bindings: Vec::new(),
                vars: Vec::new(),
                node: String::new(),
                modules: Vec::new(),
                compat: cellar::engine_api::Compat::default(),
            },
            asset_binding: None,
            loader_bindings: Vec::new(),
            assets: None,
            services: Vec::new(),
            crons: Vec::new(),
            cron_revision: None,
            containers: Vec::new(),
            fence_image: None,
            container_images: Vec::new(),
        })
    }

    pub fn compiled(self: &Arc<Self>, clock: Clock) -> Result<sdk::CompiledApplications> {
        let registry = self.clone();
        Ok(
            sdk::CompiledApplications::default().register("beyonddb", "v1", move || {
                registry
                    .application(clock.clone())
                    .map_err(anyhow::Error::new)
            })?,
        )
    }

    /// The caller registers this application in Cellar's CompiledApplications.
    /// Its gateway is programmatic ingress, never a public HTTP fallback.
    pub fn application(self: &Arc<Self>, clock: Clock) -> Result<Arc<dyn Application>> {
        let gateway_registry = self.clone();
        let mut application = sdk::Registry::new(move |context: Context, invocation| {
            let registry = gateway_registry.clone();
            Box::pin(async move {
                let Invocation::Fetch(request) = invocation else {
                    anyhow::bail!("expected internal invocation");
                };
                let uri: http::Uri = request.url.parse()?;
                let (namespace_binding, name) = uri
                    .path()
                    .strip_prefix("/_bdb/")
                    .and_then(|path| path.split_once('/'))
                    .ok_or_else(|| anyhow::anyhow!("invalid internal cell route"))?;
                anyhow::ensure!(
                    name.len() == 71
                        && name.starts_with("bdb-v1-")
                        && name[7..]
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                    "invalid internal cell name"
                );
                let found = registry.modules.values().any(|module| {
                    module
                        .namespaces
                        .iter()
                        .any(|ns| binding(ns.id) == namespace_binding)
                });
                anyhow::ensure!(found, "unknown internal namespace");
                let cell = context.namespace(namespace_binding)?.get_by_name(name);
                Ok(Output::Http(cell.fetch(&context, request).await?))
            })
        });
        for module in self.modules.values() {
            let registry = self.clone();
            let clock = clock.clone();
            let module_name = module.name;
            application = application.cell(module_name, move |_, identity| {
                let name = identity
                    .name
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("cell requires a stable name"))?;
                Ok(Arc::new(DomainCell {
                    registry: registry.clone(),
                    module: module_name,
                    name,
                    clock: clock.clone(),
                }))
            })?;
        }
        Ok(Arc::new(application))
    }

    /// Bindings for the caller's DeploymentGraph. Class names are compiled
    /// module names; no dynamic source loading or process-wide context is used.
    pub fn bindings(&self) -> Vec<(String, String)> {
        self.modules
            .values()
            .flat_map(|module| {
                module
                    .namespaces
                    .iter()
                    .map(|ns| (binding(ns.id), module.name.to_owned()))
            })
            .collect()
    }

    pub fn classes(&self) -> Vec<String> {
        self.modules.keys().map(|name| (*name).to_owned()).collect()
    }
}

struct DomainCell {
    registry: Arc<Registry>,
    module: &'static str,
    name: String,
    clock: Clock,
}

impl Cell for DomainCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                anyhow::bail!("unexpected BeyondDB event");
            };
            anyhow::ensure!(request.method == "POST", "internal commands require POST");
            // The caller supplies a finite body. Still enforce the framing limit
            // after materialization at the independently authenticated owner.
            let bytes = request.into_body_reader(&context)?.bytes(&context).await?;
            anyhow::ensure!(
                bytes.len() <= ENVELOPE_LIMIT,
                "internal invocation exceeds its byte budget"
            );
            let envelope = Envelope::decode(&bytes)?;
            let module = self.registry.module_for(envelope.target.namespace())?;
            anyhow::ensure!(
                module.name == self.module && cell_name(&envelope.target) == self.name,
                "invocation reached a different cell"
            );
            let storage = context.storage()?;
            let reply = storage.tx(|transaction| {
                initialize(
                    transaction,
                    module.migrations[0].sql,
                    module.migrations[0].digest.as_bytes(),
                )?;
                self.execute(transaction, &envelope, &bytes)
                    .map_err(anyhow::Error::new)
            }).map_err(|error| {
                tracing::warn!(module = self.module, cell = self.name, error = %error, "compiled cell operation failed");
                error
            })?;
            let response = Response::new(
                200,
                reply.encode()?,
                vec![("content-type".into(), "application/octet-stream".into())],
            );
            // Cellar's response egress supplies ownership and durability proof.
            Ok(Output::Http(response.into_http(&context)?))
        })
    }
}

impl DomainCell {
    fn execute(&self, storage: &Storage, envelope: &Envelope, encoded: &[u8]) -> Result<Reply> {
        use sdk::primitives::sql::Value as V;
        let state = rows(
            storage,
            "SELECT sequence, now_ms FROM bdb_runtime_state WHERE singleton = 1",
            &[],
        )?;
        let Some([V::Integer(sequence), V::Integer(previous_ms)]) =
            state.first().map(Vec::as_slice)
        else {
            return Err(Error::Command("invalid command sequence state"));
        };
        let now_ms = (self.clock)().max(*previous_ms);
        if now_ms < 0 {
            return Err(Error::Command("invalid runtime clock"));
        }
        let sequence =
            u64::try_from(*sequence).map_err(|_| Error::Command("invalid command sequence"))?;
        let Some(identity) = envelope.mutation else {
            if sequence < envelope.minimum_sequence {
                return Err(Error::Command("minimum read receipt is unavailable"));
            }
            let mut query = QueryContext::new(storage, &envelope.target, sequence, now_ms);
            return Ok(Reply {
                sequence,
                rejected: false,
                output: self.registry.execute_query(
                    &mut query,
                    envelope.target.namespace(),
                    envelope.id,
                    envelope.version,
                    &envelope.input,
                )?,
            });
        };
        let fingerprint = blake3::hash(encoded);
        let old = rows(
            storage,
            "SELECT fingerprint, sequence, rejected, result FROM bdb_runtime_receipts WHERE request_id = ?1",
            &[V::Blob(identity.request_id.as_bytes().to_vec())],
        )?;
        if let Some(row) = old.first() {
            identity.validate(now_ms, true)?;
            let [
                V::Blob(digest),
                V::Integer(sequence),
                V::Integer(rejected),
                V::Blob(result),
            ] = row.as_slice()
            else {
                return Err(Error::Command("invalid persisted command receipt"));
            };
            if digest.as_slice() != fingerprint.as_bytes() {
                return Err(Error::Command(
                    "mutation identity reused with another request",
                ));
            }
            return Ok(Reply {
                sequence: u64::try_from(*sequence)
                    .map_err(|_| Error::Command("invalid receipt sequence"))?,
                rejected: *rejected != 0,
                output: result.clone(),
            });
        }
        identity.validate(now_ms, false)?;
        let next = sequence
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(Error::Command("command sequence overflow"))?;
        let mut rejected = None;
        let applied = storage.tx(|application| {
            let mut context = CommandContext::new(
                application,
                &envelope.target,
                next,
                now_ms,
                self.registry.database_limit(self.module),
            );
            let result = self.registry.execute_command(
                &mut context,
                envelope.id,
                envelope.version,
                &envelope.input,
            )?;
            if result.rejected {
                rejected = Some(result);
                return Err(RejectedApplication.into());
            }
            Ok(result)
        });
        let EncodedResult { output, rejected } = match applied {
            Ok(result) => result,
            Err(error) if error.is::<RejectedApplication>() => {
                rejected.ok_or(Error::Command("rejected result was lost"))?
            }
            Err(error) => return Err(error.into()),
        };
        execute(
            storage,
            "UPDATE bdb_runtime_state SET sequence = ?1, now_ms = ?2 WHERE singleton = 1",
            &[V::Integer(next as i64), V::Integer(now_ms)],
        )?;
        execute(
            storage,
            "INSERT INTO bdb_runtime_receipts(request_id, fingerprint, sequence, rejected, result, retire_after_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            &[
                V::Blob(identity.request_id.as_bytes().to_vec()),
                V::Blob(fingerprint.as_bytes().to_vec()),
                V::Integer(next as i64),
                V::Integer(i64::from(rejected)),
                V::Blob(output.clone()),
                V::Integer(identity.expires_at_ms.saturating_add(86_400_000)),
            ],
        )?;
        execute(
            storage,
            "DELETE FROM bdb_runtime_receipts WHERE request_id IN (SELECT request_id FROM bdb_runtime_receipts WHERE retire_after_ms < ?1 ORDER BY retire_after_ms LIMIT 128)",
            &[V::Integer(now_ms)],
        )?;
        crate::capacity::validate(storage, self.registry.database_limit(self.module))?;
        Ok(Reply {
            sequence: next,
            rejected,
            output,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("application command was rejected")]
struct RejectedApplication;

fn initialize(storage: &Storage, schema: &str, digest: &[u8; 32]) -> anyhow::Result<()> {
    use sdk::primitives::sql::Value as V;
    // The schema is compiled application code, not SQL supplied by a request.
    let (remaining, _, _) = storage.sql_ingest(STATE_SCHEMA)?;
    anyhow::ensure!(remaining.trim().is_empty(), "incomplete runtime schema");
    let state = rows(
        storage,
        "SELECT schema_digest FROM bdb_runtime_state WHERE singleton = 1",
        &[],
    )?;
    if let Some(row) = state.first() {
        anyhow::ensure!(
            matches!(row.as_slice(), [V::Blob(stored)] if stored.as_slice() == digest),
            "installed application schema differs from this executable"
        );
    } else {
        let (remaining, _, _) = storage.sql_ingest(crate::capacity::SCHEMA)?;
        anyhow::ensure!(remaining.trim().is_empty(), "incomplete reservation schema");
        let (remaining, _, _) = storage.sql_ingest(schema)?;
        anyhow::ensure!(remaining.trim().is_empty(), "incomplete application schema");
        execute(
            storage,
            "INSERT INTO bdb_runtime_state(singleton, schema_digest, sequence, now_ms) VALUES (1, ?1, 0, 0)",
            &[V::Blob(digest.to_vec())],
        )?;
    }
    Ok(())
}

fn rows(
    storage: &Storage,
    query: &str,
    values: &[sdk::primitives::sql::Value],
) -> anyhow::Result<Vec<Vec<sdk::primitives::sql::Value>>> {
    storage.sql_cursor(query, values)?.to_raw_array()
}

fn execute(
    storage: &Storage,
    query: &str,
    values: &[sdk::primitives::sql::Value],
) -> anyhow::Result<()> {
    let mut cursor = storage.sql_cursor(query, values)?;
    anyhow::ensure!(
        cursor.next_raw()?.is_none(),
        "unexpected rows from internal mutation"
    );
    cursor.close()
}
