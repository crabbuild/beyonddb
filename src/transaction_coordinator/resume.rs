//! Bounded preparation discovery from one coordinator observation.

use cellule_runtime::registry::{Query, QueryContext};
use serde::{Deserialize, Serialize};

use super::{
    CoordinatorDecision, CoordinatorParticipant, CrossCellTransactionStatus, MODULE,
    ReadCoordinatorParticipant, ReadCoordinatorParticipantInput, ReadCrossCellTransaction,
    ReadCrossCellTransactionInput,
};
use crate::{Error, Json, Result, SqlValue, table::statement};

pub(super) const RESUME_OUTPUT_BYTES: u32 = 64 * 1024;
const INLINE_PAYLOAD_BYTES: i64 = 32 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CoordinatorResumeParticipant {
    pub position: u8,
    pub participant: CoordinatorParticipant,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CoordinatorResumeSnapshot {
    pub status: Option<CrossCellTransactionStatus>,
    /// `None` retains chunked discovery; `Some([])` means no prepares remain.
    pub participants: Option<Vec<CoordinatorResumeParticipant>>,
}

/// Read status and small, unprepared immutable payloads through one Cell query.
///
/// Large inputs retain the existing chunk protocol. Terminal observations
/// contain no payloads: only their durable decision permits resolution.
pub struct ReadCoordinatorResume;

impl Query for ReadCoordinatorResume {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 7;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<ReadCrossCellTransactionInput>;
    type Output = Json<CoordinatorResumeSnapshot>;

    fn execute(context: &mut QueryContext<'_>, Json(input): Self::Input) -> Result<Self::Output> {
        let status = ReadCrossCellTransaction::execute(context, Json(input.clone()))?.0;
        let mut snapshot = CoordinatorResumeSnapshot {
            status,
            participants: None,
        };
        if snapshot
            .status
            .as_ref()
            .is_none_or(|status| status.decision != CoordinatorDecision::Begin)
        {
            return Ok(Json(snapshot));
        }
        // Inspect lengths before reading any blob. One handler runs under the
        // Cell's query observation; commands cannot compact between these reads.
        // Prepared participants already have durable evidence and need no payload.
        let rows = context.sql(&statement(
            "SELECT p.position, p.operation_chunks, COUNT(c.chunk), SUM(length(c.payload)) \
             FROM ddb_coordinator_participants p \
             LEFT JOIN ddb_transaction_payloads c ON c.transaction_id = p.transaction_id \
             AND c.position = p.position \
             WHERE p.transaction_id = ?1 AND p.prepared_sequence IS NULL \
             AND p.resolved_sequence IS NULL GROUP BY p.position ORDER BY p.position",
            vec![SqlValue::Blob(input.transaction_id.to_vec())],
        ))?;
        let mut positions = Vec::with_capacity(rows[0].rows.len());
        let mut bytes = 0_i64;
        for row in &rows[0].rows {
            let [
                SqlValue::Integer(position),
                SqlValue::Integer(chunks),
                SqlValue::Integer(stored),
                SqlValue::Integer(size),
            ] = row.as_slice()
            else {
                return Ok(Json(snapshot));
            };
            if *chunks != 1 || *stored != 1 || *size <= 0 || *size > INLINE_PAYLOAD_BYTES - bytes {
                return Ok(Json(snapshot));
            }
            bytes += size;
            positions.push(
                u8::try_from(*position)
                    .map_err(|_| Error::Command("invalid coordinator participant position"))?,
            );
        }
        let mut participants = Vec::with_capacity(positions.len());
        for position in positions {
            let Some(chunk) = ReadCoordinatorParticipant::execute(
                context,
                Json(ReadCoordinatorParticipantInput {
                    account_id: input.account_id.clone(),
                    transaction_id: input.transaction_id,
                    routing_key: input.routing_key.clone(),
                    position,
                    chunk: 0,
                }),
            )?
            else {
                return Ok(Json(snapshot));
            };
            participants.push(CoordinatorResumeParticipant {
                position,
                participant: CoordinatorParticipant {
                    target: chunk.target,
                    operations: serde_json::from_slice(&chunk.payload)?,
                },
            });
        }
        snapshot.participants = Some(participants);
        // The envelope and canonical JSON can exceed the raw payload sum.
        // Leave room for WireValue's four-byte length prefix and fall back
        // before transport admission rather than failing a valid large request.
        if serde_json::to_vec(&snapshot)?.len() > RESUME_OUTPUT_BYTES as usize - 4 {
            snapshot.participants = None;
        }
        Ok(Json(snapshot))
    }
}
