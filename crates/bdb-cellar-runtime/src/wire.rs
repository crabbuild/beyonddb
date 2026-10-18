//! Private transport framing, separate from each operation's declared codec.

use crate::{
    Error, Result,
    codec::{BoundedDecoder, BoundedEncoder},
    identity::*,
};

pub(crate) const ENVELOPE_LIMIT: usize = 4 * 1024 * 1024 + 128 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationIdentity {
    pub request_id: RequestId,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

impl MutationIdentity {
    pub(crate) fn validate(self, now_ms: i64, replay: bool) -> Result<()> {
        if now_ms < 0
            || self.issued_at_ms < 0
            || self.expires_at_ms <= self.issued_at_ms
            || self.expires_at_ms.saturating_sub(self.issued_at_ms) > 86_400_000
            || self.issued_at_ms > now_ms.saturating_add(300_000)
            || (!replay && self.expires_at_ms <= now_ms)
        {
            return Err(Error::Command("invalid mutation identity lifetime"));
        }
        Ok(())
    }
}

pub(crate) struct Envelope {
    pub target: CellTarget,
    pub id: u32,
    pub version: u32,
    pub mutation: Option<MutationIdentity>,
    pub minimum_sequence: u64,
    pub input: Vec<u8>,
}

impl Envelope {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = BoundedEncoder::new(ENVELOPE_LIMIT as u32)?;
        out.write_bytes(b"BDB1")?;
        out.write_bytes(self.target.tenant().as_bytes())?;
        out.write_bytes(self.target.application().as_bytes())?;
        out.write_bytes(self.target.namespace().as_bytes())?;
        out.write_bytes(self.target.partition())?;
        out.write_u32(self.id)?;
        out.write_u32(self.version)?;
        out.write_bool(self.mutation.is_some())?;
        if let Some(identity) = self.mutation {
            out.write_bytes(identity.request_id.as_bytes())?;
            out.write_i64(identity.issued_at_ms)?;
            out.write_i64(identity.expires_at_ms)?;
        }
        out.write_u64(self.minimum_sequence)?;
        out.write_bytes(&self.input)?;
        Ok(out.finish())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut input = BoundedDecoder::new(bytes, ENVELOPE_LIMIT as u32)?;
        if input.read_bytes()? != b"BDB1" {
            return Err(Error::Command("invalid invocation frame"));
        }
        let tenant = TenantId::try_from(input.read_bytes()?)?;
        let app = ApplicationId::try_from(input.read_bytes()?)?;
        let namespace = NamespaceId::try_from(input.read_bytes()?)?;
        let target = CellTarget::new(tenant, app, namespace, input.read_bytes()?)?;
        let id = input.read_u32()?;
        let version = input.read_u32()?;
        let mutation = if input.read_bool()? {
            Some(MutationIdentity {
                request_id: RequestId::try_from(input.read_bytes()?)?,
                issued_at_ms: input.read_i64()?,
                expires_at_ms: input.read_i64()?,
            })
        } else {
            None
        };
        let minimum_sequence = input.read_u64()?;
        let bytes = input.read_bytes()?;
        if bytes.len() > crate::codec::MAX_OPERATION_BYTES {
            return Err(Error::Command("operation input exceeds its budget"));
        }
        let value = Self {
            target,
            id,
            version,
            mutation,
            minimum_sequence,
            input: bytes.to_vec(),
        };
        input.finish()?;
        Ok(value)
    }
}

pub(crate) struct Reply {
    pub sequence: u64,
    pub rejected: bool,
    pub output: Vec<u8>,
}

impl Reply {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = BoundedEncoder::new(ENVELOPE_LIMIT as u32)?;
        out.write_bytes(b"BDR1")?;
        out.write_u64(self.sequence)?;
        out.write_bool(self.rejected)?;
        out.write_bytes(&self.output)?;
        Ok(out.finish())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut input = BoundedDecoder::new(bytes, ENVELOPE_LIMIT as u32)?;
        if input.read_bytes()? != b"BDR1" {
            return Err(Error::Command("invalid result frame"));
        }
        let sequence = input.read_u64()?;
        let rejected = input.read_bool()?;
        let bytes = input.read_bytes()?;
        if bytes.len() > crate::codec::MAX_OPERATION_BYTES {
            return Err(Error::Command("operation output exceeds its budget"));
        }
        let value = Self {
            sequence,
            rejected,
            output: bytes.to_vec(),
        };
        input.finish()?;
        Ok(value)
    }
}
