//! Bounded parameterized SQL through Cellar's activation-bound storage API.

use std::ops::ControlFlow;

use cellar::sdk::Storage;
pub use cellar::sdk::primitives::sql::Value as SqlValue;
use sqlparser::{
    ast::{ObjectName, Statement, Visit, Visitor},
    dialect::SQLiteDialect,
    parser::Parser,
    tokenizer::{Token, Tokenizer},
};

use crate::{
    Error, Result,
    codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue},
};

const NULL_TAG: u8 = 0;
const INTEGER_TAG: u8 = 1;
const REAL_TAG: u8 = 2;
const TEXT_TAG: u8 = 3;
const BLOB_TAG: u8 = 4;
const MAX_COLUMNS: usize = 2_000;
const MAX_STATEMENTS: usize = 128;
const MAX_PARAMETERS: usize = 32_766;
const MAX_ROWS: usize = 1_000;
const MAX_BYTES: usize = 1 << 20;

/// Host policy for compiled BeyondDB SQL. Keep other Cellar primitives at the
/// host's selected policy. The default compatibility value ceiling is too small
/// for a legal 400-KiB DynamoDB string after JSON escaping.
pub fn host_limits() -> cellar::sdk::SqlLimits {
    cellar::sdk::SqlLimits {
        connection: Some(cellar::sdk::SqliteLimits {
            max_value_bytes: 8 * 1024 * 1024,
            max_statement_bytes: MAX_BYTES as i32,
            max_bind_parameters: MAX_PARAMETERS as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// One compiled, parameterized application statement.
#[derive(Clone, Debug)]
pub struct SqlStatement {
    pub sql: String,
    pub parameters: Vec<SqlValue>,
}

/// A bounded batch. Its caller supplies the surrounding cell transaction.
#[derive(Clone, Debug)]
pub struct SqlBatch {
    pub statements: Vec<SqlStatement>,
}

/// Owned values preserve BLOBs and signed 64-bit integers without JSON coercion.
#[derive(Clone, Debug)]
pub struct SqlResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub rows_affected: u64,
}

#[derive(Clone, Copy)]
pub enum AccessMode {
    ReadOnly,
    ReadWrite,
}

struct ApplicationSql {
    mode: AccessMode,
}

impl Visitor for ApplicationSql {
    type Break = Error;

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Error> {
        let allowed = match self.mode {
            AccessMode::ReadOnly => matches!(statement, Statement::Query(_)),
            AccessMode::ReadWrite => matches!(
                statement,
                Statement::Query(_)
                    | Statement::Insert(_)
                    | Statement::Update { .. }
                    | Statement::Delete(_)
            ),
        };
        if allowed {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(Error::Command(
                "SQL statement is outside this operation's authority",
            ))
        }
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Error> {
        let name = relation.to_string().to_ascii_lowercase();
        let name = name
            .rsplit('.')
            .next()
            .unwrap_or(&name)
            .trim_matches(['\'', '"', '`', '[', ']']);
        if name.starts_with("capacity_")
            || name.starts_with("bdb_runtime_")
            || name.starts_with("__sys_")
            || name.starts_with("sqlite_")
            || name.starts_with("pragma_")
        {
            return ControlFlow::Break(Error::Command("application SQL targets a protected table"));
        }
        ControlFlow::Continue(())
    }
}

fn validate_statement(statement: &SqlStatement, mode: AccessMode) -> Result<bool> {
    if statement.sql.len() > MAX_BYTES || statement.parameters.len() > MAX_PARAMETERS {
        return Err(Error::Command("SQL statement exceeds its budget"));
    }
    let syntax = without_index_hints(&statement.sql)?;
    let statements = Parser::parse_sql(&SQLiteDialect {}, &syntax)
        .map_err(|_| Error::Command("invalid application SQL"))?;
    if statements.len() != 1 {
        return Err(Error::Command("expected one parameterized SQL statement"));
    }
    if let ControlFlow::Break(error) = statements[0].visit(&mut ApplicationSql { mode }) {
        return Err(error);
    }
    Ok(matches!(
        statements[0],
        Statement::Insert(_) | Statement::Update { .. } | Statement::Delete(_)
    ))
}

// sqlparser does not accept SQLite's INDEXED BY / NOT INDEXED hints. Remove
// only those token sequences for authority analysis; SQLite executes the exact
// original statement. Quoted strings and identifiers are never rewritten.
fn without_index_hints(sql: &str) -> Result<String> {
    if !sql.to_ascii_uppercase().contains("INDEXED") {
        return Ok(sql.to_owned());
    }
    let tokens: Vec<_> = Tokenizer::new(&SQLiteDialect {}, sql)
        .with_unescape(false)
        .tokenize()
        .map_err(|_| Error::Command("invalid application SQL tokens"))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();
    let word = |token: Option<&Token>, expected: &str| matches!(token, Some(Token::Word(word)) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected));
    let mut output = Vec::with_capacity(tokens.len());
    let mut index = 0;
    while index < tokens.len() {
        if word(tokens.get(index), "INDEXED")
            && word(tokens.get(index + 1), "BY")
            && matches!(tokens.get(index + 2), Some(Token::Word(_)))
        {
            index += 3;
        } else if word(tokens.get(index), "NOT") && word(tokens.get(index + 1), "INDEXED") {
            index += 2;
        } else {
            output.push(tokens[index].to_string());
            index += 1;
        }
    }
    Ok(output.join(" "))
}

/// Execute only inside the caller's Cellar transaction. An error must propagate
/// to that boundary so all statements, including earlier writes, roll back.
pub fn execute(storage: &Storage, batch: &SqlBatch, mode: AccessMode) -> Result<Vec<SqlResultSet>> {
    if batch.statements.len() > MAX_STATEMENTS {
        return Err(Error::Command("too many SQL statements"));
    }
    // Validate the whole batch before executing its first statement.
    let mut input_bytes = 0_usize;
    let mut mutations = Vec::with_capacity(batch.statements.len());
    for statement in &batch.statements {
        mutations.push(validate_statement(statement, mode)?);
        add_size(&mut input_bytes, statement.sql.len())?;
        for value in &statement.parameters {
            add_size(&mut input_bytes, value_size(value))?;
        }
    }
    let mut result_bytes = 0_usize;
    let mut row_count = 0_usize;
    let mut results = Vec::with_capacity(batch.statements.len());
    for (statement, mutating) in batch.statements.iter().zip(mutations) {
        let mut cursor = storage.sql_cursor(&statement.sql, &statement.parameters)?;
        let columns = cursor.column_names().to_vec();
        for name in &columns {
            add_size(&mut result_bytes, name.len())?;
        }
        let mut rows = Vec::new();
        while let Some(row) = cursor.next_raw()? {
            row_count += 1;
            if row_count > MAX_ROWS {
                return Err(Error::Command("SQL result exceeds its row budget"));
            }
            for value in &row {
                add_size(&mut result_bytes, value_size(value))?;
            }
            rows.push(row);
        }
        if matches!(mode, AccessMode::ReadOnly) && cursor.rows_written() != 0 {
            return Err(Error::Command("read-only SQL changed rows"));
        }
        cursor.close()?;
        // SDK rows_written meters all writes, including triggers. The domain
        // SQL contract follows SQLite changes(): rows changed directly by this
        // statement. Keep the observation on the same activation/connection.
        let rows_affected = if mutating {
            let changed = storage
                .sql_cursor("SELECT changes()", &[])?
                .to_raw_array()?;
            match changed.as_slice() {
                [row] => match row.as_slice() {
                    [SqlValue::Integer(count)] => u64::try_from(*count)
                        .map_err(|_| Error::Command("invalid SQL change count"))?,
                    _ => return Err(Error::Command("invalid SQL change count")),
                },
                _ => return Err(Error::Command("missing SQL change count")),
            }
        } else {
            0
        };
        results.push(SqlResultSet {
            columns,
            rows,
            rows_affected,
        });
    }
    Ok(results)
}

fn value_size(value: &SqlValue) -> usize {
    match value {
        SqlValue::Null => 1,
        SqlValue::Integer(_) | SqlValue::Real(_) => 9,
        SqlValue::Text(value) => 5_usize.saturating_add(value.len()),
        SqlValue::Blob(value) => 5_usize.saturating_add(value.len()),
    }
}

fn add_size(size: &mut usize, increment: usize) -> Result<()> {
    *size = size
        .checked_add(increment)
        .filter(|size| *size <= MAX_BYTES)
        .ok_or(Error::Command("SQL operation exceeds its byte budget"))?;
    Ok(())
}

// SQL wire encodings adapted from Cellule (see NOTICE).
impl WireValue for SqlValue {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        match self {
            Self::Null => encoder.write_u8(NULL_TAG),
            Self::Integer(value) => {
                encoder.write_u8(INTEGER_TAG)?;
                encoder.write_i64(*value)
            }
            Self::Real(value) => {
                encoder.write_u8(REAL_TAG)?;
                encoder.write_f64(*value)
            }
            Self::Text(value) => {
                encoder.write_u8(TEXT_TAG)?;
                encoder.write_text(value)
            }
            Self::Blob(value) => {
                encoder.write_u8(BLOB_TAG)?;
                encoder.write_bytes(value)
            }
        }
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        match decoder.read_u8()? {
            NULL_TAG => Ok(Self::Null),
            INTEGER_TAG => Ok(Self::Integer(decoder.read_i64()?)),
            REAL_TAG => Ok(Self::Real(decoder.read_f64()?)),
            TEXT_TAG => Ok(Self::Text(decoder.read_text()?.to_owned())),
            BLOB_TAG => Ok(Self::Blob(decoder.read_bytes()?.to_vec())),
            _ => Err(CodecError::Invalid("invalid SQL value tag")),
        }
    }
}

impl WireValue for SqlStatement {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_text(&self.sql)?;
        encoder.write_count(self.parameters.len())?;
        for parameter in &self.parameters {
            parameter.encode(encoder)?;
        }
        Ok(())
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        let sql = decoder.read_text()?.to_owned();
        let count = bounded_count(decoder, MAX_PARAMETERS, "too many SQL parameters")?;
        let mut parameters = Vec::with_capacity(count.min(64));
        for _ in 0..count {
            parameters.push(SqlValue::decode(decoder)?);
        }
        Ok(Self { sql, parameters })
    }
}

impl WireValue for SqlBatch {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_count(self.statements.len())?;
        for statement in &self.statements {
            statement.encode(encoder)?;
        }
        Ok(())
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        let count = bounded_count(decoder, MAX_STATEMENTS, "too many SQL statements")?;
        let mut statements = Vec::with_capacity(count);
        for _ in 0..count {
            statements.push(SqlStatement::decode(decoder)?);
        }
        Ok(Self { statements })
    }
}

impl WireValue for Vec<SqlResultSet> {
    fn encode(&self, encoder: &mut BoundedEncoder) -> std::result::Result<(), CodecError> {
        encoder.write_count(self.len())?;
        for result in self {
            encode_result(result, encoder)?;
        }
        Ok(())
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> std::result::Result<Self, CodecError> {
        let count = bounded_count(decoder, MAX_STATEMENTS, "too many SQL result sets")?;
        let mut results = Vec::with_capacity(count);
        let mut remaining_rows = MAX_ROWS;
        for _ in 0..count {
            let result = decode_result(decoder, remaining_rows)?;
            remaining_rows = remaining_rows
                .checked_sub(result.rows.len())
                .ok_or(CodecError::Invalid("too many SQL result rows"))?;
            results.push(result);
        }
        Ok(results)
    }
}

fn encode_result(
    result: &SqlResultSet,
    encoder: &mut BoundedEncoder,
) -> std::result::Result<(), CodecError> {
    encoder.write_count(result.columns.len())?;
    for column in &result.columns {
        encoder.write_text(column)?;
    }
    encoder.write_count(result.rows.len())?;
    for row in &result.rows {
        encoder.write_count(row.len())?;
        for value in row {
            value.encode(encoder)?;
        }
    }
    encoder.write_u64(result.rows_affected)
}

fn decode_result(
    decoder: &mut BoundedDecoder<'_>,
    remaining_rows: usize,
) -> std::result::Result<SqlResultSet, CodecError> {
    let column_count = bounded_count(decoder, MAX_COLUMNS, "too many SQL result columns")?;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        columns.push(decoder.read_text()?.to_owned());
    }
    let row_count = bounded_count(decoder, remaining_rows, "too many SQL result rows")?;
    let mut rows = Vec::with_capacity(row_count.min(64));
    for _ in 0..row_count {
        let value_count = bounded_count(decoder, MAX_COLUMNS, "too many SQL row values")?;
        if value_count != column_count {
            return Err(CodecError::Invalid("SQL row width does not match columns"));
        }
        let mut row = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            row.push(SqlValue::decode(decoder)?);
        }
        rows.push(row);
    }
    let rows_affected = decoder.read_u64()?;
    if (columns.is_empty() && !rows.is_empty()) || (!columns.is_empty() && rows_affected != 0) {
        return Err(CodecError::Invalid("inconsistent SQL result shape"));
    }
    Ok(SqlResultSet {
        columns,
        rows,
        rows_affected,
    })
}

fn bounded_count(
    decoder: &mut BoundedDecoder<'_>,
    maximum: usize,
    message: &'static str,
) -> std::result::Result<usize, CodecError> {
    let count = decoder.read_count()?;
    if count > maximum {
        return Err(CodecError::Invalid(message));
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statement(sql: &str) -> SqlStatement {
        SqlStatement {
            sql: sql.into(),
            parameters: vec![],
        }
    }

    #[test]
    fn sqlite_index_hints_preserve_read_authority_and_literals() {
        for sql in [
            "SELECT item FROM items INDEXED BY items_by_key WHERE key = ?1",
            "SELECT item FROM items NOT INDEXED WHERE key = ?1",
            "SELECT 'INDEXED BY fake; DELETE FROM items' FROM items INDEXED BY [items_by_key]",
            "SELECT 'it''s INDEXED BY fake' FROM items INDEXED BY items_by_key WHERE key = ?1",
        ] {
            validate_statement(&statement(sql), AccessMode::ReadOnly).unwrap();
        }
        for sql in [
            "DELETE FROM items INDEXED BY items_by_key WHERE key = ?1",
            "SELECT item FROM items INDEXED BY items_by_key; DELETE FROM items",
            "SELECT * FROM bdb_runtime_receipts INDEXED BY receipt_expiry",
        ] {
            assert!(
                validate_statement(&statement(sql), AccessMode::ReadOnly).is_err(),
                "{sql}"
            );
        }
    }

    #[test]
    fn read_only_accepts_ctes_but_rejects_mutation_and_transaction_control() {
        validate_statement(
            &statement("WITH x AS (SELECT 1 AS n) SELECT n FROM x"),
            AccessMode::ReadOnly,
        )
        .unwrap();
        for sql in [
            "DELETE FROM items",
            "WITH x AS (SELECT 1) DELETE FROM items",
            "BEGIN",
            "COMMIT",
            "PRAGMA query_only = OFF",
            "SELECT 1; DELETE FROM items",
        ] {
            assert!(
                validate_statement(&statement(sql), AccessMode::ReadOnly).is_err(),
                "{sql}"
            );
        }
    }

    #[test]
    fn commands_cannot_escape_the_transaction_or_change_the_schema() {
        for sql in [
            "BEGIN",
            "ROLLBACK",
            "SAVEPOINT escape",
            "CREATE TABLE items (v)",
            "ATTACH DATABASE 'other' AS other",
            "PRAGMA journal_mode = DELETE",
        ] {
            assert!(
                validate_statement(&statement(sql), AccessMode::ReadWrite).is_err(),
                "{sql}"
            );
        }
        validate_statement(
            &statement("UPDATE items SET v = ?1 WHERE k = ?2"),
            AccessMode::ReadWrite,
        )
        .unwrap();
    }

    #[test]
    fn application_sql_cannot_rewrite_receipts_or_inspect_runtime_tables() {
        for sql in [
            "UPDATE bdb_runtime_state SET sequence = 0",
            "DELETE FROM main.\"bdb_runtime_receipts\"",
            "SELECT * FROM [BDB_RUNTIME_STATE]",
            "SELECT * FROM sqlite_schema",
            "SELECT * FROM __sys_cell_kv",
        ] {
            assert!(
                validate_statement(&statement(sql), AccessMode::ReadWrite).is_err(),
                "{sql}"
            );
        }
    }
}
