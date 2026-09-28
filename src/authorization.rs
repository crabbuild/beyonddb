//! Durable user and role policies and boundaries for ExtendDB authorization decisions.

use cellule_runtime::client::{CellClient, InvocationError};
use cellule_runtime::registry::{Command, CommandContext, CommandResult, Query, QueryContext};
use extenddb_auth::policy::document::PolicyDocument;
use extenddb_storage::BoxedFuture;
use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};
use serde::{Deserialize, Serialize};

use crate::backend::{cell_error, mutation_identity};
use crate::table::statement;
use crate::{Error, Json, MODULE, Result, SqlValue, account_target};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum PrincipalKind {
    User,
    Role,
}

impl PrincipalKind {
    fn code(&self) -> i64 {
        match self {
            Self::User => 0,
            Self::Role => 1,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct PrincipalPolicy {
    account_id: String,
    kind: PrincipalKind,
    principal_name: String,
    policy_name: String,
    document: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct PrincipalPolicyKey {
    account_id: String,
    kind: PrincipalKind,
    principal_name: String,
    policy_name: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Principal {
    account_id: String,
    kind: PrincipalKind,
    name: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct PrincipalBoundary {
    account_id: String,
    kind: PrincipalKind,
    name: String,
    document: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SetPrincipalBoundaryOutcome {
    Stored,
    Removed(bool),
    Invalid,
}

pub(crate) struct SetPrincipalBoundary;

impl Command for SetPrincipalBoundary {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 43;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<PrincipalBoundary>;
    type Output = Json<SetPrincipalBoundaryOutcome>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(boundary): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        if account_target(&boundary.account_id)? != *context.target() {
            return Err(Error::Identity(
                "principal boundary reached the wrong account",
            ));
        }
        if !valid_name(&boundary.name)
            || boundary
                .document
                .as_ref()
                .is_some_and(|document| PolicyDocument::from_json(document).is_err())
        {
            return Ok(CommandResult::Rejected(Json(
                SetPrincipalBoundaryOutcome::Invalid,
            )));
        }
        let parameters = vec![
            SqlValue::Integer(boundary.kind.code()),
            SqlValue::Text(boundary.name),
        ];
        let outcome = if let Some(document) = boundary.document {
            context.sql(&statement(
                "INSERT INTO ddb_iam_principal_boundaries (principal_kind, principal_name, document) \
                 VALUES (?1, ?2, ?3) ON CONFLICT (principal_kind, principal_name) \
                 DO UPDATE SET document = excluded.document",
                parameters.into_iter().chain([SqlValue::Text(document)]).collect(),
            ))?;
            SetPrincipalBoundaryOutcome::Stored
        } else {
            let result = context.sql(&statement(
                "DELETE FROM ddb_iam_principal_boundaries WHERE principal_kind = ?1 AND principal_name = ?2",
                parameters,
            ))?;
            SetPrincipalBoundaryOutcome::Removed(result[0].rows_affected != 0)
        };
        Ok(CommandResult::Success(Json(outcome)))
    }
}

pub(crate) struct ReadPrincipalBoundary;

impl Query for ReadPrincipalBoundary {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 43;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<Principal>;
    type Output = Json<Option<String>>;

    fn execute(
        context: &mut QueryContext<'_>,
        Json(principal): Self::Input,
    ) -> Result<Self::Output> {
        if account_target(&principal.account_id)?.cell_id() != context.cell_id() {
            return Err(Error::Identity(
                "principal boundary reached the wrong account",
            ));
        }
        if !valid_name(&principal.name) {
            return Err(Error::Identity("invalid principal name"));
        }
        let result = context.sql(&statement(
            "SELECT document FROM ddb_iam_principal_boundaries WHERE principal_kind = ?1 AND principal_name = ?2",
            vec![SqlValue::Integer(principal.kind.code()), SqlValue::Text(principal.name)],
        ))?;
        let Some(row) = result[0].rows.first() else {
            return Ok(Json(None));
        };
        let [SqlValue::Text(document)] = row.as_slice() else {
            return Err(Error::Command("invalid principal boundary row"));
        };
        Ok(Json(Some(document.clone())))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PutPrincipalPolicyOutcome {
    Stored,
    Invalid,
}

pub(crate) struct PutPrincipalPolicy;

impl Command for PutPrincipalPolicy {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 13;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<PrincipalPolicy>;
    type Output = Json<PutPrincipalPolicyOutcome>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(policy): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        if account_target(&policy.account_id)? != *context.target() {
            return Err(Error::Identity(
                "principal policy reached the wrong account",
            ));
        }
        if !valid_name(&policy.principal_name)
            || !valid_name(&policy.policy_name)
            || PolicyDocument::from_json(&policy.document).is_err()
        {
            return Ok(CommandResult::Rejected(Json(
                PutPrincipalPolicyOutcome::Invalid,
            )));
        }
        context.sql(&statement(
            "INSERT INTO ddb_iam_principal_policies (principal_kind, principal_name, policy_name, document) \
             VALUES (?1, ?2, ?3, ?4) ON CONFLICT (principal_kind, principal_name, policy_name) \
             DO UPDATE SET document = excluded.document",
            vec![
                SqlValue::Integer(policy.kind.code()),
                SqlValue::Text(policy.principal_name),
                SqlValue::Text(policy.policy_name),
                SqlValue::Text(policy.document),
            ],
        ))?;
        Ok(CommandResult::Success(Json(
            PutPrincipalPolicyOutcome::Stored,
        )))
    }
}

pub(crate) struct DeletePrincipalPolicy;

impl Command for DeletePrincipalPolicy {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 14;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<PrincipalPolicyKey>;
    type Output = Json<bool>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        Json(key): Self::Input,
    ) -> Result<CommandResult<Self::Output>> {
        if account_target(&key.account_id)? != *context.target() {
            return Err(Error::Identity(
                "principal policy removal reached the wrong account",
            ));
        }
        if !valid_name(&key.principal_name) || !valid_name(&key.policy_name) {
            return Err(Error::Identity("invalid principal policy name"));
        }
        let changed = context.sql(&statement(
            "DELETE FROM ddb_iam_principal_policies WHERE principal_kind = ?1 AND principal_name = ?2 AND policy_name = ?3",
            vec![
                SqlValue::Integer(key.kind.code()),
                SqlValue::Text(key.principal_name),
                SqlValue::Text(key.policy_name),
            ],
        ))?;
        Ok(CommandResult::Success(Json(changed[0].rows_affected != 0)))
    }
}

pub(crate) struct ReadPrincipalPolicies;

impl Query for ReadPrincipalPolicies {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 13;
    const CODEC_VERSION: u32 = 1;
    type Input = Json<Principal>;
    type Output = Json<Vec<String>>;

    fn execute(
        context: &mut QueryContext<'_>,
        Json(principal): Self::Input,
    ) -> Result<Self::Output> {
        if account_target(&principal.account_id)?.cell_id() != context.cell_id() {
            return Err(Error::Identity(
                "principal policies reached the wrong account",
            ));
        }
        if !valid_name(&principal.name) {
            return Err(Error::Identity("invalid principal name"));
        }
        let result = context.sql(&statement(
            "SELECT document FROM ddb_iam_principal_policies WHERE principal_kind = ?1 AND principal_name = ?2 ORDER BY policy_name",
            vec![SqlValue::Integer(principal.kind.code()), SqlValue::Text(principal.name)],
        ))?;
        let mut documents = Vec::with_capacity(result[0].rows.len());
        for row in &result[0].rows {
            let [SqlValue::Text(document)] = row.as_slice() else {
                return Err(Error::Command("invalid principal policy row"));
            };
            documents.push(document.clone());
        }
        Ok(Json(documents))
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 128 && name.bytes().all(|byte| byte.is_ascii_graphic())
}

/// Cell-backed inline principal policies and permission boundaries for ExtendDB.
///
/// Group, session, and tag state have no provisioning path yet;
/// those lookups are empty. Serving callers must disable the authorization
/// cache until policy mutation invalidation is connected.
pub struct CellAuthorizationStore {
    client: CellClient,
}

impl CellAuthorizationStore {
    /// Bind a client that can reach account Cells.
    pub fn new(client: CellClient) -> Self {
        Self { client }
    }

    /// Validate and durably attach or replace one inline user policy.
    pub async fn put_user_policy(
        &self,
        account_id: &str,
        user_name: &str,
        policy_name: &str,
        document: &str,
    ) -> std::result::Result<(), StorageError> {
        self.put_policy(
            account_id,
            PrincipalKind::User,
            user_name,
            policy_name,
            document,
        )
        .await
    }

    /// Validate and durably attach or replace one inline role policy.
    pub async fn put_role_policy(
        &self,
        account_id: &str,
        role_name: &str,
        policy_name: &str,
        document: &str,
    ) -> std::result::Result<(), StorageError> {
        self.put_policy(
            account_id,
            PrincipalKind::Role,
            role_name,
            policy_name,
            document,
        )
        .await
    }

    async fn put_policy(
        &self,
        account_id: &str,
        kind: PrincipalKind,
        principal_name: &str,
        policy_name: &str,
        document: &str,
    ) -> std::result::Result<(), StorageError> {
        let target = account_target(account_id)
            .map_err(|error| StorageError::Validation(error.to_string()))?;
        let policy = PrincipalPolicy {
            account_id: account_id.into(),
            kind,
            principal_name: principal_name.into(),
            policy_name: policy_name.into(),
            document: document.into(),
        };
        match self
            .client
            .command::<PutPrincipalPolicy>(&target, mutation_identity()?, Json(policy))
            .await
        {
            Ok(committed) if committed.output.0 == PutPrincipalPolicyOutcome::Stored => Ok(()),
            Err(InvocationError::Rejected(committed))
                if committed.output.0 == PutPrincipalPolicyOutcome::Invalid =>
            {
                Err(StorageError::Validation("invalid principal policy".into()))
            }
            Ok(_) => Err(StorageError::Internal(
                "unexpected principal policy result".into(),
            )),
            Err(error) => Err(cell_error(error)),
        }
    }

    /// Durably remove one inline user policy; false means it was absent.
    pub async fn delete_user_policy(
        &self,
        account_id: &str,
        user_name: &str,
        policy_name: &str,
    ) -> std::result::Result<bool, StorageError> {
        self.delete_policy(account_id, PrincipalKind::User, user_name, policy_name)
            .await
    }

    /// Durably remove one inline role policy; false means it was absent.
    pub async fn delete_role_policy(
        &self,
        account_id: &str,
        role_name: &str,
        policy_name: &str,
    ) -> std::result::Result<bool, StorageError> {
        self.delete_policy(account_id, PrincipalKind::Role, role_name, policy_name)
            .await
    }

    /// Set or remove a user permission boundary in the account Cell.
    ///
    /// `None` removes the boundary; the returned flag then indicates whether it existed.
    pub async fn set_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
        document: Option<&str>,
    ) -> std::result::Result<bool, StorageError> {
        self.set_boundary(account_id, PrincipalKind::User, user_name, document)
            .await
    }

    /// Set or remove a role permission boundary in the account Cell.
    ///
    /// `None` removes the boundary; the returned flag then indicates whether it existed.
    pub async fn set_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
        document: Option<&str>,
    ) -> std::result::Result<bool, StorageError> {
        self.set_boundary(account_id, PrincipalKind::Role, role_name, document)
            .await
    }

    async fn set_boundary(
        &self,
        account_id: &str,
        kind: PrincipalKind,
        name: &str,
        document: Option<&str>,
    ) -> std::result::Result<bool, StorageError> {
        let target = account_target(account_id)
            .map_err(|error| StorageError::Validation(error.to_string()))?;
        let boundary = PrincipalBoundary {
            account_id: account_id.into(),
            kind,
            name: name.into(),
            document: document.map(str::to_owned),
        };
        match self
            .client
            .command::<SetPrincipalBoundary>(&target, mutation_identity()?, Json(boundary))
            .await
        {
            Ok(committed) => match committed.output.0 {
                SetPrincipalBoundaryOutcome::Stored => Ok(true),
                SetPrincipalBoundaryOutcome::Removed(existed) => Ok(existed),
                SetPrincipalBoundaryOutcome::Invalid => Err(StorageError::Internal(
                    "unexpected successful boundary result".into(),
                )),
            },
            Err(InvocationError::Rejected(committed))
                if committed.output.0 == SetPrincipalBoundaryOutcome::Invalid =>
            {
                Err(StorageError::Validation(
                    "invalid principal boundary".into(),
                ))
            }
            Err(InvocationError::Rejected(_)) => Err(StorageError::Internal(
                "unexpected rejected boundary result".into(),
            )),
            Err(error) => Err(cell_error(error)),
        }
    }

    async fn delete_policy(
        &self,
        account_id: &str,
        kind: PrincipalKind,
        principal_name: &str,
        policy_name: &str,
    ) -> std::result::Result<bool, StorageError> {
        if !valid_name(principal_name) || !valid_name(policy_name) {
            return Err(StorageError::Validation(
                "invalid principal policy name".into(),
            ));
        }
        let target = account_target(account_id)
            .map_err(|error| StorageError::Validation(error.to_string()))?;
        let key = PrincipalPolicyKey {
            account_id: account_id.into(),
            kind,
            principal_name: principal_name.into(),
            policy_name: policy_name.into(),
        };
        let result = self
            .client
            .command::<DeletePrincipalPolicy>(&target, mutation_identity()?, Json(key))
            .await
            .map_err(cell_error)?;
        Ok(result.output.0)
    }

    fn policies(
        &self,
        account_id: &str,
        kind: PrincipalKind,
        name: &str,
    ) -> BoxedFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            let target = account_target(&account_id)
                .map_err(|_| OpError::Validation("invalid account ID".into()))?;
            self.client
                .query::<ReadPrincipalPolicies>(
                    &target,
                    None,
                    Json(Principal {
                        account_id,
                        kind,
                        name,
                    }),
                )
                .await
                .map(|result| result.output.0)
                .map_err(|_| OpError::Internal("authorization Cell unavailable".into()))
        })
    }

    fn boundary(
        &self,
        account_id: &str,
        kind: PrincipalKind,
        name: &str,
    ) -> BoxedFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            let target = account_target(&account_id)
                .map_err(|_| OpError::Validation("invalid account ID".into()))?;
            self.client
                .query::<ReadPrincipalBoundary>(
                    &target,
                    None,
                    Json(Principal {
                        account_id,
                        kind,
                        name,
                    }),
                )
                .await
                .map(|result| result.output.0)
                .map_err(|_| OpError::Internal("authorization Cell unavailable".into()))
        })
    }
}

impl AuthorizationStore for CellAuthorizationStore {
    fn fetch_user_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxedFuture<'_, OpResult<Vec<String>>> {
        self.policies(account_id, PrincipalKind::User, user_name)
    }

    fn fetch_user_group_policies(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxedFuture<'_, OpResult<Vec<String>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxedFuture<'_, OpResult<Option<String>>> {
        self.boundary(account_id, PrincipalKind::User, user_name)
    }

    fn fetch_role_policies(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxedFuture<'_, OpResult<Vec<String>>> {
        self.policies(account_id, PrincipalKind::Role, role_name)
    }

    fn fetch_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxedFuture<'_, OpResult<Option<String>>> {
        self.boundary(account_id, PrincipalKind::Role, role_name)
    }

    fn fetch_session_data(
        &self,
        _account_id: &str,
        _role_name: &str,
        _session_name: &str,
    ) -> BoxedFuture<'_, OpResult<Option<SessionData>>> {
        Box::pin(async { Ok(None) })
    }

    fn fetch_user_tags(
        &self,
        _account_id: &str,
        _user_name: &str,
    ) -> BoxedFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_role_tags(
        &self,
        _account_id: &str,
        _role_name: &str,
    ) -> BoxedFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn fetch_resource_tags(&self, _arn: &str) -> BoxedFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}
