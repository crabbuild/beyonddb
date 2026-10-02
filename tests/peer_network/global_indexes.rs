use crate::*;
use beyonddb::{
    ReadPartitionIndexChange, RoutePageInput, RoutePageOutcome, data_target, global_index_target,
};
use cellule_runtime::{
    control::{ControlState, OwnerFence},
    identity::CellTarget,
};

const ACCOUNT: &str = "123456789012";
const TABLE: &str = "ServingIndexFailover";

pub(crate) struct IndexRecovery {
    pub(crate) targets: Vec<CellTarget>,
    pub(crate) table_id: String,
}

impl IndexRecovery {
    pub(crate) async fn create(sdk: &aws_sdk_dynamodb::Client, client: &CellClient) -> Self {
        use aws_sdk_dynamodb::types::{
            AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType,
            Projection, ProjectionType, ScalarAttributeType,
        };
        sdk.create_table()
            .table_name(TABLE)
            .billing_mode(BillingMode::PayPerRequest)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("bucket")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name("ByBucket")
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("bucket")
                            .key_type(KeyType::Hash)
                            .build()
                            .unwrap(),
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        let account = account_target(ACCOUNT).unwrap();
        let record = client
            .query::<DescribeTable>(&account, None, Json(TABLE.into()))
            .await
            .unwrap()
            .output
            .0
            .unwrap();
        let route = crate::single_leaf_route(client, &account, &record.id.clone())
            .await
            .unwrap();
        assert_eq!(route.partitions.len(), 1);
        let mut targets =
            vec![data_target(ACCOUNT, &record.id, &route.partitions[0].partition_id).unwrap()];
        let index = &record.global_secondary_indexes[0];
        let page = beyonddb::read_route_page(
            client,
            &beyonddb::account_target("123456789012").unwrap(),
            RoutePageInput {
                table_id: index.id.clone(),
                start_hash: None,
                after_lower: None,
                expected_epoch: None,
            },
        )
        .await
        .unwrap();
        let RoutePageOutcome::Page { partitions, .. } = page else {
            panic!("global index route missing")
        };
        assert_eq!(partitions.len(), 1);
        targets.push(global_index_target(ACCOUNT, &index.id, &partitions[0].partition_id).unwrap());
        Self::write(sdk, "before").await;
        Self {
            targets,
            table_id: record.id,
        }
    }

    pub(crate) async fn write(sdk: &aws_sdk_dynamodb::Client, value: &str) {
        sdk.put_item()
            .table_name(TABLE)
            .item("id", AwsAttributeValue::S("row".into()))
            .item("bucket", AwsAttributeValue::S("same".into()))
            .item("value", AwsAttributeValue::S(value.into()))
            .send()
            .await
            .unwrap();
    }

    pub(crate) async fn assert_settled(
        &self,
        sdk: &aws_sdk_dynamodb::Client,
        client: &CellClient,
        value: &str,
    ) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let read = sdk
                    .query()
                    .table_name(TABLE)
                    .index_name("ByBucket")
                    .key_condition_expression("#bucket = :bucket")
                    .expression_attribute_names("#bucket", "bucket")
                    .expression_attribute_values(":bucket", AwsAttributeValue::S("same".into()))
                    .send()
                    .await;
                if let Ok(read) = read
                    && read.items().len() == 1
                    && read.items()[0].get("value") == Some(&AwsAttributeValue::S(value.into()))
                    && let Ok(journal) = client
                        .query::<ReadPartitionIndexChange>(
                            &self.targets[0],
                            None,
                            Json(self.table_id.clone()),
                        )
                        .await
                    && journal.output.0.is_none()
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("signed index query and journal acknowledgement must converge");
    }

    pub(crate) async fn assert_owner(
        &self,
        authority: &CellAuthority,
        session: SessionId,
    ) -> Vec<OwnerFence> {
        let mut fences = Vec::new();
        for target in &self.targets {
            let current = authority.load(target.cell_id()).await.unwrap().unwrap();
            assert_eq!(current.value().owner.as_ref().unwrap().session, session);
            fences.push(current.value().owner_fence());
        }
        fences
    }

    pub(crate) async fn assert_recovered_authority(
        &self,
        authority: &CellAuthority,
        session: SessionId,
        before: &[OwnerFence],
    ) {
        assert_eq!(self.targets.len(), before.len());
        for (target, previous) in self.targets.iter().zip(before) {
            let current = authority.load(target.cell_id()).await.unwrap().unwrap();
            let control = current.value();
            assert_eq!(control.incarnation, previous.incarnation);
            assert!(control.epoch > previous.epoch, "owner epoch must advance");
            assert!(
                control.root.is_some(),
                "recovered root must remain published"
            );
            assert!(control.recovery.is_none(), "recovery must finish");
            match control.state {
                ControlState::Serving => {
                    assert_eq!(control.owner.as_ref().unwrap().session, session);
                }
                ControlState::Idle => assert!(control.owner.is_none()),
                state => panic!("recovered Cell has invalid state: {state:?}"),
            }
        }
    }
}
