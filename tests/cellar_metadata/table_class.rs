use crate::residency_fixture::{Fixture, SdkItem, runtimes};
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::types::{AttributeValue, TableClass};
use beyonddb::*;

#[test]
fn sdk_table_class_and_change_limit_survive_account_restoration() {
    let (host, core) = runtimes();
    host.block_on(async {
        let mut fixture = Fixture::new(host.handle(), core.handle(), 2, 16).await;
        // Ordered splitting requires two distinct observed keys in the source.
        // Keep the original row in each initial range and add a low-range sibling.
        let extra = SdkItem::from([
            ("id".into(), AttributeValue::S("first-sibling".into())),
            ("value".into(), AttributeValue::S("committed".into())),
        ]);
        fixture
            .sdk
            .put_item()
            .table_name("Residency")
            .set_item(Some(extra.clone()))
            .send()
            .await
            .unwrap();
        let initial = fixture
            .sdk
            .describe_table()
            .table_name("Residency")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(
            initial.table_class_summary.unwrap().table_class(),
            Some(&TableClass::Standard)
        );
        let invalid = fixture
            .sdk
            .update_table()
            .table_name("Residency")
            .table_class(TableClass::from("INVALID_CLASS"))
            .deletion_protection_enabled(true)
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            invalid.as_service_error().unwrap().code(),
            Some("ValidationException")
        );
        let unchanged = fixture
            .sdk
            .describe_table()
            .table_name("Residency")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(unchanged.deletion_protection_enabled, Some(false));

        let changed = fixture
            .sdk
            .update_table()
            .table_name("Residency")
            .table_class(TableClass::StandardInfrequentAccess)
            .deletion_protection_enabled(true)
            .send()
            .await
            .unwrap()
            .table_description
            .unwrap();
        let class = changed.table_class_summary.unwrap();
        assert_eq!(
            class.table_class(),
            Some(&TableClass::StandardInfrequentAccess)
        );
        assert!(class.last_update_date_time().is_some());
        assert_eq!(changed.deletion_protection_enabled, Some(true));
        let expected = &fixture.data[0].1;
        let item = fixture
            .sdk
            .get_item()
            .table_name("Residency")
            .key("id", expected["id"].clone())
            .consistent_read(true)
            .send()
            .await
            .unwrap()
            .item
            .unwrap();
        assert_eq!(&item, expected);

        // Installed range contracts predate the class update. Splitting must use
        // those immutable contracts while the account retains current settings.
        let table_id = initial.table_id.as_ref().unwrap();
        let account = account_target("123456789012").unwrap();
        let route = crate::single_leaf_route(&fixture.client, &account, table_id)
            .await
            .unwrap();
        let range = &route.partitions[0];
        fixture
            .provisioner
            .split_partition(
                "123456789012",
                fixture.client.clone(),
                table_id,
                range.partition_id,
                range
                    .lower
                    .clone()
                    .unwrap_or_else(bdb_keyspace::RangeKey::minimum),
            )
            .await
            .unwrap();
        assert_eq!(
            fixture
                .sdk
                .get_item()
                .table_name("Residency")
                .key("id", expected["id"].clone())
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item
                .as_ref(),
            Some(expected)
        );

        fixture.replace().await;
        assert_data(&fixture, &extra).await;
        let restored = fixture
            .sdk
            .describe_table()
            .table_name("Residency")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(restored.table_class_summary, Some(class));
        let reverted = fixture
            .sdk
            .update_table()
            .table_name("Residency")
            .table_class(TableClass::Standard)
            .send()
            .await
            .unwrap()
            .table_description
            .unwrap();
        let summary = reverted.table_class_summary.unwrap();
        assert_eq!(summary.table_class(), Some(&TableClass::Standard));

        fixture.replace().await;
        assert_data(&fixture, &extra).await;
        let refused = fixture
            .sdk
            .update_table()
            .table_name("Residency")
            .table_class(TableClass::StandardInfrequentAccess)
            .deletion_protection_enabled(false)
            .send()
            .await
            .unwrap_err();
        assert!(
            refused
                .as_service_error()
                .unwrap()
                .is_limit_exceeded_exception()
        );
        let unchanged = fixture
            .sdk
            .describe_table()
            .table_name("Residency")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(unchanged.table_class_summary, Some(summary.clone()));
        assert_eq!(unchanged.deletion_protection_enabled, Some(true));
        let repeated = fixture
            .sdk
            .update_table()
            .table_name("Residency")
            .table_class(TableClass::Standard)
            .send()
            .await
            .unwrap()
            .table_description
            .unwrap();
        assert_eq!(repeated.table_class_summary, Some(summary));

        let created = fixture
            .sdk
            .create_table()
            .table_name("ResidencyInfrequent")
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
            .table_class(TableClass::StandardInfrequentAccess)
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap()
            .table_description
            .unwrap();
        assert_eq!(
            created.table_class_summary.as_ref().unwrap().table_class(),
            Some(&TableClass::StandardInfrequentAccess)
        );
        fixture.replace().await;
        assert_data(&fixture, &extra).await;
        let restored = fixture
            .sdk
            .describe_table()
            .table_name("ResidencyInfrequent")
            .send()
            .await
            .unwrap()
            .table
            .unwrap();
        assert_eq!(restored.table_class_summary, created.table_class_summary);
        fixture.shutdown().await;
    });
}

async fn assert_data(fixture: &Fixture, extra: &SdkItem) {
    for expected in fixture
        .data
        .iter()
        .map(|(_, item)| item)
        .chain(std::iter::once(extra))
    {
        assert_eq!(
            fixture
                .sdk
                .get_item()
                .table_name("Residency")
                .key("id", expected["id"].clone())
                .consistent_read(true)
                .send()
                .await
                .unwrap()
                .item
                .as_ref(),
            Some(expected)
        );
    }
}
