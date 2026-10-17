use bdb_keyspace::{
    Component as C, Directory, Error, KeyKind, KeySchema, KeySpan, RangeKey, SortCondition as S,
};

fn schema() -> KeySchema {
    KeySchema {
        partition: KeyKind::String,
        sort: Some(KeyKind::Number),
    }
}

fn key(pk: &str, sk: &str) -> RangeKey {
    schema().encode(C::String(pk), Some(C::Number(sk))).unwrap()
}

fn directory() -> Directory {
    Directory::initial(
        vec![key("tenant-a", "10"), key("tenant-a", "20")],
        vec![[1; 16], [2; 16], [3; 16]],
    )
    .unwrap()
}

#[test]
fn decimal_order_is_exact_and_canonical_at_extremes() {
    let values = [
        "-9.9999999999999999999999999999999999999e125",
        "-1e125",
        "-10",
        "-1.1",
        "-1.01",
        "-1",
        "-0.1",
        "-1e-130",
        "0",
        "1e-130",
        "0.1",
        "1",
        "1.01",
        "1.1",
        "10",
        "1e125",
        "9.9999999999999999999999999999999999999e125",
    ];
    let keys: Vec<_> = values.iter().map(|value| key("p", value)).collect();
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(key("p", "1"), key("p", "1.0000e0"));
    assert_eq!(key("p", "0"), key("p", "-0.00"));
    for number in [
        "1e126",
        "1e-131",
        "1.12345678901234567890123456789012345678",
        "NaN",
        "1e9223372036854775807",
        "1e-9223372036854775808",
    ] {
        assert_eq!(
            schema().encode(C::String("p"), Some(C::Number(number))),
            Err(Error::Number)
        );
    }
}

#[test]
fn decimal_pairs_match_bigdecimal_comparisons() {
    let values: Vec<String> = (-20..=20)
        .flat_map(|integer| (-4..=4).map(move |scale| format!("{integer}e{scale}")))
        .collect();
    let keys: Vec<_> = values.iter().map(|value| key("p", value)).collect();
    let decimals: Vec<bigdecimal::BigDecimal> =
        values.iter().map(|value| value.parse().unwrap()).collect();
    for a in 0..values.len() {
        for b in 0..values.len() {
            assert_eq!(
                keys[a].cmp(&keys[b]),
                decimals[a].cmp(&decimals[b]),
                "{} versus {}",
                values[a],
                values[b]
            );
        }
    }
}

#[test]
fn byte_order_survives_zeroes_prefixes_and_composite_boundaries() {
    let binary = KeySchema {
        partition: KeyKind::Binary,
        sort: Some(KeyKind::Binary),
    };
    let values: Vec<&[u8]> = vec![
        b"\0", b"\0\0", b"\0\xff", b"a", b"a\0", b"aa", b"b", b"\xff",
    ];
    let keys: Vec<_> = values
        .iter()
        .map(|value| {
            binary
                .encode(C::Binary(value), Some(C::Binary(b"x")))
                .unwrap()
        })
        .collect();
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    let strings = KeySchema {
        partition: KeyKind::String,
        sort: Some(KeyKind::String),
    };
    let a = strings
        .encode(C::String("a"), Some(C::String("zz")))
        .unwrap();
    let b = strings
        .encode(C::String("aa"), Some(C::String("a")))
        .unwrap();
    assert!(a < b); // A length prefix would violate this ordering for other pairs.
    let a = strings
        .encode(C::String("b"), Some(C::String("a")))
        .unwrap();
    let b = strings
        .encode(C::String("aa"), Some(C::String("a")))
        .unwrap();
    assert!(b < a);
    let large = binary
        .encode(C::Binary(&[0; 2_048]), Some(C::Binary(&[0; 1_024])))
        .unwrap();
    assert_eq!(
        large.as_bytes().len(),
        bdb_keyspace::MAX_ENCODED_PRIMARY_KEY_BYTES
    );
}

#[test]
fn a_single_partition_key_can_span_cells_and_queries_are_clipped() {
    let directory = directory();
    assert_eq!(
        directory.point(&key("tenant-a", "9")).route.cell_id,
        [1; 16]
    );
    assert_eq!(
        directory.point(&key("tenant-a", "10")).route.cell_id,
        [2; 16]
    );
    assert_eq!(
        directory.point(&key("tenant-a", "20")).route.cell_id,
        [3; 16]
    );
    let all = schema().query(C::String("tenant-a"), None).unwrap();
    let forward = directory.query(&all, true);
    assert_eq!(forward.len(), 3);
    assert_eq!(forward[0].span.lower(), all.lower());
    assert_eq!(forward[2].span.upper(), all.upper());
    let reverse = directory.query(&all, false);
    assert_eq!(
        reverse
            .iter()
            .map(|part| part.route.cell_id)
            .collect::<Vec<_>>(),
        vec![[3; 16], [2; 16], [1; 16]]
    );
    let span = schema()
        .query(
            C::String("tenant-a"),
            Some(S::Between(C::Number("11"), C::Number("19"))),
        )
        .unwrap();
    assert_eq!(directory.query(&span, true).len(), 1);
    assert_eq!(directory.query(&span, true)[0].route.cell_id, [2; 16]);
}

#[test]
fn all_query_operators_have_correct_inclusive_and_exclusive_bounds() {
    for (condition, expected) in [
        (S::Eq(C::Number("10")), vec![10]),
        (S::Lt(C::Number("10")), vec![8, 9]),
        (S::Le(C::Number("10")), vec![8, 9, 10]),
        (S::Gt(C::Number("10")), vec![11, 12]),
        (S::Ge(C::Number("10")), vec![10, 11, 12]),
        (S::Between(C::Number("9"), C::Number("11")), vec![9, 10, 11]),
    ] {
        let span = schema()
            .query(C::String("tenant-a"), Some(condition))
            .unwrap();
        let actual: Vec<_> = (8..=12)
            .filter(|value| span.contains(&key("tenant-a", &value.to_string())))
            .collect();
        assert_eq!(actual, expected);
        assert!(!span.contains(&key("tenant-b", "10")));
    }
    assert_eq!(
        schema().query(
            C::String("p"),
            Some(S::Between(C::Number("2"), C::Number("1")))
        ),
        Err(Error::Range)
    );
    assert_eq!(
        schema().query(C::String("p"), Some(S::BeginsWith(C::Number("1")))),
        Err(Error::PrefixType)
    );
}

#[test]
fn begins_with_handles_binary_zero_and_ff_without_a_fake_terminator() {
    let schema = KeySchema {
        partition: KeyKind::String,
        sort: Some(KeyKind::Binary),
    };
    let prefix = b"\0\xff";
    let span = schema
        .query(C::String("p"), Some(S::BeginsWith(C::Binary(prefix))))
        .unwrap();
    for value in [b"\0\xff".as_slice(), b"\0\xff\0", b"\0\xff\xff"] {
        assert!(
            span.contains(
                &schema
                    .encode(C::String("p"), Some(C::Binary(value)))
                    .unwrap()
            )
        );
    }
    assert!(
        !span.contains(
            &schema
                .encode(C::String("p"), Some(C::Binary(b"\x01")))
                .unwrap()
        )
    );
}

#[test]
fn index_duplicate_keys_span_cells_without_losing_ties_on_resume() {
    let index = KeySchema {
        partition: KeyKind::String,
        sort: Some(KeyKind::Number),
    };
    let make = |base| {
        index
            .encode_index(C::String("status"), Some(C::Number("1")), &key("p", base))
            .unwrap()
    };
    let first = make("1");
    let middle = make("2");
    let last = make("3");
    assert!(first < middle && middle < last);
    let directory = Directory::initial(vec![middle.clone()], vec![[1; 16], [2; 16]]).unwrap();
    let query = index
        .query(C::String("status"), Some(S::Eq(C::Number("1"))))
        .unwrap();
    assert!(query.contains(&first) && query.contains(&middle) && query.contains(&last));
    assert_eq!(directory.query(&query, true).len(), 2);
    let resumed = query.resume(&first, true).unwrap();
    assert!(!resumed.contains(&first));
    assert!(resumed.contains(&middle) && resumed.contains(&last));
    let reverse = query.resume(&last, false).unwrap();
    assert!(reverse.contains(&first) && reverse.contains(&middle));
    assert!(!reverse.contains(&last));
}

#[test]
fn invalid_directories_and_deserialized_bounds_fail_closed() {
    assert_eq!(
        Directory::initial(vec![], vec![[1; 16]]),
        Err(Error::Directory)
    );
    assert_eq!(
        Directory::initial(vec![key("p", "1")], vec![[1; 16], [1; 16]]),
        Err(Error::Directory)
    );
    assert!(
        Directory::initial(
            vec![key("p", "2"), key("p", "1")],
            vec![[1; 16], [2; 16], [3; 16]]
        )
        .is_err()
    );
    assert!(serde_json::from_str::<RangeKey>("[]").is_err());
    assert!(serde_json::from_str::<KeySpan>(r#"{"lower":[2],"upper":[1]}"#).is_err());
    let mut ranges = directory().ranges().to_vec();
    ranges[1].span =
        KeySpan::new(Some(key("tenant-a", "11")), Some(key("tenant-a", "20"))).unwrap();
    assert_eq!(Directory::new(1, ranges), Err(Error::Directory));
}

#[test]
fn split_cas_and_api_key_resume_survive_topology_changes_both_directions() {
    let directory = directory();
    let old_route = directory.point(&key("tenant-a", "15")).route;
    let split = directory
        .split(1, old_route, key("tenant-a", "15"), [[4; 16], [5; 16]])
        .unwrap();
    let updated = directory.replace(&split).unwrap();
    assert_eq!(updated.ranges().len(), 4);
    assert_eq!(updated.point(&key("tenant-a", "14")).route.cell_id, [4; 16]);
    assert_eq!(updated.point(&key("tenant-a", "15")).route.cell_id, [5; 16]);
    assert_eq!(updated.replace(&split), Err(Error::StaleRoute));
    let query = schema().query(C::String("tenant-a"), None).unwrap();
    let resume = query.resume(&key("tenant-a", "14"), true).unwrap();
    let requests = updated.query(&resume, true);
    for value in 8..=23 {
        let key = key("tenant-a", &value.to_string());
        assert_eq!(
            requests
                .iter()
                .filter(|part| part.span.contains(&key))
                .count(),
            usize::from(value > 14)
        );
    }
    let resume = query.resume(&key("tenant-a", "16"), false).unwrap();
    let requests = updated.query(&resume, false);
    for value in 8..=23 {
        let key = key("tenant-a", &value.to_string());
        assert_eq!(
            requests
                .iter()
                .filter(|part| part.span.contains(&key))
                .count(),
            usize::from(value < 16)
        );
    }
    assert!(
        directory
            .split(1, old_route, key("tenant-a", "10"), [[4; 16], [5; 16]])
            .is_err()
    );
    assert!(
        directory
            .split(1, old_route, key("tenant-a", "15"), [[3; 16], [5; 16]])
            .is_err()
    );
}
