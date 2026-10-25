//! Round trips, safety and malformed-input handling for every format.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::str::FromStr;

use bigdecimal::BigDecimal;
use gazabridge_bson::bson::{Bson, Regex, doc, oid::ObjectId};
use gazabridge_domain::{Record, Value};
use gazabridge_files::{
    Compression, CsvOptions, ExtraFields, FileError, FileFormat, FileReader, FileWriter,
    JsonFlavor, MalformedPolicy, ReadOptions, WriteOptions,
};

fn sample() -> Vec<Record> {
    let regex = doc! { "_id": 3, "re": Regex { pattern: "^a".into(), options: "i".into() }, "n": Bson::Int32(5) };
    vec![
        Record::from_pairs([
            ("_id", Value::ObjectId([1; 12])),
            ("name", Value::String("غزة, \"quoted\"".into())),
            (
                "price",
                Value::Decimal(BigDecimal::from_str("12.50").unwrap()),
            ),
            (
                "tags",
                Value::Array(vec![Value::String("a".into()), Value::Null]),
            ),
            ("note", Value::Null),
        ]),
        Record::from_pairs([
            ("_id", Value::Int(2)),
            ("name", Value::String("second".into())),
        ]),
        gazabridge_bson::document_to_record(&regex),
    ]
}

fn write(path: &Path, options: WriteOptions, records: &[Record]) -> gazabridge_files::WriteSummary {
    let mut w = FileWriter::create(path, options).unwrap();
    w.write(records).unwrap();
    w.finish().unwrap()
}

fn read_all(path: &Path, options: ReadOptions) -> (Vec<Record>, gazabridge_files::ReadStats) {
    let mut r = FileReader::open(path, options).unwrap();
    let mut out = Vec::new();
    loop {
        let batch = r.next_batch(2).unwrap();
        if batch.is_empty() {
            break;
        }
        out.extend(batch.into_iter().map(|s| s.record));
    }
    (out, r.finish().unwrap())
}

fn bson_of(records: &[Record]) -> Vec<gazabridge_bson::bson::Document> {
    records
        .iter()
        .map(|r| gazabridge_bson::record_to_document(r).unwrap())
        .collect()
}

#[test]
fn every_readable_format_round_trips_with_and_without_gzip() {
    let dir = tempfile::tempdir().unwrap();
    let records = sample();
    let cases = [
        (FileFormat::Json, JsonFlavor::Typed),
        (FileFormat::Json, JsonFlavor::MongoRelaxed),
        (FileFormat::Jsonl, JsonFlavor::Typed),
        (FileFormat::Jsonl, JsonFlavor::MongoCanonical),
        (FileFormat::Bson, JsonFlavor::Typed),
    ];
    for (format, flavor) in cases {
        for (compression, pretty) in [(Compression::None, false), (Compression::Gzip, true)] {
            let path = dir.path().join(format!(
                "{format:?}-{flavor:?}-{compression:?}.{}",
                format.extension()
            ));
            let mut opts = WriteOptions::new(format);
            opts.json_flavor = flavor;
            opts.compression = compression;
            opts.pretty = pretty;
            let summary = write(&path, opts, &records);
            assert_eq!(summary.records, 3);
            assert_eq!(summary.sha256.len(), 64);
            assert_eq!(summary.bytes, std::fs::metadata(&path).unwrap().len());

            let (back, stats) = read_all(
                &path,
                ReadOptions {
                    format,
                    json_flavor: flavor,
                    compression,
                    malformed: MalformedPolicy::Fail,
                },
            );
            assert_eq!(stats.records, 3, "{path:?}");
            if format == FileFormat::Bson || flavor == JsonFlavor::MongoCanonical {
                // Exact BSON types, including Int32/Int64 and the regex.
                assert_eq!(bson_of(&back), bson_of(&records), "{path:?}");
            } else {
                // Typed and relaxed JSON keep every canonical value (the regex
                // too) but not BSON's Int32/Int64 distinction, like mongoexport.
                let values = |rs: &[Record]| {
                    rs.iter()
                        .map(|r| {
                            r.fields
                                .iter()
                                .map(|(k, v)| (k.clone(), v.value.clone()))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>()
                };
                assert_eq!(values(&back), values(&records), "{path:?}");
            }
        }
    }
}

#[test]
fn mongo_relaxed_json_matches_mongoexport() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.jsonl");
    let mut opts = WriteOptions::new(FileFormat::Jsonl);
    opts.json_flavor = JsonFlavor::MongoRelaxed;
    write(&path, opts, &sample()[..1]);
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        text.contains(r#""_id":{"$oid":"010101010101010101010101"}"#),
        "{text}"
    );
    assert!(
        text.contains(r#""price":{"$numberDecimal":"12.50"}"#),
        "{text}"
    );
    // A line mongoexport writes reads back.
    let oid = ObjectId::new();
    std::fs::write(
        &path,
        format!("{{\"_id\":{{\"$oid\":\"{}\"}},\"at\":{{\"$date\":\"2026-10-25T00:00:00Z\"}},\"n\":1}}\n", oid.to_hex()),
    )
    .unwrap();
    let (back, _) = read_all(
        &path,
        ReadOptions {
            format: FileFormat::Jsonl,
            json_flavor: JsonFlavor::MongoRelaxed,
            compression: Compression::None,
            malformed: MalformedPolicy::Fail,
        },
    );
    assert!(matches!(back[0].get("at"), Value::UtcDateTime(_)));
    assert_eq!(
        gazabridge_bson::record_to_document(&back[0])
            .unwrap()
            .get("n"),
        Some(&Bson::Int32(1))
    );
}

#[test]
fn existing_files_are_never_replaced_without_permission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keep.json");
    std::fs::write(&path, "precious").unwrap();
    let err = FileWriter::create(&path, WriteOptions::new(FileFormat::Json)).unwrap_err();
    assert!(matches!(err, FileError::Exists(_)));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "precious");

    // Created meanwhile: finish refuses too.
    let late = dir.path().join("late.json");
    let mut w = FileWriter::create(&late, WriteOptions::new(FileFormat::Json)).unwrap();
    w.write(&sample()).unwrap();
    std::fs::write(&late, "someone else").unwrap();
    assert!(matches!(w.finish().unwrap_err(), FileError::Exists(_)));
    assert_eq!(std::fs::read_to_string(&late).unwrap(), "someone else");

    let mut opts = WriteOptions::new(FileFormat::Json);
    opts.overwrite = true;
    write(&path, opts, &sample());
    assert!(std::fs::read_to_string(&path).unwrap().starts_with('['));
}

#[test]
fn unfinished_exports_leave_nothing_behind() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.jsonl");
    {
        let mut w = FileWriter::create(&path, WriteOptions::new(FileFormat::Jsonl)).unwrap();
        w.write(&sample()).unwrap();
        // Dropped without finish (cancelled or failed).
    }
    assert!(!path.exists());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn csv_is_written_with_explicit_columns_and_never_drops_fields_silently() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.csv");
    let mut opts = WriteOptions::new(FileFormat::Csv);
    opts.csv = CsvOptions {
        columns: vec![
            "_id".into(),
            "name".into(),
            "price".into(),
            "tags".into(),
            "note".into(),
        ],
        null_text: "NULL".into(),
        ..CsvOptions::default()
    };
    let records = &sample()[..2];
    let mut w = FileWriter::create(&path, opts.clone()).unwrap();
    w.write(records).unwrap();
    w.finish().unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "_id,name,price,tags,note");
    assert_eq!(
        lines[1],
        r#"010101010101010101010101,"غزة, ""quoted""",12.50,"[""a"",null]",NULL"#
    );
    assert_eq!(lines[2], "2,second,,,");

    // A field outside the columns stops the export...
    let path2 = dir.path().join("y.csv");
    opts.csv.columns = vec!["_id".into()];
    let mut w = FileWriter::create(&path2, opts.clone()).unwrap();
    assert!(matches!(w.write(records).unwrap_err(), FileError::ExtraField(f) if f == "name"));
    drop(w);
    assert!(!path2.exists());
    // ...unless the user chose to leave such fields out, and then it says which.
    opts.csv.extra_fields = ExtraFields::Ignore;
    let mut w = FileWriter::create(&path2, opts).unwrap();
    w.write(records).unwrap();
    let s = w.finish().unwrap();
    assert_eq!(
        s.ignored_fields.into_iter().collect::<Vec<_>>(),
        ["name", "note", "price", "tags"]
    );
}

#[test]
fn malformed_jsonl_lines_fail_skip_or_quarantine_with_line_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.jsonl");
    std::fs::write(&path, "{\"a\":1}\n\n{broken\r\n[1]\n{\"a\":2}\n").unwrap();
    let opts = |malformed| ReadOptions {
        format: FileFormat::Jsonl,
        json_flavor: JsonFlavor::Typed,
        compression: Compression::None,
        malformed,
    };

    let mut r = FileReader::open(&path, opts(MalformedPolicy::Fail)).unwrap();
    match r.next_batch(10).unwrap_err() {
        FileError::Malformed { kind, location, .. } => assert_eq!((kind, location), ("line", 3)),
        other => panic!("{other}"),
    }

    let (records, stats) = read_all(&path, opts(MalformedPolicy::Skip));
    assert_eq!(records.len(), 2);
    assert_eq!(stats.skipped, 2);
    assert_eq!(
        stats.issues.iter().map(|i| i.location).collect::<Vec<_>>(),
        [3, 4]
    );

    let q = dir.path().join("bad.jsonl");
    let (records, stats) = read_all(&path, opts(MalformedPolicy::Quarantine { path: q.clone() }));
    assert_eq!(records.len(), 2);
    assert_eq!(stats.quarantined, 2);
    assert_eq!(std::fs::read_to_string(&q).unwrap(), "{broken\n[1]\n");
    // The quarantine file is never overwritten.
    let mut r = FileReader::open(&path, opts(MalformedPolicy::Quarantine { path: q })).unwrap();
    assert!(matches!(
        r.next_batch(10).unwrap_err(),
        FileError::Exists(_)
    ));
}

#[test]
fn json_arrays_and_bson_report_record_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let json = dir.path().join("a.json");
    std::fs::write(&json, r#"[{"a": 1}, {"a": {"$oid": "nothex"}}, {"a": 3}]"#).unwrap();
    let opts = |format, malformed| ReadOptions {
        format,
        json_flavor: JsonFlavor::Typed,
        compression: Compression::None,
        malformed,
    };
    let (records, stats) = read_all(&json, opts(FileFormat::Json, MalformedPolicy::Skip));
    assert_eq!(records.len(), 2);
    assert_eq!(stats.issues[0].location, 2);

    std::fs::write(&json, r#"[{"a": 1}, {"a": "#).unwrap();
    let mut r = FileReader::open(&json, opts(FileFormat::Json, MalformedPolicy::Skip)).unwrap();
    assert!(
        matches!(r.next_batch(10).unwrap_err(), FileError::Malformed { .. }),
        "structural damage always stops"
    );

    // Truncated BSON stops with the record number.
    let bson = dir.path().join("a.bson");
    let mut bytes = gazabridge_bson::bson::to_vec(&doc! { "a": 1 }).unwrap();
    let second = gazabridge_bson::bson::to_vec(&doc! { "b": "long enough" }).unwrap();
    bytes.extend_from_slice(&second[..second.len() - 3]);
    std::fs::write(&bson, bytes).unwrap();
    let mut r = FileReader::open(&bson, opts(FileFormat::Bson, MalformedPolicy::Fail)).unwrap();
    assert_eq!(r.next_batch(1).unwrap().len(), 1);
    match r.next_batch(1).unwrap_err() {
        FileError::Malformed { location, .. } => assert_eq!(location, 2),
        other => panic!("{other}"),
    }
}

#[test]
fn empty_inputs_and_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.json");
    let s = write(&path, WriteOptions::new(FileFormat::Json), &[]);
    assert_eq!(s.records, 0);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "[]\n");
    let (records, _) = read_all(
        &path,
        ReadOptions {
            format: FileFormat::Json,
            json_flavor: JsonFlavor::Typed,
            compression: Compression::None,
            malformed: MalformedPolicy::Fail,
        },
    );
    assert!(records.is_empty());
    assert!(
        FileReader::open(
            &path,
            ReadOptions {
                format: FileFormat::Csv,
                json_flavor: JsonFlavor::Typed,
                compression: Compression::None,
                malformed: MalformedPolicy::Fail
            }
        )
        .is_err()
    );
}
