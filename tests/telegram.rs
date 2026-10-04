use channels_manager_v1::telegram::{IntakeOutcome, RejectReason, SkipReason, inspect_message};
use serde_json::{Value, json};
const NOW: u64 = 1_791_056_304_000;
fn message() -> Value {
    json!({"message_id":7,"date":NOW/1000,"chat":{"id":-1001596367704i64,"type":"channel"},"text":include_str!("../examples/fixtures/ada-signal.txt").trim_end(),"extra":"ignored"})
}
fn inspect(value: Value) -> IntakeOutcome {
    inspect_message(&serde_json::to_vec(&value).unwrap(), NOW)
}
#[test]
fn decodes_telegram_message_without_parsing_signal_text() {
    let IntakeOutcome::Received(message) = inspect(message()) else {
        panic!("expected valid intake")
    };
    assert_eq!(message.channel_id(), -1001596367704);
    assert_eq!(message.message_id(), 7);
    assert_eq!(message.source_created_at_ms(), NOW);
    assert!(message.text().starts_with("⚜️#ADA/USDT"));
}
#[test]
fn malformed_or_missing_fields_are_rejected() {
    assert!(matches!(
        inspect_message(b"not json", NOW),
        IntakeOutcome::Rejected(RejectReason::InvalidEnvelope)
    ));
    for field in ["message_id", "date", "chat"] {
        let mut value = message();
        value.as_object_mut().unwrap().remove(field);
        assert!(matches!(
            inspect(value),
            IntakeOutcome::Rejected(RejectReason::InvalidEnvelope)
        ));
    }
    for (field, value) in [
        ("message_id", json!(0)),
        ("message_id", json!(9007199254740992u64)),
        ("date", json!(u64::MAX)),
        ("date", json!(-1)),
        ("date", json!("now")),
    ] {
        let mut input = message();
        input[field] = value;
        assert!(matches!(inspect(input), IntakeOutcome::Rejected(_)));
    }
}
#[test]
fn source_time_boundaries_match_existing_contract() {
    for (offset, valid) in [(-600, true), (-601, false), (120, true), (121, false)] {
        let mut input = message();
        input["date"] = json!((NOW / 1000) as i64 + offset);
        assert_eq!(matches!(inspect(input), IntakeOutcome::Received(_)), valid);
    }
}
#[test]
fn non_text_messages_replies_and_other_chat_types_are_skipped() {
    let mut input = message();
    input.as_object_mut().unwrap().remove("text");
    assert!(matches!(
        inspect(input),
        IntakeOutcome::Skipped(SkipReason::NoText)
    ));
    let mut input = message();
    input["text"] = json!(" \n ");
    assert!(matches!(
        inspect(input),
        IntakeOutcome::Skipped(SkipReason::NoText)
    ));
    let mut input = message();
    input["reply_to_message"] = json!({"message_id":1});
    assert!(matches!(
        inspect(input),
        IntakeOutcome::Skipped(SkipReason::Reply)
    ));
    let mut input = message();
    input["chat"]["type"] = json!("private");
    assert!(matches!(
        inspect(input),
        IntakeOutcome::Skipped(SkipReason::NotChannel)
    ));
}
