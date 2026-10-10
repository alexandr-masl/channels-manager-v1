use channels_manager_v1::{
    mongo::{MongoError, MongoErrorKind, MongoRole},
    telegram::{ChannelOutcome, ChannelRepository, ChannelSkip, ChannelUpdateManager, IntakeOutcome, inspect_message},
};
use futures_util::future::BoxFuture;
use mongodb::bson::{DateTime, Document, doc};
use std::sync::Mutex;

const NOW: u64 = 1_791_056_304_000;
#[derive(Default)]
struct Repository {
    channel: Option<Document>, profiles: Vec<Document>, accounts: Vec<Document>, configs: Vec<Document>,
    fail: Option<&'static str>, calls: Mutex<Vec<(&'static str, Vec<i64>)>>,
}
impl Repository {
    fn result<T>(&self, stage: &'static str, ids: Vec<i64>, value: T) -> Result<T, MongoError> {
        self.calls.lock().unwrap().push((stage, ids));
        if self.fail == Some(stage) { Err(MongoError { role: MongoRole::Bot, kind: MongoErrorKind::Unavailable }) } else { Ok(value) }
    }
}
impl ChannelRepository for Repository {
    fn channel(&self, id: i64) -> BoxFuture<'_, Result<Option<Document>, MongoError>> { Box::pin(async move { self.result("channel", vec![id], self.channel.clone()) }) }
    fn profiles(&self, id: i64) -> BoxFuture<'_, Result<Vec<Document>, MongoError>> { Box::pin(async move { self.result("profiles", vec![id], self.profiles.clone()) }) }
    fn accounts(&self, ids: Vec<i64>) -> BoxFuture<'_, Result<Vec<Document>, MongoError>> { Box::pin(async move { self.result("accounts", ids, self.accounts.clone()) }) }
    fn configs(&self, ids: Vec<i64>) -> BoxFuture<'_, Result<Vec<Document>, MongoError>> { Box::pin(async move { self.result("configs", ids, self.configs.clone()) }) }
}
fn repository() -> Repository {
    Repository {
        channel: Some(doc! {"id": -100_i64, "strategy":"advanced"}),
        profiles: vec![doc! {"userId":42.0, "exchangeClients":[
            {"clientId":"a", "provider":"BingX", "connectedChannel":-100.0,"api_key":"secret-key","api_secret":"secret-value","name":"test"},
            {"clientId":"b", "provider":"BingX", "connectedChannel":-100_i64,"api_key":"key-b","api_secret":"secret-b"},
            {"clientId":"other", "provider":"Binance", "connectedChannel":-100_i64},
            {"clientId":"elsewhere", "provider":"BingX", "connectedChannel":-200_i64}
        ]}],
        accounts: vec![doc! {"tg_chat_id":42.0,"auto_trading":true,"valid_till":DateTime::from_millis(NOW as i64 + 1)}],
        configs: vec![doc! {"user":42_i64,"private_channels":[{"id":-200_i64,"own_settings":true},{"id":-100_i64,"own_settings":false,"futures":{"active":true}}]}],
        ..Default::default()
    }
}
async fn run(repo: &Repository, text: &str) -> Result<ChannelOutcome, MongoError> {
    let body = serde_json::to_vec(&serde_json::json!({"message_id":7,"date":NOW/1000,"chat":{"id":-100,"type":"channel"},"text":text})).unwrap();
    let IntakeOutcome::Received(message) = inspect_message(&body, NOW) else { panic!("invalid fixture") };
    ChannelUpdateManager::new(repo).handle_channel_update(message, NOW).await
}
const SIGNAL: &str = "BTCUSDT BREAKOUT SHORT\nENTRY 84550-84650\nTG1 83595\nTG2 81048\nLEVERAGE 5x\nPOSITION SIZE 0.5%\nSL 86347";
#[tokio::test]
async fn builds_context_and_batches_users_without_losing_signal_or_settings() {
    let repo = repository();
    let ChannelOutcome::Ready(context) = run(&repo, SIGNAL).await.unwrap() else { panic!("expected ready") };
    assert_eq!(context.message.channel_id(), -100);
    assert_eq!(context.message.message_id(), 7);
    assert_eq!(context.message.source_created_at_ms(), NOW);
    assert_eq!(context.clients.len(), 2);
    assert_eq!(context.clients[0].client_id, "a");
    assert_eq!(context.clients[0].user_settings.as_ref().unwrap().get_bool("own_settings"), Ok(false));
    assert_eq!(context.clients[0].user_config.as_ref(), repo.configs.first());
    assert_eq!(context.signal.breakout_entry, Some(true));
    assert_eq!(context.signal.position, Some(0.005));
    assert_eq!(context.signal.buy_targets, ["84550", "84650"]);
    assert_eq!(*repo.calls.lock().unwrap(), vec![("channel",vec![-100]),("profiles",vec![-100]),("accounts",vec![42]),("configs",vec![42])]);
}
#[tokio::test]
async fn missing_channel_stops_before_parsing_and_queries() {
    let repo = Repository::default();
    assert!(matches!(run(&repo, "not a signal").await.unwrap(), ChannelOutcome::Skipped(ChannelSkip::UnauthorizedChannel)));
    assert_eq!(repo.calls.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn parser_outcomes_stop_context_queries() {
    let repo = repository();
    assert!(matches!(run(&repo, "hello").await.unwrap(), ChannelOutcome::Skipped(ChannelSkip::NotSignal)));
    assert!(matches!(run(&repo, &SIGNAL.replace("5x", "-5x")).await.unwrap(), ChannelOutcome::Rejected(_)));
    assert_eq!(repo.calls.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn every_database_failure_propagates_for_retry() {
    for stage in ["channel", "profiles", "accounts", "configs"] {
        let mut repo = repository(); repo.fail = Some(stage);
        assert!(run(&repo, SIGNAL).await.is_err(), "{stage}");
    }
}
#[tokio::test]
async fn expired_disabled_missing_and_invalid_accounts_are_ineligible() {
    for account in [
        doc! {"tg_chat_id":42,"auto_trading":true,"valid_till":DateTime::from_millis(NOW as i64)},
        doc! {"tg_chat_id":42,"auto_trading":false,"valid_till":DateTime::from_millis(NOW as i64+1000)},
        doc! {"tg_chat_id":42,"auto_trading":true,"valid_till":"invalid"},
        doc! {"tg_chat_id":99,"auto_trading":true,"valid_till":DateTime::from_millis(NOW as i64+1000)},
    ] {
        let mut repo = repository(); repo.accounts = vec![account];
        assert!(matches!(run(&repo,SIGNAL).await.unwrap(), ChannelOutcome::Skipped(ChannelSkip::NoEligibleAccounts)));
        assert_eq!(repo.calls.lock().unwrap().len(),3);
    }
    let mut repo = repository(); repo.accounts.clear();
    assert!(matches!(run(&repo,SIGNAL).await.unwrap(), ChannelOutcome::Skipped(ChannelSkip::NoEligibleAccounts)));
}
#[tokio::test]
async fn missing_optional_settings_use_channel_context() {
    let mut repo = repository(); repo.configs.clear();
    let ChannelOutcome::Ready(context) = run(&repo,SIGNAL).await.unwrap() else { panic!("expected ready") };
    assert!(context.clients.iter().all(|c| c.user_config.is_none() && c.user_settings.is_none()));
    assert_eq!(context.channel_settings.get_str("strategy"), Ok("advanced"));
}
#[tokio::test]
async fn no_connected_bingx_accounts_skips_account_lookup() {
    let mut repo = repository(); repo.profiles.clear();
    assert!(matches!(run(&repo,SIGNAL).await.unwrap(), ChannelOutcome::Skipped(ChannelSkip::NoConnectedAccounts)));
    assert_eq!(repo.calls.lock().unwrap().len(),2);
}
