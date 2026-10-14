//! A phase is an unordered set of requests; phases themselves must occur in order.
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

#[derive(Clone, Debug)]
pub struct Expected {
    pub method: &'static str,
    pub path: &'static str,
    pub params: BTreeMap<String, String>,
    pub signed: bool,
    pub body: Option<Value>,
    pub response: Value,
    pub status: u16,
}
impl Expected {
    pub fn new(
        method: &'static str,
        path: &'static str,
        params: &[(&str, &str)],
        signed: bool,
        response: Value,
    ) -> Self {
        Self {
            method,
            path,
            params: params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            signed,
            body: None,
            response,
            status: 200,
        }
    }
}
#[derive(Default, Debug)]
struct State {
    phases: Vec<Vec<Expected>>,
    journal: Vec<String>,
    errors: Vec<String>,
}
pub struct Script {
    pub endpoint: String,
    state: Arc<Mutex<State>>,
    task: JoinHandle<()>,
}
impl Drop for Script {
    fn drop(&mut self) {
        self.task.abort();
        if std::thread::panicking() {
            eprintln!("HTTP journal: {}", self.journal());
        }
    }
}
impl Script {
    pub async fn start(phases: Vec<Vec<Expected>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State {
            phases,
            ..Default::default()
        }));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let state = shared.clone();
                // Requests are handled serially, but phase matching never assumes GET order.
                let result = tokio::time::timeout(Duration::from_secs(5), async {
                    let mut bytes=Vec::new();
                    loop {
                        let mut part=[0;4096]; let n=socket.read(&mut part).await.unwrap();
                        if n==0 { return Err("incomplete HTTP request".to_owned()); }
                        bytes.extend_from_slice(&part[..n]);
                        if bytes.len()>65536 { return Err("oversized request".into()); }
                        let text=String::from_utf8_lossy(&bytes);
                        if let Some((headers,body))=text.split_once("\r\n\r\n") {
                            let length=headers.lines().find_map(|s|s.to_ascii_lowercase().strip_prefix("content-length: ").and_then(|n|n.parse::<usize>().ok())).unwrap_or(0);
                            if body.len()>=length { break; }
                        }
                    }
                    let text=String::from_utf8(bytes).map_err(|_|"invalid HTTP bytes".to_owned())?;
                    let checked = validate(&mut state.lock().unwrap(), &text);
                    let (status,response) = match checked {
                        Ok(v)=>v,
                        Err(error)=> {state.lock().unwrap().errors.push(error);(500,serde_json::json!({"code":999}))}
                    };
                    let body=response.to_string();
                    socket.write_all(format!("HTTP/1.1 {status} Scripted\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.map_err(|_|"HTTP write failed".to_owned())?;
                    Ok(())
                }).await;
                if !matches!(result, Ok(Ok(()))) {
                    shared
                        .lock()
                        .unwrap()
                        .errors
                        .push(format!("HTTP transport: {result:?}"));
                }
            }
        });
        Self {
            endpoint,
            state,
            task,
        }
    }
    pub fn assert_complete(&self) {
        let state = self.state.lock().unwrap();
        assert!(
            state.errors.is_empty(),
            "HTTP errors: {:?}; journal: {:?}",
            state.errors,
            state.journal
        );
        assert!(
            state.phases.iter().all(Vec::is_empty),
            "unused HTTP responses: {:?}; journal: {:?}",
            state.phases,
            state.journal
        );
    }
    pub fn journal(&self) -> String {
        format!("{:?}", self.state.lock().unwrap().journal)
    }
}
fn validate(state: &mut State, text: &str) -> Result<(u16, Value), String> {
    let (headers, body) = text
        .split_once("\r\n\r\n")
        .ok_or("missing HTTP delimiter")?;
    let mut words = headers
        .lines()
        .next()
        .ok_or("missing request")?
        .split_whitespace();
    let method = words.next().ok_or("missing method")?;
    let route = words.next().ok_or("missing route")?;
    let url = url::Url::parse(&format!("http://127.0.0.1{route}")).map_err(|_| "invalid route")?;
    let path = url.path();
    let mut params: BTreeMap<String, String> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    if params.len() != url.query_pairs().count() {
        return Err("duplicate query parameter".into());
    }
    state.journal.push(format!(
        "{method} {}",
        if path.starts_with("/bot") {
            "/bot[redacted]/sendMessage"
        } else {
            path
        }
    ));
    let phase = state
        .phases
        .iter_mut()
        .find(|p| !p.is_empty())
        .ok_or("unexpected extra HTTP request")?;
    let index = phase
        .iter()
        .position(|e| e.method == method && e.path == path)
        .ok_or_else(|| format!("unexpected method/path or phase: {method} {path}"))?;
    let e = &phase[index];
    if e.signed {
        let key = headers.lines().find_map(|s| {
            s.split_once(':')
                .filter(|(k, _)| k.eq_ignore_ascii_case("x-bx-apikey"))
                .map(|(_, v)| v.trim())
        });
        if key != Some("fixture-key") {
            return Err("API key mismatch".into());
        }
        let signature = params.remove("signature").ok_or("missing signature")?;
        let raw = url
            .query()
            .ok_or("missing query")?
            .rsplit_once("&signature=")
            .ok_or("signature must be last")?
            .0;
        let mut mac = Hmac::<Sha256>::new_from_slice(b"fixture-secret").unwrap();
        mac.update(raw.as_bytes());
        if signature != format!("{:x}", mac.finalize().into_bytes()) {
            return Err("invalid HMAC".into());
        }
        let time = params
            .remove("timestamp")
            .ok_or("missing timestamp")?
            .parse::<i64>()
            .map_err(|_| "invalid timestamp")?;
        if (mongodb::bson::DateTime::now().timestamp_millis() - time).abs() > 5000 {
            return Err("timestamp outside receive window".into());
        }
        if params.remove("recvWindow").as_deref() != Some("5000") {
            return Err("receive window mismatch".into());
        }
    }
    if params != e.params {
        return Err(format!("parameter mismatch for {path}: {params:?}"));
    }
    if let Some(expected) = &e.body {
        if serde_json::from_str::<Value>(body).map_err(|_| "invalid JSON body")? != *expected {
            return Err("request JSON mismatch".into());
        }
    } else if !body.is_empty() {
        return Err("unexpected request body".into());
    }
    let e = phase.remove(index);
    Ok((e.status, e.response))
}

#[test]
fn strict_script_rejects_extra_parameters_and_wrong_methods() {
    let expected = Expected::new(
        "GET",
        "/quote",
        &[("symbol", "ADA-USDT")],
        false,
        serde_json::json!({}),
    );
    for request in [
        "POST /quote?symbol=ADA-USDT HTTP/1.1\r\n\r\n",
        "GET /quote?symbol=ADA-USDT&extra=1 HTTP/1.1\r\n\r\n",
    ] {
        let mut state = State {
            phases: vec![vec![expected.clone()]],
            ..Default::default()
        };
        assert!(validate(&mut state, request).is_err());
        assert_eq!(state.phases[0].len(), 1);
    }
}

#[test]
fn signed_script_checks_signature_key_timestamp_and_parameters() {
    let expected = Expected::new(
        "GET",
        "/account",
        &[("symbol", "ADA-USDT")],
        true,
        serde_json::json!({}),
    );
    let timestamp = mongodb::bson::DateTime::now().timestamp_millis();
    let request = |timestamp: i64, key: &str, secret: &str, symbol: &str| {
        let query = format!("symbol={symbol}&recvWindow=5000&timestamp={timestamp}");
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(query.as_bytes());
        format!(
            "GET /account?{query}&signature={:x} HTTP/1.1\r\nX-BX-APIKEY: {key}\r\n\r\n",
            mac.finalize().into_bytes()
        )
    };
    for (text, valid) in [
        (
            request(timestamp, "fixture-key", "fixture-secret", "ADA-USDT"),
            true,
        ),
        (
            request(timestamp, "wrong-key", "fixture-secret", "ADA-USDT"),
            false,
        ),
        (
            request(timestamp, "fixture-key", "wrong-secret", "ADA-USDT"),
            false,
        ),
        (
            request(
                timestamp - 6000,
                "fixture-key",
                "fixture-secret",
                "ADA-USDT",
            ),
            false,
        ),
        (
            request(timestamp, "fixture-key", "fixture-secret", "SOL-USDT"),
            false,
        ),
    ] {
        let mut state = State {
            phases: vec![vec![expected.clone()]],
            ..Default::default()
        };
        assert_eq!(validate(&mut state, &text).is_ok(), valid);
        assert_eq!(state.phases[0].is_empty(), valid);
    }
}

#[test]
fn unordered_reads_must_finish_before_mutation_and_extras_fail() {
    let mut state = State {
        phases: vec![
            vec![
                Expected::new("GET", "/a", &[], false, serde_json::json!({})),
                Expected::new("GET", "/b", &[], false, serde_json::json!({})),
            ],
            vec![Expected::new(
                "POST",
                "/change",
                &[],
                false,
                serde_json::json!({}),
            )],
        ],
        ..Default::default()
    };
    assert!(validate(&mut state, "GET /b HTTP/1.1\r\n\r\n").is_ok());
    assert!(validate(&mut state, "POST /change HTTP/1.1\r\n\r\n").is_err());
    assert!(validate(&mut state, "GET /a HTTP/1.1\r\n\r\n").is_ok());
    assert!(validate(&mut state, "POST /change HTTP/1.1\r\n\r\n").is_ok());
    assert!(state.phases.iter().all(Vec::is_empty));
    assert!(validate(&mut state, "GET /a HTTP/1.1\r\n\r\n").is_err());
}
