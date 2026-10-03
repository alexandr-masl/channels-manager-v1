use super::{ConfigError, ConnectionString};
use std::{
    net::Ipv6Addr,
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};
use url::Url;

pub(super) struct Reader<F>(pub F);

impl<F: Fn(&'static str) -> Result<Option<String>, ConfigError>> Reader<F> {
    pub fn boolean(&self, key: &'static str, default: bool) -> Result<bool, ConfigError> {
        match self
            .text(key, Some(if default { "true" } else { "false" }))?
            .as_str()
        {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(ConfigError {
                setting: key,
                reason: "must be true or false",
            }),
        }
    }
    pub fn text(&self, key: &'static str, default: Option<&str>) -> Result<String, ConfigError> {
        let value = (self.0)(key)?
            .or_else(|| default.map(str::to_owned))
            .ok_or(ConfigError {
                setting: key,
                reason: "is required",
            })?;
        let value = value.trim();
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(ConfigError {
                setting: key,
                reason: "must be nonempty and contain no control characters",
            });
        }
        Ok(value.to_owned())
    }

    pub fn positive(&self, key: &'static str, default: u32) -> Result<NonZeroU32, ConfigError> {
        let value = self.text(key, Some(&default.to_string()))?;
        if !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ConfigError {
                setting: key,
                reason: "must be a positive integer",
            });
        }
        value.parse::<NonZeroU32>().map_err(|_| ConfigError {
            setting: key,
            reason: "must be an integer between 1 and 4294967295",
        })
    }

    pub fn prefix(&self, key: &'static str, default: &str) -> Result<String, ConfigError> {
        let value = (self.0)(key)?.unwrap_or_else(|| default.to_owned());
        if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
            return Err(ConfigError {
                setting: key,
                reason: "must be nonempty with no surrounding whitespace or control characters",
            });
        }
        Ok(value)
    }

    pub fn port(&self, key: &'static str, default: u32) -> Result<NonZeroU16, ConfigError> {
        let value = self.positive(key, default)?.get();
        u16::try_from(value)
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or(ConfigError {
                setting: key,
                reason: "must be an integer between 1 and 65535",
            })
    }

    pub fn duration(&self, key: &'static str, default: u32) -> Result<Duration, ConfigError> {
        let value = self.positive(key, default)?.get();
        // AMQP retry queue TTL uses a signed 32-bit integer; use the same safe
        // upper bound for other millisecond settings.
        if value > i32::MAX as u32 {
            return Err(ConfigError {
                setting: key,
                reason: "must be milliseconds between 1 and 2147483647",
            });
        }
        Ok(Duration::from_millis(u64::from(value)))
    }

    pub fn ratio(&self, key: &'static str, default: f64) -> Result<f64, ConfigError> {
        let parsed = self
            .text(key, Some(&default.to_string()))?
            .parse::<f64>()
            .ok();
        parsed
            .filter(|v| v.is_finite() && (0.0..=1.0).contains(v))
            .ok_or(ConfigError {
                setting: key,
                reason: "must be a finite number between 0 and 1",
            })
    }

    pub fn queue(&self, key: &'static str, default: &str) -> Result<String, ConfigError> {
        // The TypeScript output queue override treats blank as the default.
        let raw = (self.0)(key)?;
        let value = raw
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or(default);
        if value.len() > 255 || value.starts_with("amq.") || value.chars().any(char::is_control) {
            return Err(ConfigError {
                setting: key,
                reason: "must be a non-reserved AMQP queue name of at most 255 bytes",
            });
        }
        Ok(value.to_owned())
    }

    pub fn host(&self, key: &'static str) -> Result<String, ConfigError> {
        let host = self.text(key, None)?;
        let unbracketed = host
            .strip_prefix('[')
            .and_then(|v| v.strip_suffix(']'))
            .unwrap_or(&host);
        if let Ok(ipv6) = unbracketed.parse::<Ipv6Addr>() {
            return Ok(ipv6.to_string());
        }
        let dns_or_ipv4 = host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'));
        if !dns_or_ipv4 {
            return Err(ConfigError {
                setting: key,
                reason: "must be a hostname or IP address; configure the port separately",
            });
        }
        Ok(host)
    }

    pub fn uri(&self, key: &'static str, mongo: bool) -> Result<ConnectionString, ConfigError> {
        let value = self.text(key, None)?;
        let valid = !value.chars().any(char::is_whitespace)
            && !value.contains('#')
            && if mongo {
                valid_mongo_uri(&value)
            } else {
                Url::parse(&value).is_ok_and(|url| {
                    matches!(url.scheme(), "amqp" | "amqps")
                        && url.host_str().is_some()
                        && url.port() != Some(0)
                })
            };
        if !valid {
            return Err(ConfigError {
                setting: key,
                reason: if mongo {
                    "must be a mongodb:// or mongodb+srv:// URI with valid hosts"
                } else {
                    "must be an amqp:// or amqps:// URI with a valid host"
                },
            });
        }
        Ok(ConnectionString(value))
    }
}

// Validate only structure. The Mongo driver owns full option/topology validation.
fn valid_mongo_uri(value: &str) -> bool {
    let Some((scheme, rest)) = value.split_once("://") else {
        return false;
    };
    if !matches!(scheme, "mongodb" | "mongodb+srv") {
        return false;
    }
    let authority = rest.split(['/', '?']).next().unwrap_or("");
    let hosts = authority.rsplit('@').next().unwrap_or("");
    if hosts.is_empty() {
        return false;
    }
    if scheme == "mongodb+srv" && (hosts.contains(',') || hosts.contains(':')) {
        return false;
    }
    hosts.split(',').all(|host| {
        !host.is_empty()
            && Url::parse(&format!("http://{host}")).is_ok_and(|url| {
                url.host_str().is_some()
                    && url.port() != Some(0)
                    && url.username().is_empty()
                    && url.password().is_none()
            })
    })
}
