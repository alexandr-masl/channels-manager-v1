//! Structural decoding only: these values have not been authenticated or authorized.

use std::{fmt, marker::PhantomData};

use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor, value::MapAccessDeserializer},
};

pub(crate) const SERVICE_ID: &str = "channels-manager-v1";
pub(crate) const SIGNING_PREFIX: &[u8] = b"application-authorization/v1\n";

#[derive(Debug)]
pub(crate) struct Envelope {
    pub key_id: String,
    pub payload: String,
    pub signature: String,
}

#[derive(Debug)]
pub(crate) struct Payload {
    pub version: u8,
    pub key_id: String,
    pub authorization_id: String,
    pub authorized_to: String,
    pub services: Vec<String>,
}

// Serde's derived struct decoder also accepts positional arrays. This visitor
// only exposes map decoding, while the derived field decoder enforces required,
// unique, typed fields and rejects unknown fields.
fn deserialize_object<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Object<T>(PhantomData<T>);
    impl<'de, T: Deserialize<'de>> Visitor<'de> for Object<T> {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an authorization JSON object")
        }

        fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
            T::deserialize(MapAccessDeserializer::new(map))
        }
    }
    deserializer.deserialize_map(Object(PhantomData))
}

impl<'de> Deserialize<'de> for Envelope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            key_id: String,
            payload: String,
            signature: String,
        }
        let fields: Fields = deserialize_object(deserializer)?;
        Ok(Self {
            key_id: fields.key_id,
            payload: fields.payload,
            signature: fields.signature,
        })
    }
}

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            version: u8,
            key_id: String,
            authorization_id: String,
            authorized_to: String,
            services: Vec<String>,
        }
        let fields: Fields = deserialize_object(deserializer)?;
        Ok(Self {
            version: fields.version,
            key_id: fields.key_id,
            authorization_id: fields.authorization_id,
            authorized_to: fields.authorized_to,
            services: fields.services,
        })
    }
}
