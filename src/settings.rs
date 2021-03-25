use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "security.protocol")]
pub enum SecurityProtocol {
    #[serde(rename = "PLAINTEXT")]
    Plaintext,
    #[serde(rename = "SASL_SSL")]
    SaslSsl {
        #[serde(rename = "sasl.username")]
        sasl_username: String,
        #[serde(rename = "sasl.password")]
        sasl_password: String,
    },
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaSettings {
    #[serde(rename = "bootstrap.servers")]
    pub bootstrap_servers: String,
    #[serde(rename = "group.id")]
    pub group_id: String,
    #[serde(flatten)]
    pub security_protocol: SecurityProtocol,
    pub input_topics: Vec<String>,
}

impl KafkaSettings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(File::with_name("config/local").required(false))?;
        s.merge(Environment::with_prefix("app"))?;
        s.try_into()
    }

    pub fn config<'a>(
        &self,
        config: &'a mut rdkafka::ClientConfig,
    ) -> &'a mut rdkafka::ClientConfig {
        config.set("bootstrap.servers", &self.bootstrap_servers);
        match &self.security_protocol {
            SecurityProtocol::Plaintext => {
                config.set("security.protocol", "PLAINTEXT");
            }
            SecurityProtocol::SaslSsl {
                sasl_username,
                sasl_password,
            } => {
                config
                    .set("security.protocol", "SASL_SSL")
                    .set("sasl.mechanism", "PLAIN")
                    .set("sasl.username", sasl_username)
                    .set("sasl.password", sasl_password);
            }
        }
        config
    }

    #[cfg(test)]
    pub fn test_settings() -> Self {
        Self {
            bootstrap_servers: "localhost:9094".into(),
            group_id: "test".into(),
            security_protocol: SecurityProtocol::Plaintext,
            input_topics: vec!["test-input".into()],
        }
    }
}
