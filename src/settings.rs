use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SecurityProtocol {
    Plaintext,
    SaslSsl {
        sasl_username: String,
        sasl_password: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaSettings {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub security_protocol: SecurityProtocol,
    pub input_topics: Vec<String>,
}

impl KafkaSettings {
    pub fn new(
        bootstrap_servers: String,
        group_id: String,
        security_protocol: SecurityProtocol,
        input_topics: Vec<String>,
    ) -> Self {
        Self {
            bootstrap_servers,
            group_id,
            security_protocol,
            input_topics,
        }
    }

    pub(crate) fn config<'a>(
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
}
