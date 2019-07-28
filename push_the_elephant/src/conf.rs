
pub mod configuration {
    use std::error;
    use std::time::Duration;
    use std::fs;
    use std::boxed::Box;
    use std::fmt;
    use yaml_rust::YamlLoader;
    use yaml_rust::yaml::Yaml;
    use yaml_rust::yaml::Hash;

    #[derive(Debug)]
    pub struct YamlConfigurationError {
        yaml: Yaml
    }

    impl  error::Error for YamlConfigurationError {
        fn source(&self) -> Option<&(dyn error::Error + 'static)> {
            None
        }
    }
    
    impl  fmt::Display for YamlConfigurationError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "invalid configuration {:?}", self.yaml)
        }
    }

    #[derive(Debug)]
    pub struct PushTheElephantConfiguration {
        pub pgurl: Option<String>,
        pub table_name: Option<String>,
        pub column_name: Option<String>,
        pub channel: Option<String>,
        pub topic_name: Option<String>,
        pub buffer_size: Option<usize>,
        pub kafka_brokers: Option<Vec<String>>,
        pub notify_timeout: Option<Duration>,
        pub notify_timeout_total: Option<Duration>
    }

    impl  PushTheElephantConfiguration {
        fn get_str_from_yaml(key: &str, data: &Hash) -> Option<String> {
            return match data.get(&Yaml::String(String::from(key))) {
                Some(yaml_result) => Some(yaml_result.as_str()?.to_string()),
                _ => None
            };
        }

        fn get_u64_from_yaml(key: &str, data: &Hash) -> Option<u64> {
            return match data.get(&Yaml::String(String::from(key))) {
                Some(yaml_result) => yaml_result.as_i64().map(|x| x as u64),
                _ => None
            };
        }
        
        fn get_vec_string_from_yaml(key: &str, data: &Hash) -> Option<Vec<String>> {
            Some(data.get(&Yaml::String(String::from(key)))?.as_vec()?.iter().map(|e| e.as_str().unwrap().to_string()).collect())
        }

        fn create_configuration_from_yaml(yaml_conf: &Yaml) -> Result<PushTheElephantConfiguration, YamlConfigurationError> {
            let configuration = match yaml_conf.as_hash() {
                Some(conf) => conf,
                _ => {
                    return Err(YamlConfigurationError{yaml: yaml_conf.clone()});
                }
            };
            Ok(PushTheElephantConfiguration{
                pgurl: Self::get_str_from_yaml("pgurl", configuration),
                table_name: Self::get_str_from_yaml("table_name", configuration),
                column_name: Self::get_str_from_yaml("column_name", configuration),
                channel: Self::get_str_from_yaml("channel", configuration),
                topic_name: Self::get_str_from_yaml("topic_name", configuration),
                buffer_size: Self::get_u64_from_yaml("buffer_size", configuration).map(|x| x as usize),
                kafka_brokers: Self::get_vec_string_from_yaml("kafka_brokers", configuration),
                notify_timeout: Self::get_u64_from_yaml("notify_timeout", configuration).map(|x| Duration::from_millis(x)),
                notify_timeout_total: Self::get_u64_from_yaml("notify_timeout_total", configuration).map(|x| Duration::from_millis(x)),
            })
        }

        pub fn create_from_yaml_filename(filename: &str) -> Result<Vec<PushTheElephantConfiguration>, Box<error::Error>> {
            let contents = fs::read_to_string(filename)?;
            let yaml_contents = YamlLoader::load_from_str(&contents)?;
            Self::create_from_yaml(yaml_contents)
        }
        
        pub fn create_from_yaml_string(yaml_string: &str) -> Result<Vec<PushTheElephantConfiguration>, Box<error::Error>> {
            Self::create_from_yaml(YamlLoader::load_from_str(yaml_string)?)
        }

        pub fn create_from_yaml(yaml_contents: Vec<Yaml>) -> Result<Vec<PushTheElephantConfiguration>, Box<error::Error>> {
            let hash_configuration = match yaml_contents[0].as_hash() {
                Some(hash_conf) => hash_conf,
                _ => {
                    return Ok(Vec::new());
                }
            };
            let configuration_array = match hash_configuration.get(&Yaml::String("configurations".to_string())) {
                Some(conf_entry) => match conf_entry.as_vec() {
                    Some(conf_array) => conf_array,
                    _ => {
                        return Ok(Vec::new());
                    }
                }
                _ => {
                    return Ok(Vec::new());
                }
            };
            let yaml_conf_array = configuration_array.iter().map(Self::create_configuration_from_yaml);
            let mut result : Vec<PushTheElephantConfiguration> = Vec::new();
            for e in yaml_conf_array {
                result.push(match e {
                    Err(err) => {
                        return Err(Box::new(err));
                    },
                    Ok(c) => c
                });
                
            }
            Ok(result)
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;
    use super::configuration;
    #[test]
    fn multiple_configurations() {
        let conf = configuration::PushTheElephantConfiguration::create_from_yaml_string(
            "
configurations:
    - 
      pgurl: a_postgresql_url
      buffer_size: 12345
      notify_timeout: 67890
      kafka_brokers:
          - kafka_broker1
          - kafka_broker2
    - pgurl: another_postgresql_url
      notify_timeout_total: 13579
      channel: a_channel
"
        ).unwrap();
        println!("********************** {:?}", conf);
        assert_eq!(2, conf.len());
        let conf1 = &conf[0];
        let conf2 = &conf[1];
        assert_matches!(&conf1.pgurl, Some(x) => {
            assert_eq!("a_postgresql_url", x);
        });
        assert_matches!(&conf1.buffer_size, Some(x) => {
            assert_eq!(12345 as usize, *x);
        });
        assert_matches!(&conf1.notify_timeout, Some(x) => {
            assert_eq!(Duration::from_millis(67890), *x);
        });
        assert_matches!(&conf1.kafka_brokers, Some(x) => {
            assert_eq!(vec!["kafka_broker1".to_string(), "kafka_broker2".to_string()], *x);
        });
        assert_matches!(&conf1.notify_timeout_total, None);
        assert_matches!(&conf2.pgurl, Some(x) => {
            assert_eq!("another_postgresql_url", x);
        });
        assert_matches!(&conf2.notify_timeout_total, Some(x) => {
            assert_eq!(Duration::from_millis(13579), *x);
        });
        assert_matches!(&conf2.channel, Some(x) => {
            assert_eq!("a_channel", x);
        });

    }
}
