
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

    pub struct PushTheElephantConfiguration {
        pgurl: Option<String>,
        table_name: Option<String>,
        column_name: Option<String>,
        channel: Option<String>,
        topic_name: Option<String>,
        buffer_size: Option<usize>,
        kafka_brokers: Option<Vec<String>>,
        notify_timeout: Option<Duration>,
        notify_timeout_total: Option<Duration>
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

        fn create_from_yaml_filename(filename: &str ) -> Result<Vec<PushTheElephantConfiguration>, Box<error::Error>> {
            let contents = fs::read_to_string(filename)?;
            let yaml_contents = YamlLoader::load_from_str(&contents)?;
            Self::create_from_yaml(yaml_contents)
        }

        fn create_from_yaml(yaml_contents: Vec<Yaml>) -> Result<Vec<PushTheElephantConfiguration>, Box<error::Error>> {
            let hash_configuration = match yaml_contents[0].as_hash() {
                Some(hash_conf) => hash_conf,
                _ => {
                    return Ok(Vec::new());
                }
            };
            let configuration_array = match hash_configuration.get(&Yaml::String("configuration".to_string())) {
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
