use std::{
    fmt::{self, Display, Formatter},
    fs::read_to_string,
    io,
};

use clap::Parser;
use toml::Table;

pub type Value = toml::Value;

#[derive(Clone, Debug)]
pub struct Configuration {
    inner: Table,
}

impl Configuration {
    pub fn get_conf<S: AsRef<str>>(&self, a: S) -> Self {
        let inner = self.inner[a.as_ref()].as_table().unwrap().clone();
        Self { inner }
    }

    pub fn get_str<S: AsRef<str>>(&self, a: S) -> &str {
        self.inner[a.as_ref()].as_str().unwrap()
    }

    pub fn get_i64<S: AsRef<str>>(&self, a: S) -> i64 {
        self.inner[a.as_ref()].as_integer().unwrap()
    }

    pub fn get_f64<S: AsRef<str>>(&self, a: S) -> f64 {
        self.inner[a.as_ref()].as_float().unwrap()
    }

    pub fn get_bool<S: AsRef<str>>(&self, a: S) -> bool {
        self.inner[a.as_ref()].as_bool().unwrap()
    }

    pub fn get_u64<S: AsRef<str>>(&self, a: S) -> u64 {
        self.get_i64(a) as u64
    }
}

#[derive(Debug)]
pub enum ConfigError {
    IO(io::Error),
    Toml(toml::de::Error),
    Key(String),
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = String::from("./Configuration.toml"))]
    config: String,
}

pub fn load() -> Result<Configuration, ConfigError> {
    let args = Args::parse();

    let s = match read_to_string(args.config) {
        Ok(x) => x,
        Err(x) => return Err(ConfigError::IO(x)),
    };

    let inner = match s.parse::<Table>() {
        Ok(x) => x,
        Err(x) => return Err(ConfigError::Toml(x)),
    };

    Ok(Configuration { inner })
}
