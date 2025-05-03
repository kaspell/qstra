// Copyright © 2025 Kasperi Apell <apkaspell@gmail.com>
// SPDX-License-Identifier: AGPL-3.0-or-later


use std::fs;
use std::path::{PathBuf};


pub const CONF_FILE: &str = "bdb.conf";


pub struct Config {
        pub listen_local: bool,
        pub listen_network: bool,
        pub inet_addr: String,
        pub sock_addr: String,
        pub db_file: PathBuf,
        pub wal_file: PathBuf,
}


impl Default for Config {
        fn default() -> Self {
                Self {
                        listen_local: true,
                        listen_network: true,
                        inet_addr: "127.0.0.1:1234".into(),
                        sock_addr: "bdb.sock".into(),
                        db_file: PathBuf::from("bdb.db"),
                        wal_file: PathBuf::from("bdb.wal"),
                }
        }
}


impl Config {
        #[must_use]
        pub fn new(conf_file: &str) -> Self {
                let mut cfg = Self::default();

                let conf_contents = fs::read_to_string(conf_file).unwrap_or_else(|_| String::new());
                for line in conf_contents.lines() {
                        if line.trim().is_empty() || line.trim_start().starts_with('#') {
                                continue;
                        }
                        match line.split_once('=') {
                                Some(("DB_FILE", val)) => {
                                        cfg.db_file = PathBuf::from(val);
                                }
                                Some(("WAL_FILE", val)) => {
                                        cfg.wal_file = PathBuf::from(val);
                                }
                                Some(("LISTEN_LOCAL", val)) => {
                                        cfg.listen_local = val.to_lowercase().parse().unwrap_or(false);
                                }
                                Some(("LISTEN_NETWORK", val)) => {
                                        cfg.listen_network = val.to_lowercase().parse().unwrap_or(false);
                                }
                                Some(("INET_ADDRESS", val)) => {
                                        cfg.inet_addr = val.into();
                                }
                                Some(("UNIX_SOCKET", val)) => {
                                        cfg.sock_addr = val.into();
                                }
                                _ => {}
                        }
                }

                if !cfg.listen_local && !cfg.listen_network {
                        panic!("Must listen to at least one channel for connections: local or network");
                }

                cfg
        }
}