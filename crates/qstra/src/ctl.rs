// Copyright © 2025 Kasperi Apell <apkaspell@gmail.com>
// SPDX-License-Identifier: AGPL-3.0-or-later
//
//
//! Define the main control structure and its bridges to the filesystem.


use std::fs;
use std::io::{self, Read, Write};

use qstra_prob::bf::{BloomFilterStructure};
use qstra_stor::srl::{self, Deserializable, Serializable};

use crate::cfg;
use crate::db;
use crate::reg;
use crate::wal;


pub struct Ctl {
        pub curr_db: usize,
        pub db_registry: reg::Registry<db::Database>,
        cfg: cfg::Config,
        wal: wal::WriteAheadLog,
}


impl Ctl {
        pub fn config(&self) -> &cfg::Config {
                &self.cfg
        }

        pub fn wa_log(&mut self) -> &mut wal::WriteAheadLog {
                &mut self.wal
        }

        pub fn new_blank(conf: cfg::Config) -> io::Result<Self> {
                let wal = wal::WriteAheadLog::new(&conf.wal_file)?;
                Ok(Self {
                        curr_db: 0,
                        db_registry: reg::Registry::<db::Database>::new_blank(),
                        cfg: conf,
                        wal,
                })
        }

        fn clear_state(&mut self) {
                self.curr_db = 0;
                self.db_registry.clear_state();
        }

        pub fn load_from_storage(&mut self) -> io::Result<()> {
                let mut buf = Vec::<u8>::new();
                if let Err(msg) = fs::OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(true)
                                .truncate(false)
                                .open(&self.config().db_file)
                                .and_then(|mut file| file.read_to_end(&mut buf)) {
                        self.init()?;
                        return Err(io::Error::new(io::ErrorKind::Other, msg));
                }
                if buf.is_empty() {
                        self.init()?;
                        return Ok(());
                }
                self.load_state(&mut buf)?;
                Ok(())
        }

        fn load_state(&mut self, bytes: &mut [u8]) -> io::Result<()> {
                self.clear_state();
                self.deserialize(bytes)?;
                self.replay_logging_data()?;
                Ok(())
        }

        fn init(&mut self) -> io::Result<()> {
                self.curr_db = 0;
                self.db_registry.add(db::Database::new(0), &[0])?;
                Ok(())
        }

        pub fn write_to_storage(&self) -> io::Result<()> {
                let mut buf = Vec::<u8>::new();
                let tlv = self.serialize()?;
                tlv.serialize_into_buf(&mut buf)?;
                let mut file = fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&self.config().db_file)?;
                file.write_all(&buf)?;
                Ok(())
        }

        fn deserialize(&mut self, buf: &[u8]) -> io::Result<()> {
                let mut loc = 9;
                if buf.is_empty() || buf[loc] == 0 /* num_dbs */ {
                        self.init()?;
                        return Ok(());
                }
                loc += 1;

                while loc < buf.len() {
                        let tlv = srl::DeserTLV::new(&buf[loc..])?;
                        loc += tlv.len();
                        match tlv.srl_type {
                                srl::SerializableType::Database => {
                                        let db = db::Database::deserialize(&tlv)?;
                                        let id = db.id;
                                        self.db_registry.add(db, &[id])?;
                                }
                                srl::SerializableType::BloomFilterStructure => {
                                        let bfs = BloomFilterStructure::deserialize(&tlv)?;
                                        let dbid = bfs.dbid;
                                        if let Some(ref mut db) = self.db_registry.get_mut(&[dbid]) {
                                                let id = bfs.id;
                                                db.bf_registry.add(bfs, &[id])?;
                                        }
                                }
                                srl::SerializableType::Ctl | srl::SerializableType::BitVec => {}
                        }
                }

                Ok(())
        }

        pub fn replay_logging_data(&mut self) -> io::Result<()> {
                wal::WriteAheadLog::replay(self)?;
                Ok(())
        }
}


impl srl::Serializable<Ctl> for Ctl {
        fn serialize(&self) -> io::Result<srl::SerTLV> {
                let mut tlv = srl::SerTLV::new(srl::SerializableType::Ctl);
                tlv.serialize_u8(2u8);

                for db in self.db_registry.list() {
                        let db_tlv = db.serialize()?;
                        tlv.serialize_sertlv(&db_tlv)?;
                }

                for db in self.db_registry.list() {
                        for bf in db.bf_registry.list() {
                                let bf_tlv = bf.serialize()?;
                                tlv.serialize_sertlv(&bf_tlv)?;
                        }
                }

                Ok(tlv)
        }
}