// Copyright © 2025-Present Kasperi Apell <apkaspell@gmail.com>
// SPDX-License-Identifier: AGPL-3.0-or-later


use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write, ErrorKind};
use std::path::{PathBuf};

use crate::cmd;
use crate::ctl;


pub struct WriteAheadLog {
        file_path: PathBuf,
        pub writer: io::BufWriter<fs::File>,
}


impl WriteAheadLog {
        pub fn new(wal_file: &PathBuf) -> io::Result<Self> {
                let file = fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .read(true)
                        .open(wal_file)?;
                Ok(Self { file_path: wal_file.clone(), writer: io::BufWriter::new(file) })
        }

        pub fn log(&mut self, bytes: &[u8]) -> io::Result<()> {
                self.writer.write_all(&u16::to_le_bytes(bytes.len() as u16))?;
                self.writer.write_all(bytes)?;
                self.writer.flush()?;
                Ok(())
        }

        #[expect(dead_code)]
        pub fn clear(&mut self) -> io::Result<()> {
                self.writer.flush()?;
                let file = self.writer.get_mut();
                file.set_len(0)?;
                file.seek(SeekFrom::Start(0))?;
                Ok(())
        }

        pub fn replay(ctl: &mut ctl::Ctl) -> io::Result<()> {
                ctl.wa_log().writer.flush()?;

                let file = fs::File::open(&ctl.wa_log().file_path)?;
                let mut reader = io::BufReader::new(file);

                let mut prefix_len_buf = [0u8; 2];
                let mut cmd_buf = Vec::new();

                loop {
                        match reader.read_exact(&mut prefix_len_buf) {
                                Ok(()) => {
                                        let cmd_len = u16::from_le_bytes(prefix_len_buf);

                                        if cmd_len == 0 {
                                                continue;
                                        }

                                        cmd_buf.clear();
                                        cmd_buf.try_reserve(cmd_len as usize)?;

                                        if reader.by_ref().take(cmd_len as u64).read_to_end(&mut cmd_buf).is_err() {
                                                return Err(io::Error::new(io::ErrorKind::Other, "replay"));
                                        }

                                        if cmd_buf.len() != cmd_len as usize {
                                                return Err(io::Error::new(io::ErrorKind::Other, "replay"));
                                        }

                                        let tlv = cmd::CmdTLV::new(&cmd_buf[0..])?;
                                        let cmd = cmd::decode_cmd(&tlv)?;
                                        let mut resp = cmd::CmdResponseTLV::new();
                                        if let cmd::Cmd::Write(write_cmd) = cmd {
                                                cmd::dispatch_write_cmd(&write_cmd, ctl, &mut resp)?;
                                        }
                                }
                                Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => {
                                        break;
                                }
                                Err(_) => {
                                        return Err(io::Error::new(io::ErrorKind::Other, "replay"));
                                }
                        }

                }

                ctl.wa_log().writer.flush()?;
                Ok(())
        }
}