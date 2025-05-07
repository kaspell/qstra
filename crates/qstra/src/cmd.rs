// Copyright © 2025-Present Kasperi Apell <apkaspell@gmail.com>
// SPDX-License-Identifier: AGPL-3.0-or-later


use std::cell::RefCell;
use std::io;
use std::rc::Rc;

use tokio::io::AsyncWriteExt;

use qstra_prob::bf::BloomFilterStructure;

use crate::ctl;
use crate::db;


const END_SENTINEL: u8 = 255;

const TOKEN_FALSE: u8 = 0;
const TOKEN_TRUE: u8 = 1;

const U8_OFFSET: usize = std::mem::size_of::<u8>();


pub struct CmdResponseTLV {
        rc: CmdResponseCode,
        val: Vec<u8>,
}


impl CmdResponseTLV {
        #[must_use]
        pub fn new() -> Self {
                Self { rc: CmdResponseCode::Success, val: Vec::<u8>::new() }
        }

        pub fn status(&self) -> CmdResponseCode {
                self.rc
        }

        #[inline(always)]
        fn init_error_response(&mut self) {
                self.rc = CmdResponseCode::Error;
        }

        #[inline(always)]
        #[expect(dead_code)]
        fn init_success_response(&mut self) {
                self.rc = CmdResponseCode::Success;
        }

        #[inline(always)]
        fn append(&mut self, x: u8) {
                self.val.push(x);
        }

        pub async fn respond<S>(&self, mut stream: S) -> io::Result<()>
        where S: AsyncWriteExt + Unpin
        {
                let mut ret = Vec::<u8>::with_capacity(self.val.len() + 1);
                ret.push(self.rc as u8);
                ret.extend(&self.val);
                ret.push(END_SENTINEL);
                if stream.write_all(&ret).await.is_err() {
                        eprintln!("Error responding to client stream.");
                }
                Ok(())
        }
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum CmdResponseCode {
        Success = 0,
        Error = 1,
}


struct LV<'a> {
        val: &'a [u8],
}


impl<'a> LV<'a> {
        fn new(buf: &'a [u8]) -> io::Result<Self> {
                if buf.len() < 2 {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "impl LV: new: too few bytes in buffer"));
                }
                let len = buf[0] as usize;
                let val = &buf[U8_OFFSET..=len];
                Ok(Self { val })
        }
}


pub enum Cmd<'a> {
        Read(ReadCmd<'a>),
        Write(WriteCmd<'a>),
}


pub enum ReadCmd<'a> {
        BloomFilter(ReadCmdBloomFilter<'a>),
        Ctl(ReadCmdCtl),
}


pub(crate) struct ReadCmdCtl {
        op: ReadOpCtl,
}


enum ReadOpCtl {
        WriteData,
}


pub(crate) struct ReadCmdBloomFilter<'a> {
        db_id: u8,
        bf_id: u8,
        op: ReadOpBloomFilter<'a>,
}


enum ReadOpBloomFilter<'a> {
        Has(ReadOpBloomFilterHas<'a>),
        HasBatch(ReadOpBloomFilterHasBatch<'a>),
}


struct ReadOpBloomFilterHas<'a> {
        elt: &'a [u8],
}


impl ReadOpBloomFilterHas<'_> {
        fn execute(&self, bfs: &BloomFilterStructure, resp: &mut CmdResponseTLV) -> io::Result<()> {
                let mut ans = TOKEN_FALSE;
                if bfs.inner.has(self.elt)? {
                        ans = TOKEN_TRUE;
                }
                resp.append(ans);
                Ok(())
        }
}


struct ReadOpBloomFilterHasBatch<'a> {
        elts: &'a [u8],
}


impl ReadOpBloomFilterHasBatch<'_> {
        fn execute(&self, bfs: &BloomFilterStructure, resp: &mut CmdResponseTLV) -> io::Result<()> {
                let mut idx = 0;
                let len = self.elts.len();
                let mut ans_elt;

                while idx < len {
                        let lv = match LV::new(&self.elts[idx..]) {
                                Ok(v) => v,
                                Err(_) => {
                                        resp.init_error_response();
                                        return Ok(());
                                }
                        };
                        ans_elt = TOKEN_FALSE;
                        if bfs.inner.has(lv.val)? {
                                ans_elt = TOKEN_TRUE;
                        }
                        resp.append(ans_elt);
                        idx += lv.val.len()+1;
                }
                Ok(())
        }
}


pub enum WriteCmd<'a> {
        Ctl(WriteCmdCtl),
        Database(WriteCmdDatabase),
        BloomFilter(WriteCmdBloomFilter<'a>),
}


pub(crate) struct WriteCmdCtl {
        op: WriteOpCtl,
}


enum WriteOpCtl {
        WalReplay,
        LoadData,
}


pub(crate) struct WriteCmdDatabase {
        db_id: u8,
        op: WriteOpDatabase,
}


enum WriteOpDatabase {
        NewBloomFilter(WriteOpDatabaseNewBloomFilter),
}


struct WriteOpDatabaseNewBloomFilter {
        bf_id: u8,
}


impl WriteOpDatabaseNewBloomFilter {
        fn execute(&self, db: &mut db::Database, resp: &mut CmdResponseTLV) -> io::Result<()> {
                match db.bf_registry.get(&[self.bf_id]) {
                        Some(_) => {
                                resp.init_error_response();
                        }
                        None => {
                                db.bf_registry.add(BloomFilterStructure::new_default(self.bf_id, db.id), &[self.bf_id])?;
                        }
                }
                Ok(())
        }
}


pub(crate) struct WriteCmdBloomFilter<'a> {
        db_id: u8,
        bf_id: u8,
        op: WriteOpBloomFilter<'a>,
}


enum WriteOpBloomFilter<'a> {
        Add(WriteOpBloomFilterAdd<'a>),
        AddBatch(WriteOpBloomFilterAddBatch<'a>),
}


struct WriteOpBloomFilterAdd<'a> {
        elt: &'a [u8],
}


impl WriteOpBloomFilterAdd<'_> {
        fn execute(&self, bfs: &mut BloomFilterStructure, _resp: &mut CmdResponseTLV) -> io::Result<()> {
                bfs.inner.add(self.elt)?;
                Ok(())
        }
}


struct WriteOpBloomFilterAddBatch<'a> {
        elts: &'a [u8],
}


impl WriteOpBloomFilterAddBatch<'_> {
        fn execute(&self, bfs: &mut BloomFilterStructure, resp: &mut CmdResponseTLV) -> io::Result<()> {
                let mut idx = 0;
                let len = self.elts.len();

                while idx < len {
                        let lv = match LV::new(&self.elts[idx..]) {
                                Ok(v) => v,
                                Err(_) => {
                                        resp.init_error_response();
                                        return Ok(());
                                }
                        };
                        bfs.inner.add(lv.val)?;
                        idx += lv.val.len()+1;
                }
                Ok(())
        }
}


pub struct CmdTLV<'a> {
        cmd_type: [u8; 4],
        val: &'a [u8],
}


impl<'a> CmdTLV<'a> {
        pub fn new(buf: &'a [u8]) -> io::Result<Self> {
                if buf.len() < 9 {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "impl CmdTLV: new: too few bytes in buffer to form TLV"));
                }

                // The bytes at indices 0, 1, 2, and 3 are reserved for the command type
                let cmd_type: [u8; 4] = buf[0..4].try_into().unwrap();

                // The bytes at indices 4, 5, 6, and 7 are reserved for the command type
                let len = u32::from_le_bytes(buf[4..8].try_into().unwrap());

                // The bytes at indices 8 and beyond are used for the value
                if buf.len() < 8+len as usize {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "impl CmdTLV: new: too few bytes in buffer to form value"));
                }

                let val = &buf[8..(8+len as usize)];
                Ok(Self { cmd_type, val })
        }

        pub fn value(&self) -> &'a [u8] {
                self.val
        }
}


fn decode_bf_cmd<'a>(tlv: &'a CmdTLV) -> io::Result<Cmd<'a>> {
        let cmd_type = tlv.cmd_type[1];
        let val = &tlv.val;

        if val.len() < 4 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "decode_bf_cmd: too few bytes in buffer"));
        }
        let db_id = val[0];
        let bf_id = val[1];
        let lv = LV::new(&val[2..])?;

        Ok(match cmd_type {
                0 => {
                        let op = WriteOpBloomFilter::Add(WriteOpBloomFilterAdd { elt: lv.val });
                        Cmd::Write(WriteCmd::BloomFilter(WriteCmdBloomFilter { db_id, bf_id, op }))
                }
                1 => {
                        let op = WriteOpBloomFilter::AddBatch(WriteOpBloomFilterAddBatch { elts: lv.val });
                        Cmd::Write(WriteCmd::BloomFilter(WriteCmdBloomFilter { db_id, bf_id, op }))
                }
                2 => {
                        let op = ReadOpBloomFilter::Has(ReadOpBloomFilterHas { elt: lv.val });
                        Cmd::Read(ReadCmd::BloomFilter(ReadCmdBloomFilter { db_id, bf_id, op }))
                }
                3 => {
                        let op = ReadOpBloomFilter::HasBatch(ReadOpBloomFilterHasBatch { elts: lv.val });
                        Cmd::Read(ReadCmd::BloomFilter(ReadCmdBloomFilter { db_id, bf_id, op }))
                }
                _ => {
                        return Err(io::Error::new(io::ErrorKind::Other, "decode_bf_cmd: unrecognized command"));
                }
        })
}


fn decode_db_cmd<'a>(tlv: &'a CmdTLV) -> io::Result<Cmd<'a>> {
        let cmd_type = tlv.cmd_type[1];
        let val = &tlv.val;

        if val.len() < 3 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "decode_db_cmd: too few bytes in buffer"));
        }

        let db_id = val[0];
        let lv = LV::new(&val[1..])?;

        Ok(match cmd_type {
                0 => {
                        let op = WriteOpDatabase::NewBloomFilter(WriteOpDatabaseNewBloomFilter { bf_id: lv.val[0] });
                        Cmd::Write(WriteCmd::Database(WriteCmdDatabase { db_id, op }))
                }
                _ => {
                        return Err(io::Error::new(io::ErrorKind::Other, "decode_db_cmd: unrecognized command"));
                }
        })
}


fn decode_ctl_cmd<'a>(tlv: &'a CmdTLV) -> io::Result<Cmd<'a>> {
        let cmd_type = tlv.cmd_type[1];
        Ok(match cmd_type {
                0 => { Cmd::Write(WriteCmd::Ctl(WriteCmdCtl { op: WriteOpCtl::WalReplay })) }
                1 => { Cmd::Write(WriteCmd::Ctl(WriteCmdCtl { op: WriteOpCtl::LoadData })) }
                2 => { Cmd::Read(ReadCmd::Ctl(ReadCmdCtl { op: ReadOpCtl::WriteData }))}
                _ => { return Err(io::Error::new(io::ErrorKind::Other, "decode_ctl_cmd: unrecognized command")); }
        })
}


pub fn decode_cmd<'a>(tlv: &'a CmdTLV) -> io::Result<Cmd<'a>> {
        let cmd_type = tlv.cmd_type[0];
        Ok(match cmd_type {
                1 => { decode_ctl_cmd(tlv)? }
                2 => { decode_db_cmd(tlv)? }
                3 => { decode_bf_cmd(tlv)? }
                _ => { return Err(io::Error::new(io::ErrorKind::Other, "decode_cmd: unrecognized command")); }
        })
}


fn handle_write_cmd_ctl(cmd: &WriteCmdCtl, ctl: &mut ctl::Ctl, _resp: &mut CmdResponseTLV) -> io::Result<()> {
        match &cmd.op {
                WriteOpCtl::WalReplay => {
                        ctl.replay_logging_data()?;
                }
                WriteOpCtl::LoadData => {
                        ctl.load_from_storage()?;
                }
        }
        Ok(())
}


fn handle_read_cmd_ctl(cmd: &ReadCmdCtl, ctl: &ctl::Ctl, _resp: &mut CmdResponseTLV) -> io::Result<()> {
        match &cmd.op {
                ReadOpCtl::WriteData => {
                        ctl.write_to_storage()?;
                }
        }
        Ok(())
}


fn handle_read_cmd_bf(cmd: &ReadCmdBloomFilter, ctl: &ctl::Ctl, resp: &mut CmdResponseTLV) -> io::Result<()> {
        if let Some(db) = ctl.db_registry.get(&[cmd.db_id]) {
                if let Some(bf) = db.bf_registry.get(&[cmd.bf_id]).as_ref() {
                        match &cmd.op {
                                ReadOpBloomFilter::Has(op) => { op.execute(bf, resp)?; }
                                ReadOpBloomFilter::HasBatch(op) => { op.execute(bf, resp)?; }
                        }
                        return Ok(());
                }
        }
        resp.init_error_response();
        Ok(())
}


fn handle_write_cmd_bf(cmd: &WriteCmdBloomFilter, ctl: &mut ctl::Ctl, resp: &mut CmdResponseTLV) -> io::Result<()> {
        if let Some(db) = ctl.db_registry.get_mut(&[cmd.db_id]) {
                if let Some(bf) = db.bf_registry.get_mut(&[cmd.bf_id]).as_mut() {
                        match &cmd.op {
                                WriteOpBloomFilter::Add(op) => { op.execute(bf, resp)?; }
                                WriteOpBloomFilter::AddBatch(op) => { op.execute(bf, resp)?; }
                        }
                        return Ok(());
                }
        }
        resp.init_error_response();
        Ok(())
}


fn handle_write_cmd_db(cmd: &WriteCmdDatabase, ctl: &mut ctl::Ctl, resp: &mut CmdResponseTLV) -> io::Result<()> {
        if let Some(db) = ctl.db_registry.get_mut(&[cmd.db_id]) {
                match &cmd.op {
                        WriteOpDatabase::NewBloomFilter(op) => {
                                op.execute(db, resp)?;
                        }
                }
                return Ok(())
        }
        resp.init_error_response();
        Ok(())
}


pub fn dispatch_read_cmd(cmd: &ReadCmd, ctl: &ctl::Ctl, resp: &mut CmdResponseTLV) -> io::Result<()> {
        match cmd {
                ReadCmd::BloomFilter(cmd_bf) => { handle_read_cmd_bf(cmd_bf, ctl, resp)?; }
                ReadCmd::Ctl(cmd_ctl) => { handle_read_cmd_ctl(cmd_ctl, ctl, resp)?; }
        }
        Ok(())
}


pub fn dispatch_write_cmd(cmd: &WriteCmd, ctl: &mut ctl::Ctl, resp: &mut CmdResponseTLV) -> io::Result<()> {
        match cmd {
                WriteCmd::Ctl(cmd_ctl) => { handle_write_cmd_ctl(cmd_ctl, ctl, resp)?; }
                WriteCmd::Database(cmd_db) => { handle_write_cmd_db(cmd_db, ctl, resp)?; }
                WriteCmd::BloomFilter(cmd_bf) => { handle_write_cmd_bf(cmd_bf, ctl, resp)?; }
        }
        Ok(())
}


pub async fn dispatch_cmd(ctl_rc: &Rc<RefCell<ctl::Ctl>>, cmd: &Cmd<'_>, resp: &mut CmdResponseTLV ) -> io::Result<()> {
        match &cmd {
                Cmd::Read(read_cmd) => {
                        let ctl_guard = match ctl_rc.try_borrow () {
                                Ok(guard) => guard,
                                Err(e) => {
                                        eprintln!("FATAL: Failed to borrow Ctl: {e}. Shutting down client connection.");
                                        return Ok(());
                                }
                        };
                        dispatch_read_cmd(read_cmd, &ctl_guard, resp)?;
                }
                Cmd::Write(write_cmd) => {
                        let mut ctl_guard = match ctl_rc.try_borrow_mut () {
                                Ok(guard) => guard,
                                Err(e) => {
                                        eprintln!("FATAL: Failed to borrow Ctl mutably: {e}. Shutting down client connection.");
                                        return Ok(());
                                }
                        };
                        dispatch_write_cmd(write_cmd, &mut ctl_guard, resp)?;
                }
        }
        Ok(())
}


#[cfg(test)]
mod tests {
        use super::*;
        use crate::cfg;
        use crate::ctl;

        #[test]
        fn test_parsing() {
                let inbytes: &[u8] = &[1, 0, 255, 255, 3, 0, 0, 0, 0, 1, 0];
                match decode_cmd(&CmdTLV::new(inbytes).unwrap()).unwrap() {
                        Cmd::Write(WriteCmd::Ctl(WriteCmdCtl { op: WriteOpCtl::WalReplay })) => {}
                        _ => { assert!(false) }
                }

                let inbytes: &[u8] = &[1, 1, 255, 255, 3, 0, 0, 0, 0, 1, 0];
                match decode_cmd(&CmdTLV::new(inbytes).unwrap()).unwrap() {
                        Cmd::Write(WriteCmd::Ctl(WriteCmdCtl { op: WriteOpCtl::LoadData })) => {}
                        _ => { assert!(false) }
                }

                let inbytes: &[u8] = &[1, 2, 255, 255, 3, 0, 0, 0, 0, 1, 0];
                match decode_cmd(&CmdTLV::new(inbytes).unwrap()).unwrap() {
                        Cmd::Read(ReadCmd::Ctl(ReadCmdCtl { op: ReadOpCtl::WriteData })) => {}
                        _ => { assert!(false) }
                }

                let inbytes: &[u8] = &[2, 0, 255, 255, 3, 0, 0, 0, 1, 1, 3];
                match decode_cmd(&CmdTLV::new(inbytes).unwrap()).unwrap() {
                        Cmd::Write(WriteCmd::Database(WriteCmdDatabase { db_id, op: WriteOpDatabase::NewBloomFilter(WriteOpDatabaseNewBloomFilter { bf_id })})) => {
                                assert!(db_id == 1);
                                assert!(bf_id == 3);
                        }
                        _ => { assert!(false) }
                }

                let inbytes: &[u8] = &[3, 0, 255, 255, 6, 0, 0, 0, 2, 4, 3, 1, 2, 3];
                match decode_cmd(&CmdTLV::new(inbytes).unwrap()).unwrap() {
                        Cmd::Write(WriteCmd::BloomFilter(WriteCmdBloomFilter { db_id, bf_id, op: WriteOpBloomFilter::Add(WriteOpBloomFilterAdd { elt })})) => {
                                assert!(db_id == 2);
                                assert!(bf_id == 4);
                                assert!(elt == &[1, 2, 3]);
                        }
                        _ => { assert!(false) }
                }

                let inbytes: &[u8] = &[3, 1, 255, 255, 9, 0, 0, 0, 6, 7, 6, 10, 11, 12, 2, 13, 14];
                match decode_cmd(&CmdTLV::new(inbytes).unwrap()).unwrap() {
                        Cmd::Write(WriteCmd::BloomFilter(WriteCmdBloomFilter { db_id, bf_id, op: WriteOpBloomFilter::AddBatch(WriteOpBloomFilterAddBatch { elts })})) => {
                                assert!(db_id == 6);
                                assert!(bf_id == 7);
                                assert!(elts == &[10, 11, 12, 2, 13, 14]);
                        }
                        _ => { assert!(false) }
                }

                let inbytes: &[u8] = &[3, 2, 255, 255, 7, 0, 0, 0, 1, 1, 4, 99, 98, 97, 96];
                match decode_cmd(&CmdTLV::new(inbytes).unwrap()).unwrap() {
                        Cmd::Read(ReadCmd::BloomFilter(ReadCmdBloomFilter { db_id, bf_id, op: ReadOpBloomFilter::Has(ReadOpBloomFilterHas { elt })})) => {
                                assert!(db_id == 1);
                                assert!(bf_id == 1);
                                assert!(elt == &[99, 98, 97, 96]);
                        }
                        _ => { assert!(false) }
                }

                let inbytes: &[u8] = &[3, 3, 255, 255, 11, 0, 0, 0, 2, 3, 8, 3, 100, 111, 222, 3, 253, 254, 255];
                match decode_cmd(&CmdTLV::new(inbytes).unwrap()).unwrap() {
                        Cmd::Read(ReadCmd::BloomFilter(ReadCmdBloomFilter { db_id, bf_id, op: ReadOpBloomFilter::HasBatch(ReadOpBloomFilterHasBatch { elts })})) => {
                                assert!(db_id == 2);
                                assert!(bf_id == 3);
                                assert!(elts == &[3, 100, 111, 222, 3, 253, 254, 255]);
                        }
                        _ => { assert!(false) }
                }
        }
}