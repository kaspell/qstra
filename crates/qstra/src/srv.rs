// Copyright © 2025 Kasperi Apell <apkaspell@gmail.com>
// SPDX-License-Identifier: AGPL-3.0-or-later
//
//
//! Define the server-side logic.


use std::cell::RefCell;
use std::io;
use std::rc::Rc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

use crate::cmd;
use crate::ctl;


const MAX_BUF_SZ: usize = 2048;


pub async fn handle_client<S>(mut stream: S, ctl_rc: Rc<RefCell<ctl::Ctl>>) -> io::Result<()>
where S: AsyncRead + AsyncWrite + Unpin,
{
        let mut inbuf = [0; MAX_BUF_SZ];

        loop {
                let read_cnt = match stream.read(&mut inbuf).await {
                        Ok(0) => {
                                println!("Client connection closed.");
                                return Ok(());
                        }
                        Ok(n) => n,
                        Err(e) => {
                                eprintln!("Error reading from client stream: {e}");
                                return Ok(());
                        }
                };
                let inbytes = &inbuf[..read_cnt];
                let tlv = cmd::CmdTLV::new(inbytes)?;
                let cmd = cmd::decode_cmd(&tlv)?;
                let mut resp = cmd::CmdResponseTLV::new();
                cmd::dispatch_cmd(&ctl_rc, &cmd, &mut resp).await?;

                if resp.respond(&mut stream).await.is_err() {
                        eprintln!("Error responding to client stream.");
                        return Ok(());
                }

                println!("Response sent to client.");

                postprocess_cmd(&ctl_rc, &cmd, &resp, &tlv).await?;
        }
}


async fn postprocess_cmd(
        ctl_rc: &Rc<RefCell<ctl::Ctl>>,
        cmd: &cmd::Cmd<'_>,
        resp: &cmd::CmdResponseTLV,
        tlv: &cmd::CmdTLV<'_>
) -> io::Result<()>
{
        if let cmd::CmdResponseCode::Error = resp.status() {
                return Ok(())
        }
        match cmd {
                cmd::Cmd::Write(cmd::WriteCmd::BloomFilter(_) | cmd::WriteCmd::Database(_)) => {
                        let mut ctl_guard = match ctl_rc.try_borrow_mut () {
                                Ok(guard) => guard,
                                Err(e) => {
                                        eprintln!("FATAL: Failed to borrow Ctl mutably: {e}");
                                        return Ok(());
                                }
                        };
                        ctl_guard.wa_log().log(tlv.value())?;
                }
                cmd::Cmd::Write(_) | cmd::Cmd::Read(_) => {}
        }
        Ok(())
}