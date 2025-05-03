// Copyright © 2025-Present Kasperi Apell <apkaspell@gmail.com>
// SPDX-License-Identifier: AGPL-3.0-or-later


use std::io;

pub const U8_OFFSET: usize = std::mem::size_of::<u8>();
pub const USIZE_OFFSET: usize = std::mem::size_of::<usize>();


#[repr(u8)]
pub enum SerializableType {
        Ctl = 0,
        Database = 1,
        BloomFilterStructure = 2,
        BitVec = 3,
}


impl TryFrom<u8> for SerializableType {
        type Error = io::Error;

        fn try_from(byte: u8) -> io::Result<Self> {
                match byte {
                        3 => Ok(SerializableType::BitVec),
                        2 => Ok(SerializableType::BloomFilterStructure),
                        1 => Ok(SerializableType::Database),
                        0 => Ok(SerializableType::Ctl),
                        _ => {
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown discriminant for SerializableType"));
                        }
                }
        }
}


impl SerializableType {
        #[must_use]
        pub fn value(&self) -> u8 {
                match self {
                        SerializableType::Ctl => 0,
                        SerializableType::Database => 1,
                        SerializableType::BloomFilterStructure => 2,
                        SerializableType::BitVec => 3,
                }
        }
}


pub struct DeserTLV<'a> {
        pub srl_type: SerializableType,
        pub val: &'a [u8],
}


impl<'a> DeserTLV<'a> {
        pub fn new(buf: &'a [u8]) -> io::Result<Self> {
                if buf.len() < 2*U8_OFFSET + USIZE_OFFSET {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "too few bytes in buffer"));
                }
                let srl_type = buf[0].try_into()?;
                let bytes = buf[U8_OFFSET..=USIZE_OFFSET]
                        .try_into()
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "slice length mismatch"))?;
                let len = usize::from_le_bytes(bytes);
                let start_idx = U8_OFFSET + USIZE_OFFSET;
                let end_idx = start_idx.checked_add(len)
                                       .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "end_idx overflow"))?;
                if buf.len() < end_idx {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "buffer shorter than the deserialized end_idx"));
                }
                let val = &buf[start_idx..end_idx];
                Ok(Self { srl_type, val })
        }

        #[must_use]
        pub fn len(&self) -> usize {
                1 // u8 enum
                + 8 // Length of the Vec<u8> (usize)
                + self.val.len() // Vec<u8>
        }

        pub fn deserialize_u8(buf: &[u8]) -> io::Result<u8> {
                Ok(buf[0])
        }

        pub fn deserialize_usize(buf: &[u8]) -> io::Result<usize> {
                let bytes = buf[0..USIZE_OFFSET]
                        .try_into()
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "slice length mismatch"))?;
                Ok(usize::from_le_bytes(bytes))
        }

        pub fn deserialize_vec_u8(buf: &[u8]) -> io::Result<Vec<u8>> {
                Ok(buf.to_vec())
        }

        pub fn deserialize_vec_usize(buf: &[u8]) -> io::Result<Vec<usize>> {
                let chunk_size = std::mem::size_of::<usize>();
                let mut ret = Vec::<usize>::new();
                for chunk in buf.chunks(chunk_size) {
                        let bytes = chunk.try_into()
                                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "slice length mismatch"))?;
                        ret.push(usize::from_le_bytes(bytes));
                }
                Ok(ret)
        }
}


pub struct SerTLV {
        pub srl_type: SerializableType,
        pub val: Vec<u8>,
}


impl SerTLV {
        #[must_use]
        pub fn new(srl_type: SerializableType) -> Self {
                Self { srl_type, val: Vec::<u8>::new() }
        }

        #[must_use]
        pub fn len(&self) -> usize {
                1 // u8 enum
                + 8 // length of the Vec<u8> (usize)
                + self.val.len() // Vec<u8>
        }

        pub fn serialize_into_buf(&self, buf: &mut Vec<u8>) -> io::Result<usize> {
                let len = self.val.len();
                buf.push(self.srl_type.value());
                buf.extend_from_slice(&usize::to_le_bytes(len));
                buf.extend(&self.val);
                Ok(len + 9)
        }

        pub fn serialize_u8(&mut self, x: u8) {
                self.val.push(x);
        }

        pub fn serialize_usize(&mut self, x: usize) -> io::Result<()> {
                self.val.extend_from_slice(&usize::to_le_bytes(x));
                Ok(())
        }

        pub fn serialize_slice_u8(&mut self, x: &[u8]) -> io::Result<usize> {
                let ret = x.len();
                self.val.extend_from_slice(x);
                Ok(ret)
        }

        pub fn serialize_slice_usize(&mut self, words: &[usize]) -> io::Result<usize> {
                let mut bytes = Vec::<u8>::new();
                for word in words {
                        bytes.extend(word.to_le_bytes());
                }
                let ret = bytes.len();
                self.val.extend(bytes);
                Ok(ret)
        }

        pub fn serialize_sertlv(&mut self, tlv: &SerTLV) -> io::Result<()> {
                self.serialize_u8(tlv.srl_type.value());
                self.serialize_usize(tlv.val.len())?;
                self.serialize_slice_u8(&tlv.val)?;
                Ok(())
        }
}


pub trait Deserializable {
        fn deserialize(tlv: &DeserTLV) -> io::Result<Self> where Self: Sized;
}


pub trait Serializable<T> {
        fn serialize(&self) -> io::Result<SerTLV>;
}