//! Implement a bloom filter.

use std::io;

use qstra_prim::bv;
use qstra_stor::srl::{self};


#[derive(Debug)]
pub struct BloomFilterStructure {
        pub dbid: u8,
        pub id: u8,
        pub inner: BloomFilter,
}


impl BloomFilterStructure {
        #[must_use]
        pub fn new_default(id: u8, dbid: u8) -> Self {
                Self {
                        dbid,
                        id,
                        inner: BloomFilter::default(),
                }
        }

        #[must_use]
        pub fn new(id: u8, dbid: u8, cpty: usize, bit_cnt: usize, hfn_cnt: usize) -> Self {
                Self {
                        dbid,
                        id,
                        inner: BloomFilter::new(cpty, bit_cnt, hfn_cnt),
                }
        }
}


impl srl::Deserializable for BloomFilterStructure {
        fn deserialize(tlv: &srl::DeserTLV) -> io::Result<Self>
        where Self: Sized
        {
                let buf = &tlv.val;
                let bv_tlv = srl::DeserTLV::new(&buf[11..])?;
                let bf = BloomFilter {
                        hfn_cnt: 2,
                        bit_cnt: srl::DeserTLV::deserialize_usize(&buf[3..])?,
                        bits: bv::BitVec::deserialize(&bv_tlv)?
                };
                Ok(Self {
                        dbid: srl::DeserTLV::deserialize_u8(&buf[1..])?,
                        id: srl::DeserTLV::deserialize_u8(&buf[0..])?,
                        inner: bf,
                })
        }
}


impl srl::Serializable<BloomFilterStructure> for BloomFilterStructure {
        fn serialize(&self) -> io::Result<srl::SerTLV> {
                let mut tlv = srl::SerTLV::new(srl::SerializableType::BloomFilterStructure);
                tlv.serialize_u8(self.id);
                tlv.serialize_u8(self.dbid);

                #[allow(clippy::cast_possible_truncation)]
                tlv.serialize_u8(self.inner.hfn_cnt as u8);

                tlv.serialize_usize(self.inner.bit_cnt)?;
                let bv_tlv = self.inner.bits.serialize()?;
                tlv.serialize_sertlv(&bv_tlv)?;
                Ok(tlv)
        }
}


#[derive(Debug)]
pub struct BloomFilter {
        pub bits: bv::BitVec,
        pub hfn_cnt: usize,
        pub bit_cnt: usize,
}


impl Default for BloomFilter {
        #[must_use]
        fn default() -> Self {
                Self {
                        bits: bv::BitVec::with_capacity(1000),
                        bit_cnt: 1000,
                        hfn_cnt: 2,
                }
        }
}


impl BloomFilter {
        #[must_use]
        pub fn new(cpty: usize, bit_cnt: usize, hfn_cnt: usize) -> Self {
                Self {
                        bits: bv::BitVec::with_capacity(cpty),
                        bit_cnt,
                        hfn_cnt,
                }
        }

        #[inline]
        pub fn add(&mut self, bytes: &[u8]) -> io::Result<()> {
                let h0 = self.hash0(bytes);
                let h1 = self.hash1(bytes);
                self.bits.set(h0)?;
                self.bits.set(h1)?;
                if self.hfn_cnt < 3 {
                        return Ok(());
                }
                for i in 3..=self.hfn_cnt {
                        // The Kirsch–Mitzenmacher optimization
                        self.bits.set((h0.wrapping_add(h1.wrapping_mul(i))) % self.bit_cnt)?;
                }
                Ok(())
        }

        #[inline]
        pub fn has(&self, bytes: &[u8]) -> io::Result<bool> {
                let h0 = self.hash0(bytes);
                let h1 = self.hash1(bytes);
                if !self.bits.is_set(h0)? || !self.bits.is_set(h1)? {
                        return Ok(false);
                }
                if self.hfn_cnt < 3 {
                        return Ok(true);
                }
                for i in 3..=self.hfn_cnt {
                        // The Kirsch–Mitzenmacher optimization
                        if !self.bits.is_set((h0.wrapping_add(h1.wrapping_mul(i))) % self.bit_cnt)? {
                                return Ok(false);
                        }
                }
                Ok(true)
        }

        // The djb2 hash function
        #[inline]
        fn hash0(&self, bytes: &[u8]) -> usize {
                let mut h: usize = 5381;
                for b in bytes {
                        h = ((h << 5).wrapping_add(h)).wrapping_add(*b as usize);
                }
                h % self.bit_cnt
        }

        // The sdbm hash function
        #[inline]
        fn hash1(&self, bytes: &[u8]) -> usize {
                let mut h: usize = 0;
                for b in bytes {
                        h = (((*b as usize).wrapping_add(h << 6)).wrapping_add(h << 16)).wrapping_sub(h);
                }
                h % self.bit_cnt
        }
}