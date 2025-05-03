//! Provide a bit vector utility.

use std::io;

use qstra_stor::srl;


const USIZE_BITS: usize = 8 * std::mem::size_of::<usize>();


#[derive(Debug)]
pub struct BitVec {
        words: Vec::<usize>,
        size: usize,
}


impl BitVec {
        #[must_use]
        pub fn with_capacity(size: usize) -> Self {
                Self { words: vec![0; size.div_ceil(USIZE_BITS).max(1)], size }
        }

        #[inline]
        fn get_idxs(&self, i: usize) -> io::Result<(usize, usize)> {
                if i >= self.size {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, format!("impl BitVex: get_idxs: index out of bounds: size is {} but requested index is {}", self.size, i)));
                }
                Ok((i/USIZE_BITS, i%USIZE_BITS))
        }

        #[inline]
        pub fn is_set(&self, i: usize) -> io::Result<bool> {
                let (byte_idx, bit_idx) = self.get_idxs(i)?;
                Ok(((1usize << bit_idx) & self.words[byte_idx]) > 0)
        }

        #[inline]
        pub fn set(&mut self, i: usize) -> io::Result<()> {
                let (byte_idx, bit_idx) = self.get_idxs(i)?;
                self.words[byte_idx] |= 1usize << bit_idx;
                Ok(())
        }
}


impl srl::Deserializable for BitVec {
        fn deserialize(tlv: &srl::DeserTLV) -> io::Result<Self>
        where Self: Sized
        {
                let buf = &tlv.val;
                Ok(Self {
                        words: srl::DeserTLV::deserialize_vec_usize(&buf[8..])?,
                        size: srl::DeserTLV::deserialize_usize(&buf[0..])?
                })
        }
}


impl srl::Serializable<BitVec> for BitVec {
        fn serialize(&self) -> io::Result<srl::SerTLV> {
                let mut tlv = srl::SerTLV::new(srl::SerializableType::BitVec);
                tlv.serialize_usize(self.size)?;
                tlv.serialize_slice_usize(&self.words)?;
                Ok(tlv)
        }
}


#[cfg(test)]
mod tests {
        use super::*;

        #[test]
        fn test_endianness() {
                let mut bv = BitVec::with_capacity(64);
                bv.set(0).unwrap();
                assert!(bv.words.len() == 1);
                assert!(bv.words[0] == 1usize, "{}", bv.words[0]);
        }

        #[test]
        fn test_word_sizing() {
                let data = [(1usize, 1), (USIZE_BITS-1, 1), (5*USIZE_BITS+1, 6), (8*USIZE_BITS-63, 8)];
                let mut bv;
                for (nr_bits, nr_bytes) in data {
                        bv = BitVec::with_capacity(nr_bits);
                        assert!(bv.words.len() == nr_bytes, "bv.words.len() = {} did not equal nr_bytes = {}", bv.words.len(), nr_bytes)
                }
        }

        #[test]
        fn test_bit_setting() {
                let data = [(32, 2), (1000, 10), (129, 1), (55, 54)];

                for (cpty, k) in data {
                        let mut bv = BitVec::with_capacity(cpty);
                        for i in 0..cpty {
                                if i % k == 0 {
                                        bv.set(i).unwrap();
                                }
                        }

                        for i in 0..cpty {
                                let mut ans = bv.is_set(i).unwrap();
                                if i % k != 0 {
                                        ans = !ans;
                                }
                                assert!(ans);
                        }
                }
        }
}