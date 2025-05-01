use std::io;

use prob::bf::{BloomFilterStructure};
use stor::srl;

use crate::reg;


#[derive(Debug)]
pub struct Database {
        pub id: u8,
        pub bf_registry: reg::Registry<BloomFilterStructure>,
}


impl Database {
        #[must_use]
        pub fn new(id: u8) -> Self {
                Self {
                        id,
                        bf_registry: reg::Registry::<BloomFilterStructure>::new_blank(),
                }
        }
}


impl srl::Deserializable for Database {
        fn deserialize(tlv: &srl::DeserTLV) -> io::Result<Self>
        where Self: Sized
        {
                let buf = &tlv.val;
                Ok(Database::new(srl::DeserTLV::deserialize_u8(buf)?))
        }
}


impl srl::Serializable<Database> for Database {
        fn serialize(&self) -> io::Result<srl::SerTLV> {
                let mut tlv = srl::SerTLV::new(srl::SerializableType::Database);
                tlv.serialize_u8(self.id);
                Ok(tlv)
        }
}