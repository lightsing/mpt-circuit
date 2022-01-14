use num_bigint::BigUint;
use num_traits::identities::{One, Zero};
use serde::Deserialize;
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug, thiserror::Error)]
/// Row type deserialization errors.
pub enum RowDeError {
    #[error(transparent)]
    /// hex decode error
    Hex(#[from] hex::FromHexError),
    #[error("cannot parse bigInt repr")]
    /// bigInt decode error
    BigInt,
}

/// Verify Rows
pub trait Verify {
    fn verify(&self) -> bool;
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
/// struct for a halo row
pub struct Row {
    /// is this the first row
    pub is_first: bool,
    /// the sibling hash
    pub sib: Hash,
    /// the current depth
    pub depth: usize,
    /// the path
    pub path: BigUint,
    /// the path_acc
    pub path_acc: BigUint,
    /// HashType of old_hash
    pub old_hash_type: HashType,
    /// old hash
    pub old_hash: Hash,
    /// old value
    pub old_value: Hash,
    /// HashType of new_hash
    pub new_hash_type: HashType,
    /// new hash
    pub new_hash: Hash,
    /// new value
    pub new_value: Hash,
    /// key of this row
    pub key: Hash,
    /// new hash root
    pub new_root: Hash,
}

impl Verify for Vec<Row> {
    fn verify(&self) -> bool {
        const VALID_STATE: &[HashType] = &[
            HashType::Empty,
            HashType::Leaf,
            HashType::Middle,
            HashType::LeafExt,
            HashType::LeafExtFinal,
        ];
        const VALID_TRANSACTION: &[(HashType, HashType)] = &[
            (HashType::Middle, HashType::Middle),
            (HashType::Middle, HashType::Empty),
            (HashType::Middle, HashType::Leaf),
            (HashType::Middle, HashType::LeafExt),
            (HashType::Middle, HashType::LeafExtFinal),
            (HashType::LeafExt, HashType::LeafExt),
            (HashType::LeafExt, HashType::LeafExtFinal),
            (HashType::LeafExtFinal, HashType::Empty),
            (HashType::LeafExtFinal, HashType::Leaf),
        ];
        const VALID_TRANSITIONS: &[(HashType, HashType)] = &[
            (HashType::Empty, HashType::Leaf),
            (HashType::Leaf, HashType::Empty),
            (HashType::Leaf, HashType::Leaf),
            (HashType::Middle, HashType::LeafExtFinal),
            (HashType::LeafExtFinal, HashType::Middle),
            (HashType::Middle, HashType::LeafExt),
            (HashType::LeafExt, HashType::Middle),
            (HashType::Middle, HashType::Middle),
        ];

        if self.is_empty() {
            return true;
        }

        let mut verified = true;

        // part 1: check hash calculations of adjacent rows
        for (idx, row) in self.iter().enumerate() {
            if row.is_first {
                continue;
            }
            if self[idx - 1].old_value != row.old_hash {
                error!(
                    "adjacent row old hash mismatch: {} {}",
                    self[idx - 1].old_value,
                    row.old_hash
                );
                verified = false
            }
            if self[idx - 1].new_value != row.new_hash {
                error!(
                    "adjacent row new hash mismatch: {} {}",
                    self[idx - 1].new_value,
                    row.new_hash
                );
                verified = false
            }
        }
        // part 2.1: check ‘HashType’ of adjacent rows
        for (idx, row) in self.iter().enumerate() {
            if row.is_first {
                if !VALID_STATE.contains(&row.old_hash_type) {
                    error!("old hash type invalid: {:?}", row.old_hash_type);
                    verified = false
                }
                if !VALID_STATE.contains(&row.new_hash_type) {
                    error!("new hash type invalid: {:?}", row.new_hash_type);
                    verified = false
                }
            } else {
                if !VALID_TRANSACTION.contains(&(self[idx - 1].old_hash_type, row.old_hash_type)) {
                    error!(
                        "invalid transaction from {:?} to {:?}",
                        self[idx - 1].old_hash_type,
                        row.old_hash_type
                    );
                    verified = false
                }
                if !VALID_TRANSACTION.contains(&(self[idx - 1].new_hash_type, row.new_hash_type)) {
                    error!(
                        "invalid transaction from {:?} to {:?}",
                        self[idx - 1].new_hash_type,
                        row.new_hash_type
                    );
                    verified = false
                }
            }
        }
        // part 2.2: check HashType between old and new
        for row in self.iter() {
            if !VALID_TRANSITIONS.contains(&(row.old_hash_type, row.new_hash_type)) {
                error!(
                    "invalid transition from {:?} to {:?}",
                    row.old_hash_type, row.new_hash_type
                );
                verified = false
            }
        }
        // part 3: check hash calculation
        for (idx, row) in self.iter().enumerate() {
            match row.old_hash_type {
                HashType::Empty => {
                    if row.old_hash != Hash::zero() {
                        error!("#{} hash mismatch", idx);
                        verified = false
                    }
                }
                HashType::Middle => {
                    // unimplemented
                }
                HashType::LeafExt => {
                    if row.old_value != row.old_hash {
                        error!("#{} hash mismatch", idx);
                        verified = false
                    }
                }
                HashType::LeafExtFinal => {
                    if row.sib != row.old_hash {
                        error!("#{} hash mismatch", idx);
                        verified = false
                    }
                }
                HashType::Leaf => {
                    // unimplemented
                }
            }

            match row.new_hash_type {
                HashType::Empty => {
                    if row.new_hash != Hash::zero() {
                        error!("#{} hash mismatch", idx);
                        verified = false
                    }
                }
                HashType::Middle => {
                    // unimplemented
                }
                HashType::LeafExt => {
                    if row.new_value != row.new_hash {
                        error!("#{} hash mismatch", idx);
                        verified = false
                    }
                }
                HashType::LeafExtFinal => {
                    if row.sib != row.new_hash {
                        error!("#{} hash mismatch", idx);
                        verified = false
                    }
                }
                HashType::Leaf => {
                    // unimplemented
                }
            }
        }
        // part4: check key
        for (idx, row) in self.iter().enumerate() {
            if let HashType::Middle | HashType::LeafExt | HashType::LeafExtFinal = row.old_hash_type
            {
                if row.path != BigUint::zero() && row.path != BigUint::one() {
                    error!(
                        "mid/leafext/leafextfinal should have path 0/1 instead of {}",
                        row.path.to_str_radix(2)
                    );
                    verified = false
                }
            }
            if let HashType::Middle | HashType::LeafExt | HashType::LeafExtFinal = row.new_hash_type
            {
                if row.path != BigUint::zero() && row.path != BigUint::one() {
                    error!(
                        "mid/leafext/leafextfinal should have path 0/1 instead of {}",
                        row.path.to_str_radix(2)
                    );
                    verified = false
                }
            }
            if row.is_first {
                if row.depth != 0 {
                    error!("first row should have depth 0 instead of {}", row.depth);
                    verified = false
                }
            } else {
                let expected = self[idx - 1].depth + 1;
                if row.depth != expected {
                    error!(
                        "row should have depth {} instead of {}",
                        expected, row.depth
                    );
                    verified = false
                }
            }
            if idx == self.len() - 1 || self[idx + 1].is_first {
                let converted = BigUint::from(row.key);
                if converted != row.path_acc {
                    error!(
                        "leaf should have path_acc == key, key: {}, path_acc: {}",
                        converted.to_str_radix(2),
                        row.path_acc.to_str_radix(2)
                    );
                    verified = false
                }
            }
        }
        verified
    }
}

#[derive(Debug, Deserialize)]
struct RowDe {
    is_first: bool,
    sib: String,
    depth: usize,
    path: String,
    path_acc: String,
    old_hash_type: HashType,
    old_hash: String,
    old_value: String,
    new_hash_type: HashType,
    new_hash: String,
    new_value: String,
    key: String,
    new_root: String,
}

impl RowDe {
    pub fn from_lines(lines: &str) -> Result<Vec<RowDe>, serde_json::Error> {
        lines.trim().split('\n').map(serde_json::from_str).collect()
    }
}

impl TryFrom<&RowDe> for Row {
    type Error = RowDeError;

    fn try_from(r: &RowDe) -> Result<Self, Self::Error> {
        Ok(Self {
            is_first: r.is_first,
            sib: Hash::try_from(r.sib.as_str())?,
            depth: r.depth,
            path: BigUint::parse_bytes(r.path.as_bytes(), 2).ok_or(RowDeError::BigInt)?,
            path_acc: BigUint::parse_bytes(r.path_acc.as_bytes(), 2).ok_or(RowDeError::BigInt)?,
            old_hash_type: r.old_hash_type,
            old_hash: Hash::try_from(r.old_hash.as_str())?,
            old_value: Hash::try_from(r.old_value.as_str())?,
            new_hash_type: r.new_hash_type,
            new_hash: Hash::try_from(r.new_hash.as_str())?,
            new_value: Hash::try_from(r.new_value.as_str())?,
            key: Hash::try_from(r.key.as_str())?,
            new_root: Hash::try_from(r.new_root.as_str())?,
        })
    }
}

#[derive(Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
/// a wrapper for Hash operations.
pub struct Hash([u8; 32]);

impl Hash {
    /// create hash from bytes array
    pub fn new(value: [u8; 32]) -> Self {
        Self(value)
    }

    /// get hex representation of hash
    pub fn hex(&self) -> String {
        hex::encode(self.0)
    }

    #[inline(always)]
    /// get constant zero hash
    const fn zero() -> Hash {
        Hash([0; 32])
    }
}

impl Debug for Hash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{:032}", self.hex())
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{:032}", self.hex())
    }
}

impl AsRef<[u8; 32]> for Hash {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl AsMut<[u8; 32]> for Hash {
    fn as_mut(&mut self) -> &mut [u8; 32] {
        &mut self.0
    }
}

impl TryFrom<&str> for Hash {
    type Error = hex::FromHexError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut hash = Self::default();
        hex::decode_to_slice(value, &mut hash.0)?;
        Ok(hash)
    }
}

impl From<Hash> for BigUint {
    fn from(hash: Hash) -> Self {
        BigUint::from_bytes_le(&hash.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;

    static INIT: Once = Once::new();
    const TEST_FILE: &'static str = include_str!("../rows.jsonl");

    fn setup() {
        INIT.call_once(|| {
            pretty_env_logger::init();
        })
    }

    #[test]
    fn test_de() {
        setup();
        RowDe::from_lines(TEST_FILE).unwrap();
    }

    #[test]
    fn test_parse() {
        setup();
        let rows: Result<Vec<Row>, RowDeError> = RowDe::from_lines(TEST_FILE)
            .unwrap()
            .iter()
            .map(Row::try_from)
            .collect();
        let rows = rows.unwrap();
        for row in rows.iter() {
            println!("{:?}", row);
        }
        assert!(rows.verify());
    }
}

/// Indicate the type of a row
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialOrd, PartialEq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum HashType {
    /// Empty node
    Empty = 1,
    /// middle node
    Middle,
    /// leaf node which is extended to middle in insert
    LeafExt,
    /// leaf node which is extended to middle in insert, which is the last node in new path
    LeafExtFinal,
    /// leaf node
    Leaf,
}
