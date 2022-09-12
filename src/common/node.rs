#[cfg(not(feature = "v10"))] // tree module has the byte manipulation for v10
use anyhow::ensure;
#[cfg(not(feature = "v10"))] // tree module has the byte manipulation for v10
use anyhow::Result;
#[cfg(not(feature = "v10"))] // tree module has the byte manipulation for v10
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use merkle_tree_stream::Node as NodeTrait;
use merkle_tree_stream::{NodeKind, NodeParts};
use pretty_hash::fmt as pretty_fmt;
use std::cmp::Ordering;
use std::convert::AsRef;
use std::fmt::{self, Display};
#[cfg(not(feature = "v10"))] // tree module has the byte manipulation for v10
use std::io::Cursor;

use crate::crypto::Hash;

/// Node byte range
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeByteRange {
    pub(crate) index: u64,
    pub(crate) length: u64,
}

/// Nodes that are persisted to disk.
// TODO: replace `hash: Vec<u8>` with `hash: Hash`. This requires patching /
// rewriting the Blake2b crate to support `.from_bytes()` to serialize from
// disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub(crate) index: u64,
    pub(crate) hash: Vec<u8>,
    pub(crate) length: u64,
    pub(crate) parent: u64,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) blank: bool,
}

impl Node {
    /// Create a new instance.
    // TODO: ensure sizes are correct.
    pub fn new(index: u64, hash: Vec<u8>, length: u64) -> Self {
        Self {
            index,
            hash,
            length: length as u64,
            parent: flat_tree::parent(index),
            data: Some(Vec::with_capacity(0)),
            blank: false,
        }
    }

    /// Creates a new blank node
    pub fn new_blank(index: u64) -> Self {
        Self {
            index,
            hash: vec![0, 32],
            length: 0,
            parent: 0,
            data: None,
            blank: true,
        }
    }

    /// Convert a vector to a new instance.
    ///
    /// Requires the index at which the buffer was read to be passed.
    #[cfg(not(feature = "v10"))] // tree module has the byte manipulation for v10
    pub fn from_bytes(index: u64, buffer: &[u8]) -> Result<Self> {
        ensure!(buffer.len() == 40, "buffer should be 40 bytes");

        let parent = flat_tree::parent(index);
        let mut reader = Cursor::new(buffer);

        // TODO: subslice directly, move cursor forward.
        let capacity = 32;
        let mut hash = Vec::with_capacity(capacity);
        let mut blank = true;
        for _ in 0..capacity {
            let byte = reader.read_u8()?;
            if blank && byte != 0 {
                blank = false;
            };
            hash.push(byte);
        }

        let length = reader.read_u64::<BigEndian>()?;
        Ok(Self {
            hash,
            length,
            index,
            parent,
            data: Some(Vec::with_capacity(0)),
            blank,
        })
    }

    /// Convert to a buffer that can be written to disk.
    #[cfg(not(feature = "v10"))] // tree module has the byte manipulation for v10
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut writer = Vec::with_capacity(40);
        writer.extend_from_slice(&self.hash);
        writer.write_u64::<BigEndian>(self.length as u64)?;
        Ok(writer)
    }
}

impl NodeTrait for Node {
    #[inline]
    fn index(&self) -> u64 {
        self.index
    }

    #[inline]
    fn hash(&self) -> &[u8] {
        &self.hash
    }

    #[inline]
    fn len(&self) -> u64 {
        self.length as u64
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[inline]
    fn parent(&self) -> u64 {
        self.parent
    }
}

impl AsRef<Node> for Node {
    #[inline]
    fn as_ref(&self) -> &Self {
        self
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Node {{ index: {}, hash: {}, length: {} }}",
            self.index,
            pretty_fmt(&self.hash).unwrap(),
            self.length
        )
    }
}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.index.cmp(&other.index))
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        self.index.cmp(&other.index)
    }
}

impl From<NodeParts<Hash>> for Node {
    fn from(parts: NodeParts<Hash>) -> Self {
        let partial = parts.node();
        let data = match partial.data() {
            NodeKind::Leaf(data) => Some(data.clone()),
            NodeKind::Parent => None,
        };
        let hash: Vec<u8> = parts.hash().as_bytes().into();
        let mut blank = true;
        for byte in &hash {
            if *byte != 0 {
                blank = false;
                break;
            }
        }

        Node {
            index: partial.index(),
            parent: partial.parent,
            length: partial.len() as u64,
            hash,
            data,
            blank,
        }
    }
}
