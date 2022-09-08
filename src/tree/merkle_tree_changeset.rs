use ed25519_dalek::{PublicKey, SecretKey, Signature};

use crate::{
    crypto::{signable_tree, Hash},
    sign, Node,
};

/// Changeset for a `MerkleTree`. This allows to incrementally change a `MerkleTree` in two steps:
/// first create the changes to this changeset, get out information from this to put to the oplog,
/// and the commit the changeset to the tree.
///
/// This is called "MerkleTreeBatch" in Javascript, see:
/// https://github.com/hypercore-protocol/hypercore/blob/master/lib/merkle-tree.js
#[derive(Debug)]
pub struct MerkleTreeChangeset {
    pub(crate) length: u64,
    pub(crate) ancestors: u64,
    pub(crate) byte_length: u64,
    pub(crate) batch_length: u64,
    pub(crate) fork: u64,
    pub(crate) roots: Vec<Node>,
    pub(crate) nodes: Vec<Node>,
    pub(crate) hash_and_signature: Option<(Box<[u8]>, Signature)>,
    pub(crate) upgraded: bool,

    // Safeguarding values
    pub(crate) original_tree_length: u64,
    pub(crate) original_tree_fork: u64,
}

impl MerkleTreeChangeset {
    pub fn new(length: u64, byte_length: u64, fork: u64, roots: Vec<Node>) -> MerkleTreeChangeset {
        Self {
            length,
            ancestors: length,
            byte_length,
            batch_length: 0,
            fork,
            roots,
            nodes: vec![],
            hash_and_signature: None,
            upgraded: false,
            original_tree_length: length,
            original_tree_fork: fork,
        }
    }

    pub fn append(&mut self, data: &[u8]) -> usize {
        let len = data.len();
        let head = self.length * 2;
        let mut iter = flat_tree::Iterator::new(head);
        let node = Node::new(head, Hash::data(data).as_bytes().to_vec(), len as u64);
        self.append_root(node, &mut iter);
        self.batch_length += 1;
        len
    }

    pub fn append_root(&mut self, node: Node, iter: &mut flat_tree::Iterator) {
        self.upgraded = true;
        self.length += iter.factor() / 2;
        self.byte_length += node.length;
        self.roots.push(node.clone());
        self.nodes.push(node);

        while self.roots.len() > 1 {
            let a = &self.roots[self.roots.len() - 1];
            let b = &self.roots[self.roots.len() - 2];
            if iter.sibling() != b.index {
                iter.sibling(); // unset so it always points to last root
                break;
            }

            let node = Node::new(
                iter.parent(),
                Hash::parent(&a, &b).as_bytes().into(),
                a.length + b.length,
            );
            let _ = &self.nodes.push(node.clone());
            let _ = &self.roots.pop();
            let _ = &self.roots.pop();
            let _ = &self.roots.push(node);
        }
    }

    /// Hashes and signs the changeset
    pub fn hash_and_sign(&mut self, public_key: &PublicKey, secret_key: &SecretKey) {
        let hash = self.hash();
        let signable = signable_tree(&hash, self.length, self.fork);
        let signature = sign(&public_key, &secret_key, &signable);
        self.hash_and_signature = Some((hash, signature));
    }

    /// Calculates a hash of the current set of roots
    pub fn hash(&self) -> Box<[u8]> {
        Hash::tree(&self.roots).as_bytes().into()
    }
}
