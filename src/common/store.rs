/// The types of stores that can be created.
#[derive(Debug, Clone, PartialEq)]
pub enum Store {
    /// Tree
    Tree,
    /// Data (block store)
    Data,
    /// Bitfield
    Bitfield,
    /// Oplog
    Oplog,
}

/// Information type about a store.
#[derive(Debug, PartialEq)]
pub enum StoreInfoType {
    /// Read/write content of the store
    Content,
    /// Size in bytes of the store. When flushed, truncates to the given index. `data` is `None`.
    Size,
}

/// Piece of information about a store. Useful for indicating changes that should be made to random
/// access storages or information read from them.
#[derive(Debug)]
pub struct StoreInfo {
    pub(crate) store: Store,
    pub(crate) info_type: StoreInfoType,
    pub(crate) index: u64,
    pub(crate) length: Option<u64>,
    pub(crate) data: Option<Box<[u8]>>,
    /// When reading, indicates missing value (can be true only if allow_miss is given as instruction).
    /// When writing indicates that the value should be dropped.
    pub(crate) miss: bool,
}

impl StoreInfo {
    pub fn new_content(store: Store, index: u64, data: &[u8]) -> Self {
        Self {
            store,
            info_type: StoreInfoType::Content,
            index,
            length: Some(data.len() as u64),
            data: Some(data.into()),
            miss: false,
        }
    }

    pub fn new_content_miss(store: Store, index: u64) -> Self {
        Self {
            store,
            info_type: StoreInfoType::Content,
            index,
            length: None,
            data: None,
            miss: true,
        }
    }

    pub fn new_delete(store: Store, index: u64, length: u64) -> Self {
        Self {
            store,
            info_type: StoreInfoType::Content,
            index,
            length: Some(length),
            data: None,
            miss: true,
        }
    }

    pub fn new_truncate(store: Store, index: u64) -> Self {
        Self {
            store,
            info_type: StoreInfoType::Size,
            index,
            length: None,
            data: None,
            miss: true,
        }
    }

    pub fn new_size(store: Store, index: u64, length: u64) -> Self {
        Self {
            store,
            info_type: StoreInfoType::Size,
            index,
            length: Some(length),
            data: None,
            miss: false,
        }
    }
}

/// Represents an instruction to obtain information about a store.
#[derive(Debug)]
pub struct StoreInfoInstruction {
    pub(crate) store: Store,
    pub(crate) info_type: StoreInfoType,
    pub(crate) index: u64,
    pub(crate) length: Option<u64>,
    pub(crate) allow_miss: bool,
}

impl StoreInfoInstruction {
    pub fn new_content(store: Store, index: u64, length: u64) -> Self {
        Self {
            store,
            info_type: StoreInfoType::Content,
            index,
            length: Some(length),
            allow_miss: false,
        }
    }

    pub fn new_content_allow_miss(store: Store, index: u64, length: u64) -> Self {
        Self {
            store,
            info_type: StoreInfoType::Content,
            index,
            length: Some(length),
            allow_miss: true,
        }
    }

    pub fn new_all_content(store: Store) -> Self {
        Self {
            store,
            info_type: StoreInfoType::Content,
            index: 0,
            length: None,
            allow_miss: false,
        }
    }

    pub fn new_size(store: Store, index: u64) -> Self {
        Self {
            store,
            info_type: StoreInfoType::Size,
            index,
            length: None,
            allow_miss: false,
        }
    }
}
