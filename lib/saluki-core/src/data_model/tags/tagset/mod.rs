pub(crate) mod frozen;

mod owned;
pub(crate) use self::owned::TagSetIter;
pub use self::owned::TagSet;

mod shared;
pub use self::shared::{SharedTagSet, SharedTagSetIterator};
