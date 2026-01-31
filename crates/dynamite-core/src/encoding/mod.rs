pub mod binary;
pub mod composite;
pub mod number;
pub mod string;

/// A key value that can be encoded for byte-ordered comparison.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyValue {
    String(String),
    Number(f64),
    Binary(Vec<u8>),
}
