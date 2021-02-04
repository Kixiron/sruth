use abomonation_derive::Abomonation;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Constant {
    Bool(bool),
    Int(i64),
    Uint(u64),
}
