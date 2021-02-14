use abomonation_derive::Abomonation;

pub type BuildResult<T> = Result<T, BuilderError>;

// TODO: Impl Display and Error, add docs
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum BuilderError {
    MissingTerminator,
    EmptyFunctionBody,
    MissingEntryBlock,
    MismatchedReturnTypes,
    MismatchedOperandTypes,
    IncorrectConditionType,
}
