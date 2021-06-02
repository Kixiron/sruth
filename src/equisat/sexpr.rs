use std::collections::HashSet;

use lalrpop_util::lalrpop_mod;

lalrpop_mod!(pub sexpr, "/equisat/sexpr.rs");

pub use sexpr::RewriteParser;

#[derive(Debug, Clone, PartialEq, PartialOrd, Hash)]
pub struct Rewrite {
    pub left: Term,
    pub right: Term,
}

impl Rewrite {
    pub const fn new(left: Term, right: Term) -> Self {
        Self { left, right }
    }

    pub fn evaluate(self) -> DeltaJoin {
        assert!(self
            .right
            .declared_vars()
            .is_subset(&self.left.declared_vars()));

        todo!()
    }

    pub fn declared_vars(&self) -> HashSet<String> {
        let mut vars = HashSet::new();
        vars.extend(self.left.declared_vars());
        vars.extend(self.right.declared_vars());
        vars
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Hash)]
pub enum Term {
    Var(String),
    Ident(String),
    Int(i64),
    Bool(bool),
    List(Vec<Self>),
}

impl Term {
    fn declared_vars(&self) -> HashSet<String> {
        match self {
            Self::Ident(var) => {
                let mut vars = HashSet::new();
                vars.insert(var.to_owned());
                vars
            }

            Self::List(list) => {
                let mut vars = HashSet::new();
                for term in list {
                    vars.extend(term.declared_vars());
                }

                vars
            }

            _ => HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct DeltaJoin {
    required_arrangements: Vec<Arrangement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum Arrangement {
    AddById,
    AddByRhs,
    AddByLhs,
    SubById,
    SubByRhs,
    SubByLhs,
}

#[test]
fn add_rewrite() {
    let rewrite = "(add ?x (sub ?x ?y)) => ?y";
    let parsed = dbg!(RewriteParser::new().parse(rewrite).unwrap());
}
