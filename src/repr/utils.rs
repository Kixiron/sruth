use std::num::NonZeroU32;

use abomonation::Abomonation;
use lasso::Spur;

use crate::repr::instruction::VarId;

pub trait Cast<T> {
    fn is(&self) -> bool;
    fn cast(self) -> Option<T>;
}

pub trait InstructionExt {
    fn destination(&self) -> VarId;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Ident(Spur);

impl Abomonation for Ident {
    unsafe fn entomb<W: std::io::Write>(&self, write: &mut W) -> std::io::Result<()> {
        <NonZeroU32 as Abomonation>::entomb(&*(self as *const Self as *const NonZeroU32), write)
    }

    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        <NonZeroU32 as Abomonation>::exhume(&mut *(self as *mut Self as *mut NonZeroU32), bytes)
    }

    fn extent(&self) -> usize {
        unsafe { <NonZeroU32 as Abomonation>::extent(&*(self as *const Self as *const NonZeroU32)) }
    }
}
