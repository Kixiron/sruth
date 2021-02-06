use crate::repr::{instruction::VarId, Type};
use abomonation::Abomonation;
use lasso::{Resolver, Spur};
use pretty::{DocAllocator, DocBuilder};
use std::{marker::PhantomData, num::NonZeroU32, ops::Deref};

pub trait RawCast<T> {
    fn is_raw(&self) -> bool;
    fn cast_raw(self) -> Option<T>;
}

pub trait Cast {
    fn is<T>(&self) -> bool
    where
        Self: RawCast<T>;

    fn cast<T>(self) -> Option<T>
    where
        Self: RawCast<T>;
}

impl<U> Cast for U {
    fn is<T>(&self) -> bool
    where
        Self: RawCast<T>,
    {
        self.is_raw()
    }

    fn cast<T>(self) -> Option<T>
    where
        Self: RawCast<T>,
    {
        self.cast_raw()
    }
}

pub trait InstructionExt: IRDisplay {
    fn dest(&self) -> VarId;

    fn dest_type(&self) -> Type;
}

pub trait IRDisplay {
    fn display<'a, D, A, R>(&self, ctx: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver;
}

#[derive(Debug)]
pub struct DisplayCtx<'a, D, A, R>
where
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    A: Clone + 'a,
    R: Resolver,
{
    pub alloc: &'a D,
    pub interner: &'a R,
    __alloc: PhantomData<&'a A>,
}

impl<'a, D, A, R> DisplayCtx<'a, D, A, R>
where
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    A: Clone + 'a,
    R: Resolver,
{
    pub fn new(alloc: &'a D, interner: &'a R) -> Self {
        Self {
            alloc,
            interner,
            __alloc: PhantomData,
        }
    }
}

impl<'a, D, A, R> Deref for DisplayCtx<'a, D, A, R>
where
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    A: Clone + 'a,
    R: Resolver,
{
    type Target = &'a D;

    fn deref(&self) -> &Self::Target {
        &self.alloc
    }
}

impl<'a, D, A, R> Clone for DisplayCtx<'a, D, A, R>
where
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    A: Clone + 'a,
    R: Resolver,
{
    fn clone(&self) -> Self {
        Self {
            alloc: self.alloc,
            interner: self.interner,
            __alloc: PhantomData,
        }
    }
}

impl<'a, D, A, R> Copy for DisplayCtx<'a, D, A, R>
where
    D: DocAllocator<'a, A>,
    D::Doc: Clone,
    A: Clone + 'a,
    R: Resolver,
{
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

impl IRDisplay for Ident {
    fn display<'a, D, A, R>(&self, alloc: DisplayCtx<'a, D, A, R>) -> DocBuilder<'a, D, A>
    where
        D: DocAllocator<'a, A>,
        D::Doc: Clone,
        A: Clone + 'a,
        R: Resolver,
    {
        alloc.text(alloc.interner.resolve(&self.0))
    }
}
