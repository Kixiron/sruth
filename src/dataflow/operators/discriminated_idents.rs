use abomonation::Abomonation;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use differential_dataflow::{
    algorithms::identifiers::Identifiers, difference::Abelian, lattice::Lattice, Collection,
    ExchangeData,
};
use std::{
    fmt::{self, Debug, Display},
    hash::{Hash, Hasher},
    io, mem,
};
use timely::dataflow::Scope;

pub trait DiscriminatedIdents<Discriminant, Ident> {
    type Output;

    fn discriminated_idents(&self, discriminant: Discriminant) -> Self::Output;
}

impl<S, D, R, Discriminant, Ident> DiscriminatedIdents<Discriminant, Ident> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData + Hash,
    R: Abelian + ExchangeData,
    Discriminant: Display + Clone + 'static,
    Ident: Identifier<Discriminant = Discriminant> + Clone + 'static,
{
    type Output = Collection<S, (D, Ident), R>;

    fn discriminated_idents(&self, discriminant: Discriminant) -> Self::Output {
        tracing::debug!(
            discriminant = %discriminant,
            "initializing a discriminated idents producer",
        );

        self.identifiers()
            .map(move |(data, hash)| (data, Ident::new_ident(discriminant.clone(), hash)))
    }
}

pub trait Identifier {
    type Discriminant;

    fn new_ident(discriminant: Self::Discriminant, hash: u64) -> Self;
}

#[derive(Clone, Copy, PartialOrd, Ord)]
pub struct Uuid {
    discriminant: u8,
    hash: u64,
}

impl Uuid {
    pub const fn new(discriminant: u8, hash: u64) -> Self {
        Self { discriminant, hash }
    }

    const fn from_u128(uuid: u128) -> Self {
        Self::from_le_bytes(uuid.to_le_bytes())
    }

    #[allow(clippy::many_single_char_names)]
    const fn from_le_bytes(bytes: [u8; mem::size_of::<u128>()]) -> Self {
        let [a, _, _, _, _, _, _, _, b, c, d, e, f, g, h, i] = bytes;

        Self {
            discriminant: u8::from_le_bytes([a]),
            hash: u64::from_le_bytes([b, c, d, e, f, g, h, i]),
        }
    }

    const fn as_u128(&self) -> u128 {
        u128::from_le_bytes(self.to_le_bytes())
    }

    #[allow(clippy::many_single_char_names)]
    const fn to_le_bytes(&self) -> [u8; mem::size_of::<u128>()] {
        let [a] = self.discriminant.to_le_bytes();
        let [b, c, d, e, f, g, h, i] = self.hash.to_le_bytes();

        [a, 0, 0, 0, 0, 0, 0, 0, b, c, d, e, f, g, h, i]
    }
}

impl Identifier for Uuid {
    type Discriminant = u8;

    fn new_ident(discriminant: Self::Discriminant, hash: u64) -> Self {
        Self::new(discriminant, hash)
    }
}

impl Debug for Uuid {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:#02X}{:08X}", self.discriminant, self.hash)
    }
}

impl Display for Uuid {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{:#02X}{:08X}", self.discriminant, self.hash)
    }
}

impl PartialEq for Uuid {
    fn eq(&self, other: &Uuid) -> bool {
        self.as_u128() == other.as_u128()
    }
}

impl Eq for Uuid {}

impl Hash for Uuid {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u128(self.as_u128());
    }
}

impl Abomonation for Uuid {
    unsafe fn entomb<W: io::Write>(&self, write: &mut W) -> io::Result<()> {
        write.write_u128::<LittleEndian>(self.as_u128())
    }

    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        *self = Self::from_u128((&*bytes).read_u128::<LittleEndian>().ok()?);
        Some(&mut bytes[mem::size_of::<u128>()..])
    }

    fn extent(&self) -> usize {
        mem::size_of::<u128>()
    }
}
