//! A settled title derivation in the metadata ledger.

use super::Agency;
use super::projection::Projection;

/// A settled title derivation in the metadata ledger: no ask, inert — a pure
/// record. It settles exactly ONE request, the earliest still outstanding
/// before it, and it carries no reference to that request: the pairing is
/// positional, and
/// [`MetadataTitleRequest::settled_in`](super::MetadataTitleRequest::settled_in)
/// is where it is decided. Doneness is read off the ledger rather than held
/// anywhere.
#[derive(Debug, Clone, Copy)]
pub struct MetadataTitleResponse;

impl Agency for MetadataTitleResponse {}

/// Lives in the metadata ledger, which never enters the neutral pass —
/// invisible by inertness, with nothing special-cased to keep it out.
impl Projection for MetadataTitleResponse {}
