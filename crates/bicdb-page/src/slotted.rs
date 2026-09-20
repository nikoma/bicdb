//! Slotted heap pages.
//!
//! Phase 1 of `docs/server-paged-storage-todo.md`: "Implement slotted heap pages
//! so variable-size records can move within a page without changing their stable
//! `(page_id, slot_id, generation)` locator."
//!
//! # Layout
//!
//! ```text
//! +----------------------+  0
//! | page header (64 B)   |
//! +----------------------+  free_start grows down ->
//! | slot directory       |
//! |   slot 0: off,len,gen|
//! |   slot 1: ...        |
//! +----------------------+
//! |    (free space)      |
//! +----------------------+  <- free_end, tuple data grows up
//! | tuple data           |
//! +----------------------+  page_size - 8
//! | trailer (8 B)        |
//! +----------------------+
//! ```
//!
//! The slot directory grows forward from the header; tuple data grows backward
//! from the trailer. They meet in the middle, and the gap between `free_start`
//! and `free_end` is the free space.
//!
//! # Why the generation is part of the locator
//!
//! A slot number alone is not a safe identity. Delete a record, insert a
//! different one, and the new record can land in the recycled slot — at which
//! point a stale index entry or a long-running snapshot pointing at
//! `(page, slot)` would silently resolve to *the wrong row*. That is the
//! roadmap's "free-space map and reuse policy that cannot expose stale tuple
//! data after slot reuse", and a monotonically increasing per-slot generation is
//! what enforces it: [`SlottedPage::get`] rejects a locator whose generation no
//! longer matches instead of returning whatever now occupies the slot.
//!
//! Compaction, by contrast, moves tuple *bytes* within the page and rewrites
//! offsets, but never touches slot numbers or generations. Locators stay valid
//! across it, which is the entire point of the indirection.

use crate::error::{PageError, Result};
use crate::page::{PageHeader, PageId, PageType, PAGE_HEADER_BYTES, PAGE_TRAILER_BYTES};

/// Bytes per slot-directory entry: offset u16, length u16, generation u32.
pub const SLOT_BYTES: usize = 8;

/// A slot's offset value meaning "no tuple here".
const DEAD_OFFSET: u16 = 0;

/// A stable pointer to one tuple.
///
/// This is the server-paged analogue of the in-memory `RowId`, and unlike it,
/// it is durable: index entries and version chains can persist a
/// `TupleLocator` across restarts.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct TupleLocator {
    pub page_id: PageId,
    pub slot: u16,
    pub generation: u32,
}

impl TupleLocator {
    pub fn new(page_id: PageId, slot: u16, generation: u32) -> Self {
        Self {
            page_id,
            slot,
            generation,
        }
    }

    /// Pack into 16 bytes for storage in an index entry.
    pub fn encode(&self) -> [u8; 14] {
        let mut out = [0u8; 14];
        out[0..8].copy_from_slice(&self.page_id.to_le_bytes());
        out[8..10].copy_from_slice(&self.slot.to_le_bytes());
        out[10..14].copy_from_slice(&self.generation.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 14 {
            return None;
        }
        Some(Self {
            page_id: u64::from_le_bytes(bytes[0..8].try_into().ok()?),
            slot: u16::from_le_bytes(bytes[8..10].try_into().ok()?),
            generation: u32::from_le_bytes(bytes[10..14].try_into().ok()?),
        })
    }
}

/// A read/write view over a heap page buffer.
///
/// Borrows the buffer rather than owning it, so it composes with a
/// [`crate::PageGuardMut`] from the buffer pool without copying a page.
#[derive(Debug)]
pub struct SlottedPage<'a> {
    bytes: &'a mut [u8],
}

/// A read-only view over a heap page buffer.
#[derive(Debug)]
pub struct SlottedPageRef<'a> {
    bytes: &'a [u8],
}

macro_rules! slot_accessors {
    ($ty:ty) => {
        impl $ty {
            #[inline]
            fn read_u16(&self, at: usize) -> u16 {
                u16::from_le_bytes([self.bytes[at], self.bytes[at + 1]])
            }

            #[inline]
            fn slot_count(&self) -> u16 {
                self.read_u16(40)
            }

            #[inline]
            fn free_start(&self) -> usize {
                self.read_u16(36) as usize
            }

            #[inline]
            fn free_end(&self) -> usize {
                self.read_u16(38) as usize
            }

            #[inline]
            fn slot_offset(&self, slot: u16) -> usize {
                PAGE_HEADER_BYTES + slot as usize * SLOT_BYTES
            }

            fn slot_entry(&self, slot: u16) -> Result<(u16, u16, u32)> {
                let count = self.slot_count();
                if slot >= count {
                    return Err(PageError::NoSuchSlot {
                        page_id: self.page_id(),
                        slot,
                        slot_count: count,
                    });
                }
                let at = self.slot_offset(slot);
                Ok((
                    self.read_u16(at),
                    self.read_u16(at + 2),
                    u32::from_le_bytes(self.bytes[at + 4..at + 8].try_into().unwrap()),
                ))
            }

            /// This page's id, from its header.
            pub fn page_id(&self) -> PageId {
                u64::from_le_bytes(self.bytes[8..16].try_into().unwrap())
            }

            /// Live (non-deleted) tuples on this page.
            pub fn live_count(&self) -> u16 {
                (0..self.slot_count())
                    .filter(|slot| {
                        self.slot_entry(*slot)
                            .map(|(offset, _, _)| offset != DEAD_OFFSET)
                            .unwrap_or(false)
                    })
                    .count() as u16
            }

            /// Total slots ever allocated, live or dead.
            pub fn slots(&self) -> u16 {
                self.slot_count()
            }

            /// Highest slot generation on this page, dead slots included.
            /// This is the page's generation FLOOR contribution when it is
            /// freed: any future tenancy must mint strictly above it.
            pub fn max_generation(&self) -> u32 {
                (0..self.slot_count())
                    .filter_map(|slot| self.slot_entry(slot).ok())
                    .map(|(_, _, generation)| generation)
                    .max()
                    .unwrap_or(0)
            }

            /// Contiguous free bytes between the slot directory and tuple data.
            pub fn free_space(&self) -> usize {
                self.free_end().saturating_sub(self.free_start())
            }

            /// Free bytes including space held by dead tuples, which
            /// [`SlottedPage::compact`] would reclaim.
            pub fn reclaimable_space(&self) -> usize {
                let dead: usize = (0..self.slot_count())
                    .filter_map(|slot| self.slot_entry(slot).ok())
                    .filter(|(offset, _, _)| *offset == DEAD_OFFSET)
                    .map(|(_, len, _)| len as usize)
                    .sum();
                self.free_space() + dead
            }

            /// Read a tuple through a locator, rejecting a stale one.
            pub fn get(&self, locator: TupleLocator) -> Result<&[u8]> {
                let (offset, len, generation) = self.slot_entry(locator.slot)?;
                if offset == DEAD_OFFSET {
                    return Err(PageError::DeadSlot {
                        page_id: self.page_id(),
                        slot: locator.slot,
                    });
                }
                if generation != locator.generation {
                    return Err(PageError::StaleLocator {
                        page_id: self.page_id(),
                        slot: locator.slot,
                        expected: locator.generation as u64,
                        found: generation as u64,
                    });
                }
                Ok(&self.bytes[offset as usize..offset as usize + len as usize])
            }

            /// Read by slot number without generation validation.
            ///
            /// For maintenance passes that walk a page's contents (compaction,
            /// verification, recovery) and legitimately want whatever is there.
            /// Never use it to resolve a locator — that is exactly the stale-read
            /// hole the generation exists to close.
            pub fn get_by_slot(&self, slot: u16) -> Result<&[u8]> {
                let (offset, len, _) = self.slot_entry(slot)?;
                if offset == DEAD_OFFSET {
                    return Err(PageError::DeadSlot {
                        page_id: self.page_id(),
                        slot,
                    });
                }
                Ok(&self.bytes[offset as usize..offset as usize + len as usize])
            }

            /// The current generation of a slot, if it is live.
            pub fn generation_of(&self, slot: u16) -> Result<u32> {
                let (offset, _, generation) = self.slot_entry(slot)?;
                if offset == DEAD_OFFSET {
                    return Err(PageError::DeadSlot {
                        page_id: self.page_id(),
                        slot,
                    });
                }
                Ok(generation)
            }

            /// Locators for every live tuple, in slot order.
            pub fn locators(&self) -> Vec<TupleLocator> {
                (0..self.slot_count())
                    .filter_map(|slot| {
                        let (offset, _, generation) = self.slot_entry(slot).ok()?;
                        (offset != DEAD_OFFSET)
                            .then(|| TupleLocator::new(self.page_id(), slot, generation))
                    })
                    .collect()
            }
        }
    };
}

slot_accessors!(SlottedPage<'_>);
slot_accessors!(SlottedPageRef<'_>);

impl<'a> SlottedPageRef<'a> {
    /// View an existing heap page. Does not validate the checksum — the page
    /// store did that on read.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

impl<'a> SlottedPage<'a> {
    /// View an existing heap page for modification.
    pub fn new(bytes: &'a mut [u8]) -> Self {
        Self { bytes }
    }

    /// Initialize a buffer as an empty heap page.
    pub fn init(bytes: &'a mut [u8], page_id: PageId, page_size: u32) -> Self {
        Self::init_with_floor(bytes, page_id, page_size, 0)
    }

    /// Initialize with a GENERATION FLOOR: every slot this page ever mints
    /// starts above `floor`. The floor is the page-reuse extension of the
    /// B11b defense — when a fully-dead page is freed and later re-allocated
    /// as heap, its old slot generations are gone with the directory; without
    /// a floor the new tenancy would restart at generation 1 and a stale
    /// locator from the FIRST tenancy could validate against a new tuple.
    /// The caller (heap allocation) reads the floor from the durable catalog,
    /// where it survives any intervening tenancies of the page id.
    ///
    /// Stored in the header's `payload_len` field, which heap pages do not
    /// otherwise use (it is the overflow chunk size on overflow pages).
    pub fn init_with_floor(
        bytes: &'a mut [u8],
        page_id: PageId,
        page_size: u32,
        floor: u32,
    ) -> Self {
        bytes.fill(0);
        let mut header = PageHeader::new(page_id, PageType::Heap, page_size);
        header.payload_len = floor;
        header.encode(bytes);
        Self { bytes }
    }

    #[inline]
    fn generation_floor(&self) -> u32 {
        u32::from_le_bytes(self.bytes[52..56].try_into().unwrap())
    }

    #[inline]
    fn write_u16(&mut self, at: usize, value: u16) {
        self.bytes[at..at + 2].copy_from_slice(&value.to_le_bytes());
    }

    #[inline]
    fn set_free_start(&mut self, value: usize) {
        self.write_u16(36, value as u16);
    }

    #[inline]
    fn set_free_end(&mut self, value: usize) {
        self.write_u16(38, value as u16);
    }

    #[inline]
    fn set_slot_count(&mut self, value: u16) {
        self.write_u16(40, value);
    }

    fn write_slot(&mut self, slot: u16, offset: u16, len: u16, generation: u32) {
        let at = self.slot_offset(slot);
        self.write_u16(at, offset);
        self.write_u16(at + 2, len);
        self.bytes[at + 4..at + 8].copy_from_slice(&generation.to_le_bytes());
    }

    /// Bytes needed to store a value of `len` in a fresh slot.
    pub fn required_space(len: usize) -> usize {
        len + SLOT_BYTES
    }

    /// Whether a value of `len` fits without compaction.
    pub fn can_fit(&self, len: usize) -> bool {
        // A recyclable dead slot avoids the directory cost.
        let needs_new_slot = self.find_dead_slot().is_none();
        let required = len + if needs_new_slot { SLOT_BYTES } else { 0 };
        self.free_space() >= required
    }

    fn find_dead_slot(&self) -> Option<u16> {
        (0..self.slot_count()).find(|slot| {
            self.slot_entry(*slot)
                .map(|(offset, _, _)| offset == DEAD_OFFSET)
                .unwrap_or(false)
        })
    }

    /// Insert a tuple, returning its durable locator.
    ///
    /// Reuses a dead slot when one exists, bumping its generation so that any
    /// locator still pointing at the previous occupant is detectably stale.
    pub fn insert(&mut self, value: &[u8]) -> Result<TupleLocator> {
        if value.is_empty() {
            // A zero-length tuple would be indistinguishable from a dead slot,
            // since DEAD_OFFSET is encoded as offset 0.
            return Err(PageError::ValueTooLarge {
                page_id: self.page_id(),
                len: 0,
                free: self.free_space(),
            });
        }
        let recycled = self.find_dead_slot();
        let needed = value.len() + if recycled.is_none() { SLOT_BYTES } else { 0 };
        if self.free_space() < needed {
            return Err(PageError::ValueTooLarge {
                page_id: self.page_id(),
                len: value.len(),
                free: self.free_space(),
            });
        }

        let data_offset = self.free_end() - value.len();
        self.bytes[data_offset..data_offset + value.len()].copy_from_slice(value);
        self.set_free_end(data_offset);

        let (slot, generation) = match recycled {
            Some(slot) => {
                let (_, _, previous) = self.slot_entry(slot)?;
                // Wrapping is astronomically unlikely (2^32 reuses of one slot)
                // but must not alias a live generation, so it wraps rather than
                // saturating: saturation would make every later reuse
                // indistinguishable.
                (slot, previous.wrapping_add(1))
            }
            None => {
                let slot = self.slot_count();
                self.set_slot_count(slot + 1);
                self.set_free_start(self.free_start() + SLOT_BYTES);
                // Above the page's floor: 1 on a first tenancy, past every
                // prior tenancy's generations on a reused page.
                (slot, self.generation_floor().wrapping_add(1))
            }
        };

        self.write_slot(slot, data_offset as u16, value.len() as u16, generation);
        Ok(TupleLocator::new(self.page_id(), slot, generation))
    }

    /// Mark a tuple dead. Its space is reclaimed by [`Self::compact`].
    ///
    /// The generation is deliberately *not* bumped here. Bumping happens when
    /// the slot is reused, which is the moment a stale locator could otherwise
    /// resolve to a different row. Bumping on delete as well would be harmless
    /// but would make "deleted" and "replaced" indistinguishable to a reader
    /// holding an old locator, and they warrant different errors.
    pub fn delete(&mut self, locator: TupleLocator) -> Result<()> {
        let (offset, len, generation) = self.slot_entry(locator.slot)?;
        if offset == DEAD_OFFSET {
            return Err(PageError::DeadSlot {
                page_id: self.page_id(),
                slot: locator.slot,
            });
        }
        if generation != locator.generation {
            return Err(PageError::StaleLocator {
                page_id: self.page_id(),
                slot: locator.slot,
                expected: locator.generation as u64,
                found: generation as u64,
            });
        }
        // Zero the tuple bytes so a page image cannot leak deleted content, then
        // keep the length so compaction knows how much to reclaim.
        self.bytes[offset as usize..offset as usize + len as usize].fill(0);
        self.write_slot(locator.slot, DEAD_OFFSET, len, generation);
        Ok(())
    }

    /// Replace a tuple's contents, keeping its locator valid.
    ///
    /// Updates in place when the new value is no larger; otherwise the tuple
    /// moves within the page and the slot's offset is rewritten. Either way the
    /// slot and generation are unchanged, so persisted index entries stay valid
    /// — this is exactly what the slot indirection buys.
    pub fn update(&mut self, locator: TupleLocator, value: &[u8]) -> Result<()> {
        let (offset, len, generation) = self.slot_entry(locator.slot)?;
        if offset == DEAD_OFFSET {
            return Err(PageError::DeadSlot {
                page_id: self.page_id(),
                slot: locator.slot,
            });
        }
        if generation != locator.generation {
            return Err(PageError::StaleLocator {
                page_id: self.page_id(),
                slot: locator.slot,
                expected: locator.generation as u64,
                found: generation as u64,
            });
        }
        if value.is_empty() {
            return Err(PageError::ValueTooLarge {
                page_id: self.page_id(),
                len: 0,
                free: self.free_space(),
            });
        }

        if value.len() <= len as usize {
            let offset = offset as usize;
            self.bytes[offset..offset + value.len()].copy_from_slice(value);
            // Zero any tail left over from the longer previous value.
            self.bytes[offset + value.len()..offset + len as usize].fill(0);
            self.write_slot(locator.slot, offset as u16, value.len() as u16, generation);
            return Ok(());
        }

        if self.free_space() < value.len() {
            return Err(PageError::ValueTooLarge {
                page_id: self.page_id(),
                len: value.len(),
                free: self.free_space(),
            });
        }
        self.bytes[offset as usize..offset as usize + len as usize].fill(0);
        let data_offset = self.free_end() - value.len();
        self.bytes[data_offset..data_offset + value.len()].copy_from_slice(value);
        self.set_free_end(data_offset);
        self.write_slot(
            locator.slot,
            data_offset as u16,
            value.len() as u16,
            generation,
        );
        Ok(())
    }

    /// Reclaim space held by deleted tuples by sliding live tuples together.
    ///
    /// Slot numbers and generations are preserved, so every outstanding locator
    /// remains valid. Returns the bytes reclaimed.
    pub fn compact(&mut self, page_size: u32) -> Result<usize> {
        let before = self.free_space();
        let slot_count = self.slot_count();

        // Collect live tuples, then rewrite them from the end of the page.
        let mut live: Vec<(u16, Vec<u8>, u32)> = Vec::new();
        for slot in 0..slot_count {
            let (offset, len, generation) = self.slot_entry(slot)?;
            if offset == DEAD_OFFSET {
                continue;
            }
            live.push((
                slot,
                self.bytes[offset as usize..offset as usize + len as usize].to_vec(),
                generation,
            ));
        }

        let data_top = page_size as usize - PAGE_TRAILER_BYTES;
        let directory_end = PAGE_HEADER_BYTES + slot_count as usize * SLOT_BYTES;
        self.bytes[directory_end..data_top].fill(0);

        let mut cursor = data_top;
        for (slot, value, generation) in &live {
            cursor -= value.len();
            self.bytes[cursor..cursor + value.len()].copy_from_slice(value);
            self.write_slot(*slot, cursor as u16, value.len() as u16, *generation);
        }
        self.set_free_end(cursor);

        // Trailing dead slots are deliberately KEPT (B11b). Dropping them
        // discards their generation history: a later append re-creates the
        // same slot number at generation 1, and a stale locator from the
        // slot's FIRST occupancy (also generation 1) then validates against
        // a different tuple — a silent wrong-row read. The dead entries cost
        // SLOT_BYTES each and are recycled by `insert`'s dead-slot reuse,
        // which bumps the generation monotonically — that bump is the whole
        // stale-locator defense, and it only works if the directory
        // remembers the last generation.
        Ok(self.free_space().saturating_sub(before))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::DEFAULT_PAGE_SIZE;

    fn page(page_size: u32) -> Vec<u8> {
        vec![0u8; page_size as usize]
    }

    #[test]
    fn insert_and_read_back() {
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 5, DEFAULT_PAGE_SIZE);

        let a = heap.insert(b"first").unwrap();
        let b = heap.insert(b"second value").unwrap();

        assert_eq!(heap.get(a).unwrap(), b"first");
        assert_eq!(heap.get(b).unwrap(), b"second value");
        assert_eq!(a.page_id, 5);
        assert_eq!(a.slot, 0);
        assert_eq!(b.slot, 1);
        assert_eq!(heap.live_count(), 2);
    }

    #[test]
    fn a_page_fills_up_and_says_so_rather_than_overflowing() {
        let mut buffer = page(512);
        let mut heap = SlottedPage::init(&mut buffer, 1, 512);
        let value = vec![b'x'; 64];

        let mut inserted = 0;
        loop {
            match heap.insert(&value) {
                Ok(_) => inserted += 1,
                Err(PageError::ValueTooLarge { .. }) => break,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
            assert!(inserted < 100, "page never filled");
        }
        assert!(inserted > 0);
        // Directory and data must not have crossed.
        assert!(heap.free_start() <= heap.free_end());
    }

    #[test]
    fn deleted_tuples_are_unreadable_and_their_bytes_are_zeroed() {
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 1, DEFAULT_PAGE_SIZE);
        let locator = heap.insert(b"CONFIDENTIAL").unwrap();
        heap.delete(locator).unwrap();

        assert!(matches!(heap.get(locator), Err(PageError::DeadSlot { .. })));
        assert_eq!(heap.live_count(), 0);
        assert!(
            !buffer.windows(12).any(|w| w == b"CONFIDENTIAL"),
            "deleted tuple bytes survived in the page image"
        );
    }

    #[test]
    fn a_recycled_slot_rejects_the_previous_occupants_locator() {
        // The reason the generation is in the locator at all. Without it, the
        // stale locator below would silently return the NEW row's bytes.
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 1, DEFAULT_PAGE_SIZE);

        let old = heap.insert(b"original row").unwrap();
        heap.delete(old).unwrap();
        let new = heap.insert(b"different row").unwrap();

        assert_eq!(new.slot, old.slot, "slot should have been recycled");
        assert_ne!(new.generation, old.generation);

        match heap.get(old) {
            Err(PageError::StaleLocator {
                expected, found, ..
            }) => {
                assert_eq!(expected, old.generation as u64);
                assert_eq!(found, new.generation as u64);
            }
            other => panic!("stale locator resolved instead of failing: {other:?}"),
        }
        assert_eq!(heap.get(new).unwrap(), b"different row");
    }

    #[test]
    fn deleting_through_a_stale_locator_does_not_delete_the_new_row() {
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 1, DEFAULT_PAGE_SIZE);
        let old = heap.insert(b"first").unwrap();
        heap.delete(old).unwrap();
        let new = heap.insert(b"second").unwrap();

        assert!(matches!(
            heap.delete(old),
            Err(PageError::StaleLocator { .. })
        ));
        assert_eq!(heap.get(new).unwrap(), b"second");
    }

    #[test]
    fn update_in_place_keeps_the_locator_valid() {
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 1, DEFAULT_PAGE_SIZE);
        let locator = heap.insert(b"original value").unwrap();

        heap.update(locator, b"shorter").unwrap();
        assert_eq!(heap.get(locator).unwrap(), b"shorter");
        assert!(
            !buffer.windows(8).any(|w| w == b"al value"),
            "the tail of the longer previous value was left behind"
        );
    }

    #[test]
    fn a_growing_update_moves_the_tuple_but_not_its_locator() {
        // The property the slot indirection exists for: persisted index entries
        // must survive a record growing.
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 1, DEFAULT_PAGE_SIZE);
        let locator = heap.insert(b"small").unwrap();
        let neighbour = heap.insert(b"neighbour").unwrap();

        let bigger = vec![b'z'; 400];
        heap.update(locator, &bigger).unwrap();

        assert_eq!(heap.get(locator).unwrap(), &bigger[..]);
        assert_eq!(
            heap.get(neighbour).unwrap(),
            b"neighbour",
            "moving one tuple corrupted another"
        );
    }

    #[test]
    fn compaction_reclaims_dead_space_and_preserves_every_locator() {
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 1, DEFAULT_PAGE_SIZE);

        let mut locators = Vec::new();
        for index in 0..20 {
            locators.push(
                heap.insert(format!("row-{index:04}-{}", "p".repeat(50)).as_bytes())
                    .unwrap(),
            );
        }
        // Delete every other row, fragmenting the page.
        for locator in locators.iter().step_by(2) {
            heap.delete(*locator).unwrap();
        }

        let free_before = heap.free_space();
        let reclaimable = heap.reclaimable_space();
        assert!(
            reclaimable > free_before,
            "nothing to reclaim; test is inert"
        );

        heap.compact(DEFAULT_PAGE_SIZE).unwrap();
        assert!(
            heap.free_space() > free_before,
            "compaction reclaimed nothing"
        );

        // Every surviving locator still resolves to its own value.
        for (index, locator) in locators.iter().enumerate() {
            if index % 2 == 0 {
                continue;
            }
            let expected = format!("row-{index:04}-{}", "p".repeat(50));
            assert_eq!(
                heap.get(*locator).unwrap(),
                expected.as_bytes(),
                "locator {locator:?} broke across compaction"
            );
        }
    }

    #[test]
    fn compaction_leaves_no_trace_of_deleted_rows() {
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 1, DEFAULT_PAGE_SIZE);
        let doomed = heap.insert(b"TOP-SECRET-PAYLOAD").unwrap();
        heap.insert(b"kept").unwrap();
        heap.delete(doomed).unwrap();
        heap.compact(DEFAULT_PAGE_SIZE).unwrap();

        assert!(
            !buffer.windows(18).any(|w| w == b"TOP-SECRET-PAYLOAD"),
            "compacted page still contains deleted data"
        );
    }

    #[test]
    fn compaction_keeps_trailing_dead_slots_and_their_generations() {
        // B11b: dropping trailing dead slots discards generation history — a
        // later append re-creates the slot number at generation 1, and a
        // stale locator from the slot's FIRST occupancy (also generation 1)
        // validates against a different tuple: a silent wrong-row read.
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 1, DEFAULT_PAGE_SIZE);
        let keep = heap.insert(b"keep").unwrap();
        let a = heap.insert(b"drop-a").unwrap();
        let b = heap.insert(b"drop-b").unwrap();
        heap.delete(a).unwrap();
        heap.delete(b).unwrap();

        assert_eq!(heap.slots(), 3);
        heap.compact(DEFAULT_PAGE_SIZE).unwrap();
        assert_eq!(heap.slots(), 3, "dead slots must survive compaction");
        assert_eq!(heap.get(keep).unwrap(), b"keep");

        // Reuse bumps generations past the dead occupants'.
        let reused = heap.insert(b"new-tenant").unwrap();
        assert_eq!(reused.slot, a.slot, "dead slot should be recycled");
        assert!(
            reused.generation > a.generation,
            "recycled generation must move forward"
        );
        // The stale locator from the first occupancy must NOT read the new
        // tenant.
        assert!(matches!(heap.get(a), Err(PageError::StaleLocator { .. })));
    }

    #[test]
    fn compaction_does_not_renumber_slots_before_a_live_tuple() {
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 1, DEFAULT_PAGE_SIZE);
        let dead = heap.insert(b"dead").unwrap();
        let live = heap.insert(b"live").unwrap();
        heap.delete(dead).unwrap();
        heap.compact(DEFAULT_PAGE_SIZE).unwrap();

        assert_eq!(live.slot, 1);
        assert_eq!(
            heap.get(live).unwrap(),
            b"live",
            "a leading dead slot must not shift later slot numbers"
        );
    }

    #[test]
    fn zero_length_tuples_are_rejected() {
        // A zero-length tuple is indistinguishable from a dead slot in this
        // encoding, so it must be refused rather than stored ambiguously.
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let mut heap = SlottedPage::init(&mut buffer, 1, DEFAULT_PAGE_SIZE);
        assert!(heap.insert(b"").is_err());
        let locator = heap.insert(b"x").unwrap();
        assert!(heap.update(locator, b"").is_err());
    }

    #[test]
    fn locators_encode_and_decode_round_trip() {
        let locator = TupleLocator::new(u64::MAX / 7, 4242, u32::MAX - 3);
        let encoded = locator.encode();
        assert_eq!(TupleLocator::decode(&encoded).unwrap(), locator);
        assert!(TupleLocator::decode(&encoded[..5]).is_none());
    }

    #[test]
    fn a_read_only_view_sees_the_same_tuples() {
        let mut buffer = page(DEFAULT_PAGE_SIZE);
        let locator = {
            let mut heap = SlottedPage::init(&mut buffer, 9, DEFAULT_PAGE_SIZE);
            heap.insert(b"visible").unwrap()
        };
        let view = SlottedPageRef::new(&buffer);
        assert_eq!(view.get(locator).unwrap(), b"visible");
        assert_eq!(view.page_id(), 9);
        assert_eq!(view.locators(), vec![locator]);
    }

    #[test]
    fn interleaved_operations_keep_the_page_self_consistent() {
        // A small deterministic workload mixing every operation, checking the
        // page's own invariants after each step. Catches offset arithmetic that
        // only breaks in combination.
        let mut buffer = page(2048);
        let mut heap = SlottedPage::init(&mut buffer, 1, 2048);
        let mut live: Vec<(TupleLocator, Vec<u8>)> = Vec::new();

        for round in 0..60u32 {
            let value = vec![(round % 251) as u8; (round as usize % 40) + 1];
            match round % 4 {
                0 | 1 => {
                    if heap.can_fit(value.len()) {
                        if let Ok(locator) = heap.insert(&value) {
                            live.push((locator, value));
                        }
                    }
                }
                2 => {
                    if !live.is_empty() {
                        let (locator, _) = live.remove(round as usize % live.len());
                        heap.delete(locator).unwrap();
                    }
                }
                _ => {
                    if !live.is_empty() {
                        let index = round as usize % live.len();
                        let (locator, _) = live[index];
                        if heap.update(locator, &value).is_ok() {
                            live[index].1 = value;
                        }
                    }
                }
            }

            assert!(
                heap.free_start() <= heap.free_end(),
                "directory crossed data"
            );
            assert_eq!(heap.live_count() as usize, live.len());
            for (locator, expected) in &live {
                assert_eq!(heap.get(*locator).unwrap(), &expected[..]);
            }
        }

        heap.compact(2048).unwrap();
        for (locator, expected) in &live {
            assert_eq!(
                heap.get(*locator).unwrap(),
                &expected[..],
                "compaction after a mixed workload broke a locator"
            );
        }
    }
}
