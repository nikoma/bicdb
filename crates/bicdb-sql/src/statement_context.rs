//! Synchronous statement context shared by nested row and routine evaluators.
use std::cell::{Cell, RefCell};
use std::rc::Rc;

thread_local! {
    static DESCRIBING: Cell<bool> = const { Cell::new(false) };
    static EFFECTS: RefCell<Option<Rc<Effects>>> = const { RefCell::new(None) };
}

#[derive(Default)]
struct Effects {
    independent_execution: Cell<bool>,
    independent_commits: Cell<bool>,
    latest_sequence: Cell<u64>,
}

/// Prevent independently committing routines from executing during metadata
/// discovery. The host must hold this scope only during synchronous Describe.
pub struct SqlDescriptionScope(bool, std::marker::PhantomData<Rc<()>>);
impl SqlDescriptionScope {
    pub fn enter() -> Self {
        Self(
            DESCRIBING.with(|c| c.replace(true)),
            std::marker::PhantomData,
        )
    }
}
impl Drop for SqlDescriptionScope {
    fn drop(&mut self) {
        DESCRIBING.with(|c| c.set(self.0));
    }
}
pub(crate) fn describing() -> bool {
    DESCRIBING.with(Cell::get)
}

pub(crate) struct StatementScope {
    effects: Rc<Effects>,
    root: bool,
}
impl StatementScope {
    pub(crate) fn enter() -> Self {
        EFFECTS.with(|current| {
            let root = current.borrow().is_none();
            if root {
                *current.borrow_mut() = Some(Rc::new(Effects::default()));
            }
            Self {
                effects: current.borrow().as_ref().unwrap().clone(),
                root,
            }
        })
    }
    pub(crate) fn independently_committed(&self) -> bool {
        self.effects.independent_commits.get()
    }
    pub(crate) fn independent_execution(&self) -> bool {
        self.effects.independent_execution.get()
    }
    pub(crate) fn latest_sequence(&self) -> u64 {
        self.effects.latest_sequence.get()
    }
}
impl Drop for StatementScope {
    fn drop(&mut self) {
        if self.root {
            EFFECTS.with(|c| {
                c.borrow_mut().take();
            });
        }
    }
}
pub(crate) fn note_independent_commit(sequence: u64) {
    EFFECTS.with(|c| {
        if let Some(e) = c.borrow().as_ref() {
            e.independent_commits.set(true);
            e.latest_sequence.set(e.latest_sequence.get().max(sequence));
        }
    });
}
pub(crate) fn note_independent_execution() {
    EFFECTS.with(|c| {
        if let Some(e) = c.borrow().as_ref() {
            e.independent_execution.set(true);
        }
    });
}
pub(crate) fn latest_independent_commit() -> u64 {
    EFFECTS.with(|c| c.borrow().as_ref().map_or(0, |e| e.latest_sequence.get()))
}
