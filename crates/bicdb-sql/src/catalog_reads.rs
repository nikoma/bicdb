//! Catalog admission is checked before metadata caches. Inference helpers may
//! treat unsupported metadata as unknown; a reservation refusal is never that.
use crate::*;
use std::rc::Rc;

struct Validation {
    database: u64,
    denied: Rc<RefCell<Option<String>>>,
}

thread_local! {
    static VALIDATIONS: RefCell<Vec<Validation>> = const { RefCell::new(Vec::new()) };
}

struct ValidationScope {
    denied: Rc<RefCell<Option<String>>>,
}

impl Drop for ValidationScope {
    fn drop(&mut self) {
        let _ = VALIDATIONS.try_with(|frames| {
            frames
                .borrow_mut()
                .retain(|frame| !Rc::ptr_eq(&frame.denied, &self.denied));
        });
    }
}

pub(crate) fn check_sql_catalog_read(db: &BicDb, collection: &str, id: Option<&str>) -> Result<()> {
    match db.check_catalog_read_access(collection, id) {
        Ok(()) => Ok(()),
        Err(error) => {
            if let BicDbError::TransactionConflict(message) = &error {
                VALIDATIONS.with(|frames| {
                    for frame in frames
                        .borrow()
                        .iter()
                        .filter(|frame| frame.database == db.instance_id())
                    {
                        frame
                            .denied
                            .borrow_mut()
                            .get_or_insert_with(|| message.clone());
                    }
                });
            }
            Err(error.into())
        }
    }
}

/// Preserve retryable reservation errors when a metadata-only inference path
/// intentionally converts ordinary lookup failures to `None`. No SQL is run.
pub(crate) fn validate_catalog_reads<T>(
    db: &BicDb,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let denied = Rc::new(RefCell::new(None));
    VALIDATIONS.with(|frames| {
        frames.borrow_mut().push(Validation {
            database: db.instance_id(),
            denied: Rc::clone(&denied),
        })
    });
    let _scope = ValidationScope {
        denied: Rc::clone(&denied),
    };
    let result = operation();
    if let Some(message) = denied.borrow().as_ref() {
        return Err(BicDbError::TransactionConflict(message.clone()).into());
    }
    result
}
