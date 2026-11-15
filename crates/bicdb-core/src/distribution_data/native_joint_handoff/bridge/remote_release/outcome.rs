//! Bounded attribution of actual API returns. Neither bytes nor success/error
//! history resolve the actual release owner or provide a current grant.
use super::*;
use sha2::{Digest, Sha256};
use std::fmt::Write;

#[derive(Default)]
pub(super) struct RetainedReturns {
    count: u64,
    chain: [u8; 32],
    overflow: bool,
    last_ok: Option<bool>,
    first_error: Option<Failure>,
}
struct Failure {
    kind: String,
    prefix: String,
    prefix_complete: bool,
    formatted_complete: bool,
    sha256: [u8; 32],
}
impl Failure {
    fn retain(error: &BicDbError) -> Self {
        struct Bounded {
            prefix: String,
            complete: bool,
            hash: Sha256,
        }
        impl std::fmt::Write for Bounded {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                self.hash.update(text.as_bytes());
                if self.complete {
                    let mut end = (4096 - self.prefix.len()).min(text.len());
                    while !text.is_char_boundary(end) {
                        end -= 1;
                    }
                    self.prefix.push_str(&text[..end]);
                    self.complete = end == text.len();
                }
                Ok(())
            }
        }
        let mut actual = Bounded {
            prefix: String::new(),
            complete: true,
            hash: Sha256::new(),
        };
        let formatted_complete = write!(&mut actual, "{error}").is_ok();
        Self {
            kind: format!("{:?}", std::mem::discriminant(error)),
            prefix: actual.prefix,
            prefix_complete: actual.complete,
            formatted_complete,
            sha256: actual.hash.finalize().into(),
        }
    }
}
impl RetainedReturns {
    pub(super) fn require_not_exhausted(&self) -> Result<()> {
        if self.overflow {
            return Err(data_error(
                "original release return history exhausted; state retained",
            ));
        }
        Ok(())
    }
    pub(super) fn require_known_ok(&self) -> Result<()> {
        self.require_not_exhausted()?;
        if self.last_ok != Some(true) {
            return Err(data_error(
                "policy needs actual known original release API success",
            ));
        }
        Ok(())
    }
    pub(super) fn record(&mut self, ok: bool, error: Option<&BicDbError>) {
        let failure = error.map(Failure::retain);
        match self.count.checked_add(1) {
            Some(next) => self.count = next,
            None => self.overflow = true,
        }
        let mut chain = Sha256::new();
        chain.update(self.chain);
        chain.update(self.count.to_be_bytes());
        chain.update([u8::from(ok), u8::from(self.overflow)]);
        if let Some(failure) = &failure {
            chain.update(failure.sha256);
        }
        self.chain = chain.finalize().into();
        self.last_ok = Some(ok);
        if self.first_error.is_none() {
            self.first_error = failure;
        }
    }
    #[cfg(test)]
    pub(super) fn data(&self) -> serde_json::Value {
        serde_json::json!({"returns":self.count,"chain_sha256":hex::encode(self.chain),
            "overflow":self.overflow,"last_actual_return_ok":self.last_ok,
            "first_original_error":self.first_error.as_ref().map(|e|serde_json::json!({
                "kind":e.kind,"prefix":e.prefix,"prefix_complete":e.prefix_complete,
                "formatted_complete":e.formatted_complete,"sha256":hex::encode(e.sha256)}))})
    }
}
