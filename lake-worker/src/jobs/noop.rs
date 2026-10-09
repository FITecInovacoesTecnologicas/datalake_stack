//! `noop` (alias `smoke`): does nothing. Used by the smoke test to prove the queue works.
//!
//! Payload: anything (ignored).

use anyhow::Result;
use serde::de::IgnoredAny;

use super::{JobContext, JobKind};

pub struct Noop;

impl JobKind for Noop {
    const KIND: &'static str = "noop";
    type Payload = IgnoredAny;

    async fn run(_cx: &JobContext<'_>, _payload: IgnoredAny) -> Result<()> {
        Ok(())
    }
}
