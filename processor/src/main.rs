// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;

#[cfg(unix)]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<()> {
    println!("Hello, world!");
    Ok(())
}
