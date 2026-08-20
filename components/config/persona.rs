/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! The process-global navigator persona: the identity the engine reports
//! through `navigator` when the embedder installs one. Absent a persona,
//! consumers fall back to their compile-time constants.

use std::sync::RwLock;

/// The fork-side persona projection. The embedder converts its own persona
/// spec into this type before installing it; the two stay intentionally
/// distinct so the embedder owns spec validation and this crate stays a
/// plain data carrier.
#[allow(dead_code)] // pub fields don't trip dead_code; this marks the not-yet-read fields' consumers:
// hardware_concurrency/device_memory_gb/color_depth (T5), gpu_* (T8), noise_seed (T9)
#[derive(Clone, Debug)]
pub struct Persona {
    pub platform: String,
    pub app_version: String,
    pub hardware_concurrency: u8,
    pub device_memory_gb: Option<u8>,
    pub color_depth: u16,
    pub gpu_vendor: String,
    pub gpu_renderer: String,
    pub gpu_unmasked_vendor: String,
    pub gpu_unmasked_renderer: String,
    pub noise_seed: u64,
}

static PERSONA: RwLock<Option<Persona>> = RwLock::new(None);

/// Install the process-global persona (see `ServoBuilder::persona`).
pub fn set(persona: Persona) {
    *PERSONA.write().unwrap() = Some(persona);
}

/// Read the process-global persona, if one is installed.
///
/// Clone-on-read rather than returning a guard: the persona is a small
/// (~200 byte) struct of strings and plain numbers, read rarely (navigator
/// getters), so an owned `Option<Persona>` avoids holding the read lock
/// across getter bodies and the guard-lifetime fallout at call sites.
pub fn get() -> Option<Persona> {
    PERSONA.read().unwrap().clone()
}
