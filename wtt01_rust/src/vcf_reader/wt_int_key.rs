use std::{ffi::{c_char, CString, CStr}, str::FromStr};

use noodles::vcf::{record::{Genotype, genotype::field::Value}, header::format::Key};

use super::wt_genotypes;

#[repr(C)]
pub struct WTIntKey {
    error: *const c_char,
    none: bool,
    value: i32,
}

impl Default for WTIntKey {
    fn default() -> Self {
        Self {
            error: std::ptr::null(),
            none: true,
            value: 0,
        }
    }
}

impl WTIntKey {
    pub fn new(value: i32) -> Self {
        Self {
            error: std::ptr::null(),
            none: false,
            value,
        }
    }

    pub fn none() -> Self {
        Self {
            error: std::ptr::null(),
            none: true,
            value: 0,
        }
    }

    pub fn error(error: &str) -> Self {
        Self {
            error: CString::new(error).unwrap().into_raw(),
            none: true,
            value: 0,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn next_int_key(
    wt_genotype: &mut wt_genotypes::WTGenotype,
    key_str: *const c_char,
) -> WTIntKey {
    let genotype = unsafe { &mut *(wt_genotype.genotype as *mut Genotype) };
    let key_str = CStr::from_ptr(key_str).to_str().unwrap();

    let key = Key::from_str(key_str).unwrap();

    let value = match genotype.get(&key) {
        Some(v) => v,
        None => return WTIntKey::none(),
    };

    match value.value() {
        Some(v) => match v {
            Value::Integer(i) => WTIntKey::new(*i),
            _ => WTIntKey::error("Value is not an integer"),
        },
        None => WTIntKey::none(),
    }
}
