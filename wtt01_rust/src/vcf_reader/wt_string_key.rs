use std::{
    ffi::{c_char, CStr, CString},
    str::FromStr,
};

use noodles::vcf::{
    header::format::Key,
    record::{genotype::field::Value, Genotype},
};

use super::wt_genotypes::WTGenotype;

#[repr(C)]
pub struct WTStringKey {
    error: *const c_char,
    none: bool,
    value: *const c_char,
}

impl Default for WTStringKey {
    fn default() -> Self {
        Self {
            error: std::ptr::null(),
            none: true,
            value: std::ptr::null(),
        }
    }
}

impl WTStringKey {
    pub fn new(value: &str) -> Self {
        Self {
            error: std::ptr::null(),
            none: false,
            value: CString::new(value).unwrap().into_raw(),
        }
    }

    pub fn none() -> Self {
        Self {
            error: std::ptr::null(),
            none: true,
            value: std::ptr::null(),
        }
    }

    pub fn error(error: &str) -> Self {
        Self {
            error: CString::new(error).unwrap().into_raw(),
            none: true,
            value: std::ptr::null(),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn next_string_key(
    wt_genotype: &mut WTGenotype,
    key_str: *const c_char,
) -> WTStringKey {
    let genotype = unsafe { &mut *(wt_genotype.genotype as *mut Genotype) };
    let key_str = CStr::from_ptr(key_str).to_str().unwrap();

    let key = Key::from_str(key_str).unwrap();

    let value = match genotype.get(&key) {
        Some(v) => v,
        None => return WTStringKey::none(),
    };

    match value.value() {
        Some(v) => match v {
            Value::String(s) => WTStringKey::new(s),
            _ => WTStringKey::error("Value is not a string"),
        },
        None => WTStringKey::none(),
    }
}
