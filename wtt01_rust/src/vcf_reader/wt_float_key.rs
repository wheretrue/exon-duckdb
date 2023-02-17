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
pub struct WTFloatKey {
    error: *const c_char,
    none: bool,
    value: f32,
}

impl Default for WTFloatKey {
    fn default() -> Self {
        Self {
            error: std::ptr::null(),
            none: true,
            value: f32::NAN,
        }
    }
}

impl WTFloatKey {
    pub fn new(value: &f32) -> Self {
        Self {
            error: std::ptr::null(),
            none: false,
            value: value.to_owned(),
        }
    }

    pub fn none() -> Self {
        Self {
            error: std::ptr::null(),
            none: true,
            value: f32::NAN,
        }
    }

    pub fn error(error: &str) -> Self {
        Self {
            error: CString::new(error).unwrap().into_raw(),
            none: true,
            value: f32::NAN,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn next_float_key(
    wt_genotype: &mut WTGenotype,
    key_str: *const c_char,
) -> WTFloatKey {
    let genotype = unsafe { &mut *(wt_genotype.genotype as *mut Genotype) };
    let key_str = CStr::from_ptr(key_str).to_str().unwrap();

    let key = Key::from_str(key_str).unwrap();

    let value = match genotype.get(&key) {
        Some(v) => v,
        None => return WTFloatKey::none(),
    };

    match value.value() {
        Some(v) => match v {
            Value::Float(s) => WTFloatKey::new(s),
            _ => WTFloatKey::error("Value is not a Float"),
        },
        None => WTFloatKey::none(),
    }
}
