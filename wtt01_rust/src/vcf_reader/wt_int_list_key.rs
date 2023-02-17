use std::{ffi::{c_char, CStr, c_void}, str::FromStr};

use noodles::vcf::{record::{Genotype, genotype::field::Value}, header::format::Key};

use super::wt_genotypes::WTGenotype;

#[repr(C)]
pub struct WTIntListValue {
    list: *mut c_void
}

#[repr(C)]
pub struct WTIntListKey {
    error: *const c_char,
    length: u32,
    value_position: u32,
    value: WTIntListValue,
}

impl Default for WTIntListKey {
    fn default() -> Self {
        Self {
            error: std::ptr::null(),
            length: 0,
            value_position: 0,
            value: WTIntListValue {
                list: std::ptr::null_mut(),
            },
        }
    }
}

#[repr(C)]
pub struct WTIntListElement {
    value: i32,
    is_none: bool,
    error: *const c_char,
}

#[no_mangle]
pub unsafe extern "C" fn next_int_list_value(
    wt_int_list_key: &mut WTIntListKey,
) -> WTIntListElement {
    let list = unsafe { &mut *(wt_int_list_key.value.list as *mut Vec<i32>) };

    if wt_int_list_key.value_position >= wt_int_list_key.length {
        return WTIntListElement {
            value: 0,
            is_none: true,
            error: std::ptr::null(),
        };
    }

    let value = list[wt_int_list_key.value_position as usize].clone();

    wt_int_list_key.value_position += 1;

    WTIntListElement {
        value,
        is_none: false,
        error: std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn next_int_list_key(
    wt_genotype: &mut WTGenotype,
    key_str: *const c_char,
) -> WTIntListKey {

    let genotype = unsafe { &mut *(wt_genotype.genotype as *mut Genotype) };
    let key_str = CStr::from_ptr(key_str).to_str().unwrap();

    let key = Key::from_str(key_str).unwrap();

    let value = match genotype.get(&key) {
        Some(v) => v,
        None => {
            return WTIntListKey {
                error: std::ptr::null(),
                length: 0,
                value_position: 0,
                value: WTIntListValue {
                    list: std::ptr::null_mut(),
                },
            }
        }
    };

    match value.value() {
        Some(v) => match v {
            Value::IntegerArray(i) => {
                let length = i.len() as u32;
                let value_position = 0;
                let value = WTIntListValue {
                    list: Box::into_raw(Box::new(i.clone())) as *mut c_void,
                };
                WTIntListKey {
                    error: std::ptr::null(),
                    length,
                    value_position,
                    value,
                }
            }
            _ => {
                return WTIntListKey {
                    error: std::ptr::null(),
                    length: 0,
                    value_position: 0,
                    value: WTIntListValue {
                        list: std::ptr::null_mut(),
                    },
                }
            }
        },
        None => {
            return WTIntListKey {
                error: std::ptr::null(),
                length: 0,
                value_position: 0,
                value: WTIntListValue {
                    list: std::ptr::null_mut(),
                },
            }
        }
    }
}