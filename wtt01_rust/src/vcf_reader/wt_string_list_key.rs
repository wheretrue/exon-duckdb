use std::{ffi::{c_void, CString, c_char, CStr}, str::FromStr};

use noodles::vcf::{record::{Genotype, genotype::field::Value}, header::format::Key};

use super::wt_genotypes::WTGenotype;

#[repr(C)]
pub struct WTStringListValue {
    list: *mut c_void,
}

#[no_mangle]
pub unsafe extern "C" fn next_string_list_value(
    wt_string_list_key: &mut WTStringListKey,
) -> *const c_char {
    let value = unsafe { &mut *(wt_string_list_key.value.list as *mut Vec<String>) };

    if wt_string_list_key.value_position >= wt_string_list_key.length {
        return std::ptr::null();
    }

    let value = value
        .get(wt_string_list_key.value_position as usize)
        .unwrap();

    wt_string_list_key.value_position += 1;

    CString::new(value.clone()).unwrap().into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn next_string_list_key(
    wt_genotype: &mut WTGenotype,
    key_str: *const c_char,
) -> WTStringListKey {
    let genotype = unsafe { &mut *(wt_genotype.genotype as *mut Genotype) };
    let key_str = CStr::from_ptr(key_str).to_str().unwrap();

    let key = Key::from_str(key_str).unwrap();

    let value = match genotype.get(&key) {
        Some(v) => v,
        None => {
            return WTStringListKey {
                error: std::ptr::null(),
                length: 0,
                value_position: 0,
                value: WTStringListValue {
                    list: std::ptr::null_mut(),
                },
            }
        }
    };

    match value.value() {
        Some(v) => match v {
            Value::StringArray(s) => WTStringListKey::new(s),
            _ => WTStringListKey::error("Value is not a string list"),
        },
        None => WTStringListKey::none(),
    }
}

#[repr(C)]
pub struct WTStringListKey {
    error: *const c_char,
    length: i32,
    value_position: i32,
    value: WTStringListValue,
}

impl WTStringListKey {
    fn new(value: &Vec<Option<String>>) -> Self {
        let non_null_values: Vec<String> = value
            .iter()
            .filter_map(|v| v.clone())
            .collect();

        Self {
            error: std::ptr::null(),
            length: non_null_values.len() as i32,
            value_position: 0,
            value: WTStringListValue {
                list: Box::into_raw(Box::new(non_null_values.clone())) as *mut c_void,
            },
        }
    }

    fn error(msg: &str) -> Self {
        Self {
            error: CString::new(msg).unwrap().into_raw(),
            length: 0,
            value_position: 0,
            value: WTStringListValue {
                list: std::ptr::null_mut(),
            },
        }
    }

    fn none() -> Self {
        Self {
            error: std::ptr::null(),
            length: 0,
            value_position: 0,
            value: WTStringListValue {
                list: std::ptr::null_mut(),
            },
        }
    }

}
