use std::{
    ffi::{c_char, c_void, CStr, CString},
    str::FromStr,
};

use noodles::vcf::{
    header::format::Key,
    record::{genotype::field::Value, Genotype},
};

use super::wt_genotypes::WTGenotype;

#[repr(C)]
pub struct WTFloatListValue {
    list: *mut c_void,
}

#[no_mangle]
pub unsafe extern "C" fn next_float_list_value(wt_float_list_key: &mut WTFloatListKey) -> f32 {
    let value = unsafe { &mut *(wt_float_list_key.value.list as *mut Vec<f32>) };

    if wt_float_list_key.value_position >= wt_float_list_key.length {
        return f32::NAN;
    }

    let value = value
        .get(wt_float_list_key.value_position as usize)
        .unwrap();

    wt_float_list_key.value_position += 1;

    value.to_owned()
}

#[no_mangle]
pub unsafe extern "C" fn next_float_list_key(
    wt_genotype: &mut WTGenotype,
    key_str: *const c_char,
) -> WTFloatListKey {
    let genotype = unsafe { &mut *(wt_genotype.genotype as *mut Genotype) };
    let key_str = CStr::from_ptr(key_str).to_str().unwrap();

    let key = Key::from_str(key_str).unwrap();

    let value = match genotype.get(&key) {
        Some(v) => v,
        None => {
            return WTFloatListKey {
                error: std::ptr::null(),
                length: 0,
                value_position: 0,
                value: WTFloatListValue {
                    list: std::ptr::null_mut(),
                },
            }
        }
    };

    match value.value() {
        Some(v) => match v {
            Value::FloatArray(s) => WTFloatListKey::new(s),
            _ => WTFloatListKey::error("Value is not a float list"),
        },
        None => WTFloatListKey::none(),
    }
}

#[repr(C)]
pub struct WTFloatListKey {
    error: *const c_char,
    length: i32,
    value_position: i32,
    value: WTFloatListValue,
}

impl WTFloatListKey {
    fn new(value: &Vec<Option<f32>>) -> Self {
        let non_null_values: Vec<f32> = value.iter().filter_map(|v| v.clone()).collect();

        Self {
            error: std::ptr::null(),
            length: non_null_values.len() as i32,
            value_position: 0,
            value: WTFloatListValue {
                list: Box::into_raw(Box::new(non_null_values.clone())) as *mut c_void,
            },
        }
    }

    fn error(msg: &str) -> Self {
        Self {
            error: CString::new(msg).unwrap().into_raw(),
            length: 0,
            value_position: 0,
            value: WTFloatListValue {
                list: std::ptr::null_mut(),
            },
        }
    }

    fn none() -> Self {
        Self {
            error: std::ptr::null(),
            length: 0,
            value_position: 0,
            value: WTFloatListValue {
                list: std::ptr::null_mut(),
            },
        }
    }
}
