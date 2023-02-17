use std::{
    ffi::{c_char, c_void, CStr, CString},
    str::FromStr,
};

use noodles::vcf::{
    header::info::Key,
    record::info::{field::Value, Field, Info},
};

#[repr(C)]
pub struct WTInfos {
    pub infos: *mut c_void,
    pub number_of_infos: i32,
    pub info_position: i32,
}

impl Default for WTInfos {
    fn default() -> Self {
        Self {
            infos: std::ptr::null_mut(),
            number_of_infos: 0,
            info_position: 0,
        }
    }
}

impl WTInfos {
    pub fn new(infos: &Info) -> Self {
        Self {
            infos: Box::into_raw(Box::new(infos.clone())) as *mut c_void,
            number_of_infos: infos.len() as i32,
            info_position: 0,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn next_info(key: *const c_char, wt_infos: &mut WTInfos) -> WTField {
    let key_str = CStr::from_ptr(key).to_str().unwrap();
    let infos = unsafe { &mut *(wt_infos.infos as *mut Info) };

    let noodles_key = Key::from_str(key_str).unwrap();

    match infos.get(&noodles_key) {
        Some(field) => match field.value() {
            Some(_) => WTField::new(field),
            None => WTField::none(),
        },
        None => WTField::none(),
    }
}

#[repr(C)]
pub struct WTFieldList {
    pub fields: *mut c_void,
    pub number_of_fields: i32,
    pub field_position: i32,
}

#[no_mangle]
pub unsafe extern "C" fn info_field_ints(wt_field: &mut WTField) -> WTFieldList {
    let value = unsafe { &mut *(wt_field.value as *mut Vec<i32>) };

    WTFieldList {
        fields: Box::into_raw(Box::new(value.clone())) as *mut c_void,
        number_of_fields: value.len() as i32,
        field_position: 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn info_field_int_value(wt_field_list: &mut WTFieldList) -> i32 {
    let value = unsafe { &mut *(wt_field_list.fields as *mut Vec<i32>) };

    let field = value.get(wt_field_list.field_position as usize).unwrap();

    wt_field_list.field_position += 1;

    *field
}

#[no_mangle]
pub unsafe extern "C" fn info_field_strings(wt_field: &mut WTField) -> WTFieldList {
    let value = unsafe { &mut *(wt_field.value as *mut Vec<String>) };

    WTFieldList {
        fields: Box::into_raw(Box::new(value.clone())) as *mut c_void,
        number_of_fields: value.len() as i32,
        field_position: 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn info_field_floats(wt_field: &mut WTField) -> WTFieldList {
    let value = unsafe { &mut *(wt_field.value as *mut Vec<f32>) };

    WTFieldList {
        fields: Box::into_raw(Box::new(value.clone())) as *mut c_void,
        number_of_fields: value.len() as i32,
        field_position: 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn info_field_string_value(wt_field_list: &mut WTFieldList) -> *const c_char {
    let value = unsafe { &mut *(wt_field_list.fields as *mut Vec<String>) };

    let field = value.get(wt_field_list.field_position as usize).unwrap();

    wt_field_list.field_position += 1;

    CString::new(field.clone()).unwrap().into_raw()
}

#[repr(C)]
pub struct WTFloatValue {
    pub value: f32,
    pub is_null: bool,
}

impl WTFloatValue {
    pub fn new(value: f32) -> Self {
        Self {
            value,
            is_null: false,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn info_field_float_value(wt_field_list: &mut WTFieldList) -> WTFloatValue {
    let value = unsafe { &mut *(wt_field_list.fields as *mut Vec<f32>) };

    let field = value.get(wt_field_list.field_position as usize).unwrap();

    wt_field_list.field_position += 1;

    WTFloatValue::new(*field)
}

#[no_mangle]
pub unsafe extern "C" fn info_field_int(wt_field: &mut WTField) -> i32 {
    // unbox the value field into an i32
    let value = unsafe { &mut *(wt_field.value as *mut i32) };

    *value
}

#[no_mangle]
pub unsafe extern "C" fn info_field_float(wt_field: &mut WTField) -> f32 {
    let value = unsafe { &mut *(wt_field.value as *mut f32) };

    *value
}

#[no_mangle]
pub unsafe extern "C" fn info_field_string(wt_field: &mut WTField) -> *const c_char {
    let cstr = unsafe { CStr::from_ptr(wt_field.value as *const _) };

    let value = cstr.to_str().unwrap();

    CString::new(value).unwrap().into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn info_field_bool(wt_field: &mut WTField) -> bool {
    let value = unsafe { &mut *(wt_field.value as *mut bool) };

    *value
}

#[repr(C)]
pub struct WTField {
    pub id: *const c_char,
    pub ty: *const c_char,
    pub number: *const c_char,
    pub value: *mut c_void,
    pub error: *const c_char,
}

impl WTField {
    pub fn none() -> Self {
        Self {
            id: std::ptr::null(),
            ty: std::ptr::null(),
            number: std::ptr::null(),
            value: std::ptr::null_mut(),
            error: std::ptr::null(),
        }
    }

    pub fn new(field: &Field) -> Self {
        let id = CString::new(field.key().to_string()).unwrap();

        let value = field.value();

        match value {
            Some(value) => match value {
                Value::Integer(i) => Self {
                    id: id.into_raw(),
                    ty: CString::new("Integer").unwrap().into_raw(),
                    number: CString::new("1").unwrap().into_raw(),
                    value: Box::into_raw(Box::new(i.to_owned())) as *mut c_void,
                    error: std::ptr::null(),
                },
                Value::String(s) => Self {
                    id: id.into_raw(),
                    ty: CString::new("String").unwrap().into_raw(),
                    number: CString::new("1").unwrap().into_raw(),
                    value: CString::new(s.as_str()).unwrap().into_raw() as *mut c_void,
                    error: std::ptr::null(),
                },
                Value::Float(f) => Self {
                    id: id.into_raw(),
                    ty: CString::new("Float").unwrap().into_raw(),
                    number: CString::new("1").unwrap().into_raw(),
                    value: Box::into_raw(Box::new(f.to_owned())) as *mut c_void,
                    error: std::ptr::null(),
                },
                Value::FloatArray(f) => {
                    let filtered_floats = f
                        .iter()
                        .filter(|f| f.is_some())
                        .map(|f| f.unwrap())
                        .collect::<Vec<f32>>();

                    Self {
                        id: id.into_raw(),
                        ty: CString::new("Float").unwrap().into_raw(),
                        number: CString::new(".").unwrap().into_raw(),
                        value: Box::into_raw(Box::new(filtered_floats)) as *mut c_void,
                        error: std::ptr::null(),
                    }
                }
                Value::IntegerArray(i) => {
                    let filtered_ints = i
                        .iter()
                        .filter(|i| i.is_some())
                        .map(|i| i.unwrap())
                        .collect::<Vec<i32>>();

                    Self {
                        id: id.into_raw(),
                        ty: CString::new("Integer").unwrap().into_raw(),
                        number: CString::new(".").unwrap().into_raw(),
                        value: Box::into_raw(Box::new(filtered_ints)) as *mut c_void,
                        error: std::ptr::null(),
                    }
                }
                Value::StringArray(s) => {
                    let filtered_strings = s
                        .iter()
                        .filter_map(|f| f.as_ref())
                        .map(|f| f.to_owned())
                        .collect::<Vec<String>>();

                    Self {
                        id: id.into_raw(),
                        ty: CString::new("String").unwrap().into_raw(),
                        number: CString::new(".").unwrap().into_raw(),
                        value: Box::into_raw(Box::new(filtered_strings)) as *mut c_void,
                        error: std::ptr::null(),
                    }
                }
                Value::Flag => Self {
                    id: id.into_raw(),
                    ty: CString::new("Flag").unwrap().into_raw(),
                    number: std::ptr::null(),
                    value: Box::into_raw(Box::new(true)) as *mut c_void,
                    error: std::ptr::null(),
                },
                _ => Self {
                    id: id.into_raw(),
                    ty: std::ptr::null(),
                    number: std::ptr::null(),
                    value: std::ptr::null_mut(),
                    error: CString::new(
                        "Unsupported value type ".to_owned()
                            + &field.to_string()
                            + &value.to_string(),
                    )
                    .unwrap()
                    .into_raw(),
                },
            },
            None => Self {
                id: id.into_raw(),
                ty: std::ptr::null(),
                number: std::ptr::null(),
                value: std::ptr::null_mut(),
                error: CString::new("No value").unwrap().into_raw(),
            },
        }
    }
}
