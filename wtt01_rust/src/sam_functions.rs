use std::ffi::{c_char, CString};

use noodles::sam::record::{cigar::Cigar, Flags};

#[no_mangle]
pub extern "C" fn is_segmented(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::SEGMENTED)
}

#[no_mangle]
pub extern "C" fn is_unmapped(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::UNMAPPED)
}

#[no_mangle]
pub extern "C" fn is_properly_aligned(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::PROPERLY_ALIGNED)
}

#[no_mangle]
pub extern "C" fn is_mate_unmapped(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::MATE_UNMAPPED)
}

#[no_mangle]
pub extern "C" fn is_reverse_complemented(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::REVERSE_COMPLEMENTED)
}

#[no_mangle]
pub extern "C" fn is_mate_reverse_complemented(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::MATE_REVERSE_COMPLEMENTED)
}

#[no_mangle]
pub extern "C" fn is_first_segment(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);

    flag.contains(Flags::FIRST_SEGMENT)
}

#[no_mangle]
pub extern "C" fn is_last_segment(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::LAST_SEGMENT)
}

#[no_mangle]
pub extern "C" fn is_secondary(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::SECONDARY)
}

#[no_mangle]
pub extern "C" fn is_quality_control_failed(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::QC_FAIL)
}

#[no_mangle]
pub extern "C" fn is_duplicate(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::DUPLICATE)
}

#[no_mangle]
pub extern "C" fn is_supplementary(flag: u16) -> bool {
    let flag = Flags::from_bits_truncate(flag);
    flag.contains(Flags::SUPPLEMENTARY)
}

#[repr(C)]
pub struct CResult {
    value: *const c_char,
    error: *const c_char,
}

impl CResult {
    fn new(value: &str) -> Self {
        Self {
            value: CString::new(value).unwrap().into_raw(),
            error: std::ptr::null(),
        }
    }

    fn error(error: &str) -> Self {
        Self {
            value: std::ptr::null(),
            error: CString::new(error).unwrap().into_raw(),
        }
    }
}

#[no_mangle]
pub extern "C" fn parse_cigar(cigar: *const c_char) -> CResult {
    let cigar = unsafe { std::ffi::CStr::from_ptr(cigar) };
    let cigar = cigar.to_str().unwrap();

    let cigar_obj: Cigar = match cigar.parse() {
        Ok(cigar) => cigar,
        Err(e) => return CResult::error(&e.to_string()),
    };

    let serialized_obj = cigar_obj
        .iter()
        .map(|op| format!("{}={}", op.kind(), op.len()))
        .collect::<Vec<String>>()
        .join(";");

    CResult::new(serialized_obj.as_str())
}
