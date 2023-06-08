// Copyright 2023 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

#[repr(C)]
pub struct CExtractResponse {
    sequence_start: usize,
    sequence_len: usize,
    extracted_sequence: *const c_char,
    error: *const c_char,
}

impl CExtractResponse {
    fn new(sequence_start: usize, sequence_len: usize, extracted_sequence: &str) -> Self {
        Self {
            sequence_start,
            sequence_len,
            extracted_sequence: CString::new(extracted_sequence).unwrap().into_raw(),
            error: std::ptr::null(),
        }
    }

    fn error(error: &str) -> Self {
        Self {
            sequence_start: 0,
            sequence_len: 0,
            extracted_sequence: std::ptr::null(),
            error: CString::new(error).unwrap().into_raw(),
        }
    }
}

#[no_mangle]
pub extern "C" fn extract_from_cigar(
    sequence_str: *const c_char,
    cigar_str: *const c_char,
) -> CExtractResponse {
    let cigar = unsafe { std::ffi::CStr::from_ptr(cigar_str) };
    let cigar = match cigar.to_str() {
        Ok(cigar) => cigar,
        Err(e) => return CExtractResponse::error(&e.to_string()),
    };

    let cigar_obj: Cigar = match cigar.parse() {
        Ok(cigar) => cigar,
        Err(e) => return CExtractResponse::error(&e.to_string()),
    };

    let total_ops = cigar_obj.len();
    let first_ops = cigar_obj[0];
    let last_ops = cigar_obj[total_ops - 1];

    let sequence = unsafe { std::ffi::CStr::from_ptr(sequence_str) };
    let sequence = match sequence.to_str() {
        Ok(sequence) => sequence,
        Err(e) => return CExtractResponse::error(&e.to_string()),
    };

    let sequence_start = match first_ops.kind() {
        noodles::sam::record::cigar::op::Kind::Insertion => first_ops.len(),
        _ => 0,
    };

    let sequence_len = match last_ops.kind() {
        noodles::sam::record::cigar::op::Kind::Insertion => sequence.len() - last_ops.len(),
        _ => sequence.len(),
    };

    let sequence = &sequence[sequence_start..sequence_len];

    CExtractResponse::new(sequence_start, sequence_len, sequence)
}
