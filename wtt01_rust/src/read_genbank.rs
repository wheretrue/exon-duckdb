use std::{
    ffi::{c_char, c_void, CStr},
    io::{BufRead, BufReader},
    path::Path,
};

use gb_io::reader::SeqReader;

#[repr(C)]
pub struct GenbankReader {
    inner_reader: *mut c_void,
}

pub fn build_from_path<P>(src: P) -> std::io::Result<SeqReader<Box<dyn BufRead>>>
where
    P: AsRef<Path>,
{
    let src = src.as_ref();
    let file = std::fs::File::open(src)?;
    let reader = Box::new(BufReader::new(file)) as Box<dyn BufRead>;

    Ok(SeqReader::new(reader))
}

#[no_mangle]
pub unsafe extern "C" fn genbank_new(filename: *const c_char) -> GenbankReader {
    let filename = CStr::from_ptr(filename).to_str().unwrap();

    let reader = build_from_path(filename).unwrap();

    return GenbankReader {
        inner_reader: Box::into_raw(Box::new(reader)) as *mut c_void,
    };
}

#[no_mangle]
pub unsafe extern "C" fn genbank_free(reader: GenbankReader) {
    let _ = Box::from_raw(reader.inner_reader as *mut SeqReader<Box<dyn BufRead>>);
}

#[no_mangle]
pub unsafe extern "C" fn genbank_next(reader: &GenbankReader) -> *mut c_char {
    let reader = reader.inner_reader as *mut SeqReader<Box<dyn BufRead>>;
    // let record = reader.next();

    match reader.as_mut() {
        Some(reader) => match reader.next() {
            Some(record) => match record {
                Ok(record) => {
                    let seq = record.seq;
                    let seq = std::ffi::CString::new(seq).unwrap();
                    seq.into_raw()
                }
                Err(_) => std::ptr::null_mut(),
            },
            _ => std::ptr::null_mut(),
        },
        _ => std::ptr::null_mut(),
    }
}
