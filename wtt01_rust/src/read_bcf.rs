use std::ffi::{c_char, c_void, CStr};
use std::fs::File;

use duckdb::ffi::duckdb_data_chunk;
use noodles::bcf::header::StringMaps;
use noodles::bcf::Record;
use noodles::vcf::Header;
use noodles::{bcf, bgzf};

use crate::read_vcf::VCFRecordBatch;

#[repr(C)]
pub struct BcfReaderC {
    bcf_reader: *mut c_void,
    bcf_header: *mut c_void,
    bcf_string_maps: *mut c_void,
    error: *const c_char,
}

#[no_mangle]
pub unsafe extern "C" fn bcf_new(filename: *const c_char) -> BcfReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let file = File::open(filename);

    if let Err(e) = file {
        return BcfReaderC {
            bcf_reader: std::ptr::null_mut(),
            bcf_header: std::ptr::null_mut(),
            bcf_string_maps: std::ptr::null_mut(),
            error: std::ffi::CString::new(format!("{}", e)).unwrap().into_raw(),
        };
    }
    let file = file.unwrap();

    let mut reader = bcf::Reader::new(file);
    if let Err(e) = reader.read_file_format() {
        return BcfReaderC {
            bcf_reader: std::ptr::null_mut(),
            bcf_header: std::ptr::null_mut(),
            bcf_string_maps: std::ptr::null_mut(),
            error: std::ffi::CString::new(format!("{}", e)).unwrap().into_raw(),
        };
    }

    let header = reader.read_header().unwrap();
    let rust_header: Header = header.parse().unwrap();

    let string_maps = StringMaps::from(&rust_header);

    BcfReaderC {
        bcf_reader: Box::into_raw(Box::new(reader)) as *mut c_void,
        bcf_header: Box::into_raw(Box::new(rust_header)) as *mut c_void,
        bcf_string_maps: Box::into_raw(Box::new(string_maps)) as *mut c_void,
        error: std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn bcf_next(
    bcf_reader: &mut BcfReaderC,
    chunk_ptr: *mut c_void,
    done: &mut bool,
    chunk_size: usize,
) {
    let bcf_reader_ptr = bcf_reader.bcf_reader as *mut bcf::Reader<bgzf::Reader<File>>;

    if (bcf_reader_ptr as usize) == 0 {
        eprintln!("bcf_next: bcf_reader is null");
    }

    let duckdb_ptr = chunk_ptr as duckdb_data_chunk;
    let duckdb_chunk = duckdb::vtab::DataChunk::from(duckdb_ptr);

    let mut record_batch = VCFRecordBatch::new(duckdb_chunk);

    let header = bcf_reader.bcf_header as *mut Header;
    let header = header.as_mut().unwrap();

    let string_maps = bcf_reader.bcf_string_maps as *mut StringMaps;
    let string_maps = string_maps.as_mut().unwrap();

    match bcf_reader_ptr.as_mut() {
        Some(reader) => {
            let mut i = 0;
            let mut record = Record::default();

            while i < chunk_size {
                match reader.read_record(&mut record) {
                    Ok(read_bytes) => {
                        if read_bytes == 0 {
                            *done = true;
                            break;
                        }

                        match record.try_into_vcf_record(header, string_maps) {
                            Ok(vcf_record) => {
                                record_batch.add_record(i, vcf_record);
                            }
                            Err(e) => {
                                eprintln!("bcf_next: {}", e);
                                *done = true;
                                break;
                            }
                        }

                        i += 1;
                    }
                    Err(e) => {
                        eprintln!("bcf_next: {}", e);
                        *done = true;
                        break;
                    }
                }
            }
        }
        None => {
            eprintln!("bcf_next: bcf_reader is null");
        }
    }
}
