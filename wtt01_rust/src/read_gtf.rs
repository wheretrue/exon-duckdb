use std::{
    ffi::{c_char, c_void, CStr, CString},
    io::{BufRead, BufReader},
    path::Path,
};

use arrow::{
    array::{Array, StructBuilder},
    datatypes::*,
};
use arrow::{
    array::{ArrayBuilder, StringBuilder},
    datatypes::Schema,
};

use arrow::ffi::FFI_ArrowArray as ArrowArray;
use arrow::ffi::FFI_ArrowSchema as ArrowSchema;
use flate2::read::GzDecoder;
use noodles::gtf::Reader;

#[repr(C)]
pub struct GTFReaderC {
    inner_reader: *mut c_void,
}

pub fn build_from_path<P>(src: P, compression: &str) -> std::io::Result<Reader<Box<dyn BufRead>>>
where
    P: AsRef<Path>,
{
    let src = src.as_ref();

    let file = std::fs::File::open(src)?;

    let reader = match compression {
        "gzip" => {
            let decoder = GzDecoder::new(file);
            Box::new(BufReader::new(decoder)) as Box<dyn BufRead>
        }
        "zstd" => {
            let decoder = zstd::stream::read::Decoder::new(file)?;
            Box::new(BufReader::new(decoder)) as Box<dyn BufRead>
        }
        _ => {
            // match based on the extension.
            let extension = src.extension().unwrap().to_str().unwrap();
            match extension {
                "gz" => {
                    let decoder = GzDecoder::new(file);
                    Box::new(BufReader::new(decoder)) as Box<dyn BufRead>
                }
                "zst" => {
                    let decoder = zstd::stream::read::Decoder::new(file)?;
                    Box::new(BufReader::new(decoder)) as Box<dyn BufRead>
                }
                _ => Box::new(BufReader::new(file)) as Box<dyn BufRead>,
            }
        }
    };

    Ok(Reader::new(reader))
}

#[no_mangle]
pub unsafe extern "C" fn gtf_new(
    filename: *const c_char,
    compression: *const c_char,
) -> GTFReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let compression = CStr::from_ptr(compression).to_str().unwrap();

    let reader = build_from_path(filename, compression).unwrap();

    return GTFReaderC {
        inner_reader: Box::into_raw(Box::new(reader)) as *mut c_void,
    };
}

#[repr(C)]
pub struct GTFResult {
    pub error: *mut c_char,
    pub done: bool,
    pub array: ArrowArray,
    pub schema: ArrowSchema,
}

impl GTFResult {
    pub fn default() -> Self {
        Self {
            error: std::ptr::null_mut(),
            done: false,
            array: ArrowArray::empty(),
            schema: ArrowSchema::empty(),
        }
    }

    pub fn new(array: ArrowArray, schema: ArrowSchema) -> Self {
        Self {
            error: std::ptr::null_mut(),
            done: false,
            array,
            schema,
        }
    }

    pub fn new_error(error: &str) -> Self {
        let error = CString::new(error).unwrap();
        Self {
            error: error.into_raw(),
            done: true,
            array: ArrowArray::empty(),
            schema: ArrowSchema::empty(),
        }
    }

    pub fn new_done() -> Self {
        Self {
            error: std::ptr::null_mut(),
            done: true,
            array: ArrowArray::empty(),
            schema: ArrowSchema::empty(),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn gtf_insert_record_batch(
    gtf_reader: &GTFReaderC,
    batch_size: usize,
) -> GTFResult {
    let gtf_reader_ptr = gtf_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    let fields = vec![Field::new("seqname", DataType::Utf8, false)];
    let file_schema = Schema::new(fields);

    let out_schema = ArrowSchema::try_from(file_schema).unwrap();

    let builders = vec![Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>];

    let fields2 = vec![Field::new("seqname", DataType::Utf8, false)];
    let mut batch_builder = StructBuilder::new(fields2, builders);

    let mut seqname_builder = batch_builder.field_builder::<StringBuilder>(0).unwrap();
    seqname_builder.append_value("valuhie");
    seqname_builder.append_value("valuhie");
    seqname_builder.append_value("valuhie");

    batch_builder.append(true);
    batch_builder.append(true);
    batch_builder.append(true);

    let built_array = batch_builder.finish();
    let out_array = ArrowArray::new(built_array.data());

    GTFResult::new(out_array, out_schema)
}
