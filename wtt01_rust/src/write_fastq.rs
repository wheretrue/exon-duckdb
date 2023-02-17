use std::{
    ffi::{c_char, c_void, CStr},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use bytes::Bytes;
use noodles::fastq::{Record, Writer};

#[no_mangle]
pub extern "C" fn fastq_writer_new(
    filename: *const c_char,
    compression: *const c_char,
) -> *mut c_void {
    let filename = unsafe { CStr::from_ptr(filename) }.to_str().unwrap();
    let compression = unsafe { CStr::from_ptr(compression) }.to_str().unwrap();

    let file = match File::create(filename) {
        Ok(file) => file,
        Err(_) => return std::ptr::null_mut(),
    };

    let boxxed_writer = match compression {
        "gzip" => {
            let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            Box::new(BufWriter::new(encoder)) as Box<dyn Write>
        }
        "zstd" => {
            let encoder = zstd::stream::write::Encoder::new(file, 0).unwrap();
            let auto_finish_encoder = encoder.auto_finish();
            Box::new(BufWriter::new(auto_finish_encoder)) as Box<dyn Write>
        }
        _ => {
            // match based on the extension.
            let extension = Path::new(filename).extension().unwrap().to_str().unwrap();
            match extension {
                "gz" => {
                    let encoder =
                        flate2::write::GzEncoder::new(file, flate2::Compression::default());
                    Box::new(BufWriter::new(encoder)) as Box<dyn Write>
                }
                "zst" => {
                    let encoder = zstd::stream::write::Encoder::new(file, 0).unwrap();
                    let auto_finish_encoder = encoder.auto_finish();

                    Box::new(BufWriter::new(auto_finish_encoder)) as Box<dyn Write>
                }
                _ => Box::new(BufWriter::new(file)) as Box<dyn Write>,
            }
        }
    };

    let fastq_writer = Writer::new(boxxed_writer);

    Box::into_raw(Box::new(fastq_writer)) as *mut c_void
}

#[no_mangle]
pub extern "C" fn fastq_writer_write(
    writer: *mut c_void,
    id: *const c_char,
    description: *const c_char,
    seq: *const c_char,
    quality_scores: *const c_char,
) -> i32 {
    let writer = unsafe { &mut *(writer as *mut Writer<Box<dyn Write>>) };

    let id = unsafe { CStr::from_ptr(id) }.to_str().unwrap();
    let seq = unsafe { CStr::from_ptr(seq) }.to_str().unwrap();
    let quality_scores = unsafe { CStr::from_ptr(quality_scores) }.to_str().unwrap();

    // TODO: Need description updates here and for fastq.
    let description = unsafe { CStr::from_ptr(description) }.to_str().unwrap();

    let record = Record::new(
        Bytes::from(id),
        Bytes::from(seq),
        Bytes::from(quality_scores),
    );

    if description != "" {
        // &record.description_mut() = Some(Bytes::from(description));
    }

    match writer.write_record(&record) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

#[no_mangle]
pub extern "C" fn fastq_writer_destroy(writer: *mut c_void) {
    let _ = unsafe { Box::from_raw(writer as *mut Writer<Box<dyn Write>>) };
}
