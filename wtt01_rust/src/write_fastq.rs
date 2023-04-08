use std::{
    ffi::{c_char, c_void, CStr},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use bytes::Bytes;
use noodles::fastq::{Record, Writer};

#[repr(C)]
pub struct FastqWriter {
    pub writer: *mut c_void,
    pub error: *const c_char,
}

impl FastqWriter {
    pub fn new(writer: *mut c_void, error: *const c_char) -> Self {
        Self { writer, error }
    }

    pub fn error(error: &str) -> Self {
        Self {
            writer: std::ptr::null_mut(),
            error: CStr::from_bytes_with_nul(error.as_bytes())
                .unwrap()
                .as_ptr(),
        }
    }

    pub fn from_writer(writer: Writer<Box<dyn Write>>) -> Self {
        Self::new(
            Box::into_raw(Box::new(writer)) as *mut c_void,
            std::ptr::null(),
        )
    }
}

#[no_mangle]
pub extern "C" fn fastq_writer_new(
    filename: *const c_char,
    compression: *const c_char,
) -> FastqWriter {
    let filename = match unsafe { CStr::from_ptr(filename) }.to_str() {
        Ok(filename) => filename,
        Err(_) => {
            return FastqWriter::error("fastq_writer_new: filename is not valid UTF-8");
        }
    };

    let compression = match unsafe { CStr::from_ptr(compression) }.to_str() {
        Ok(compression) => compression,
        Err(_) => {
            return FastqWriter::error("fastq_writer_new: compression is not valid UTF-8");
        }
    };

    let file = match File::create(filename) {
        Ok(file) => file,
        Err(_) => {
            return FastqWriter::error("fastq_writer_new: could not create file");
        }
    };

    let boxxed_writer = match compression {
        "gzip" => {
            let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            Box::new(BufWriter::new(encoder)) as Box<dyn Write>
        }
        "zstd" => {
            let encoder = match zstd::stream::write::Encoder::new(file, 0) {
                Ok(encoder) => encoder,
                Err(_) => {
                    return FastqWriter::error("fastq_writer_new: could not create zstd encoder");
                }
            };

            let auto_finish_encoder = encoder.auto_finish();
            Box::new(BufWriter::new(auto_finish_encoder)) as Box<dyn Write>
        }
        _ => {
            // match based on the extension.
            let extension = match Path::new(filename).extension() {
                Some(extension) => match extension.to_str() {
                    Some(extension) => extension,
                    None => {
                        return FastqWriter::error(
                            "fastq_writer_new: could not get extension from filename",
                        );
                    }
                },
                None => "",
            };

            match extension {
                "gz" => {
                    let encoder =
                        flate2::write::GzEncoder::new(file, flate2::Compression::default());
                    Box::new(BufWriter::new(encoder)) as Box<dyn Write>
                }
                "zst" => {
                    let encoder = match zstd::stream::write::Encoder::new(file, 0) {
                        Ok(encoder) => encoder,
                        Err(_) => {
                            return FastqWriter::error(
                                "fastq_writer_new: could not create zstd encoder",
                            );
                        }
                    };
                    let auto_finish_encoder = encoder.auto_finish();

                    Box::new(BufWriter::new(auto_finish_encoder)) as Box<dyn Write>
                }
                _ => Box::new(BufWriter::new(file)) as Box<dyn Write>,
            }
        }
    };

    let fastq_writer = Writer::new(boxxed_writer);

    FastqWriter::from_writer(fastq_writer)
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

    let id = match unsafe { CStr::from_ptr(id) }.to_str() {
        Ok(id) => id,
        Err(_) => {
            return 1;
        }
    };
    let seq = match unsafe { CStr::from_ptr(seq) }.to_str() {
        Ok(seq) => seq,
        Err(_) => {
            return 1;
        }
    };

    let quality_scores = match unsafe { CStr::from_ptr(quality_scores) }.to_str() {
        Ok(quality_scores) => quality_scores,
        Err(_) => {
            return 1;
        }
    };

    let description = match unsafe { CStr::from_ptr(description) }.to_str() {
        Ok(description) => description,
        Err(_) => {
            return 1;
        }
    };

    let mut record = Record::new(
        Bytes::from(id),
        Bytes::from(seq),
        Bytes::from(quality_scores),
    );

    if description != "" {
        *record.description_mut() = Bytes::from(description).to_vec();
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
