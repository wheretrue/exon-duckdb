use std::{
    ffi::{c_char, c_void, CStr, CString},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use bytes::Bytes;
use noodles::fasta::{
    record::{Definition, Sequence},
    Record, Writer,
};

#[repr(C)]
pub struct NoodlesWriter {
    pub writer: *mut c_void,
    pub error: *const c_char,
}

impl NoodlesWriter {
    pub fn new(writer: *mut c_void, error: *const c_char) -> Self {
        Self { writer, error }
    }
}

#[no_mangle]
pub extern "C" fn fasta_writer_new(
    filename: *const c_char,
    compression: *const c_char,
) -> NoodlesWriter {
    let filename = unsafe { CStr::from_ptr(filename) }.to_str();
    let filename = match filename {
        Ok(filename) => filename,
        Err(_) => {
            return NoodlesWriter::new(
                std::ptr::null_mut(),
                CString::new("fasta_writer_new: filename is not valid UTF-8")
                    .unwrap()
                    .into_raw(),
            )
        }
    };

    if filename == "/dev/stdout" {
        let stdout = std::io::stdout();
        let boxxed_writer = Box::new(BufWriter::new(stdout)) as Box<dyn Write>;
        let fasta_writer = Writer::new(boxxed_writer);

        return NoodlesWriter::new(
            Box::into_raw(Box::new(fasta_writer)) as *mut c_void,
            std::ptr::null(),
        );
    };

    let compression = unsafe { CStr::from_ptr(compression) }.to_str().unwrap();

    let file = match File::create(filename) {
        Ok(file) => file,
        Err(e) => {
            return NoodlesWriter::new(
                std::ptr::null_mut(),
                CString::new(e.to_string()).unwrap().into_raw(),
            )
        }
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
            let extension = match Path::new(filename).extension() {
                Some(extension) => extension.to_str().unwrap(),
                None => {
                    return NoodlesWriter::new(
                        std::ptr::null_mut(),
                        CString::new("fasta_writer_new: filename missing or has no extension")
                            .unwrap()
                            .into_raw(),
                    )
                }
            };

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

    let fasta_writer = Writer::new(boxxed_writer);

    NoodlesWriter::new(
        Box::into_raw(Box::new(fasta_writer)) as *mut c_void,
        std::ptr::null(),
    )
}

#[no_mangle]
pub extern "C" fn fasta_writer_write(
    writer: *mut c_void,
    id: *const c_char,
    description: *const c_char,
    seq: *const c_char,
) -> i32 {
    let writer = unsafe { &mut *(writer as *mut Writer<Box<dyn Write>>) };

    let id = unsafe { CStr::from_ptr(id) }.to_str().unwrap();

    // TODO: use description
    let seq = unsafe { CStr::from_ptr(seq) }.to_str().unwrap();

    // if the description is empty, we don't want to use it.
    let description = if description.is_null() {
        None
    } else {
        let desc = unsafe { CStr::from_ptr(description) }.to_str().unwrap();
        Some(desc.to_string())
    };

    let definition = Definition::new(id, description);
    let sequence = Sequence::from(Bytes::from(seq));

    let record = Record::new(definition, sequence);

    writer.write_record(&record).unwrap();

    0
}

#[no_mangle]
pub extern "C" fn destroy_writer(writer: *mut c_void) {
    let _ = unsafe { Box::from_raw(writer as *mut Writer<Box<dyn Write>>) };
}
