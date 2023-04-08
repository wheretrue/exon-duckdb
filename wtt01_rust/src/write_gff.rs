use std::{
    ffi::{c_char, c_void, CStr},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    str::FromStr,
};

use noodles::core::Position;
use noodles::gff::{Record, Writer};

#[repr(C)]
pub struct GffWriter {
    pub writer: *mut c_void,
    pub error: *const c_char,
}

impl GffWriter {
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
pub extern "C" fn gff_writer_new(filename: *const c_char, compression: *const c_char) -> GffWriter {
    let filename = match unsafe { CStr::from_ptr(filename) }.to_str() {
        Ok(filename) => filename,
        Err(_) => {
            return GffWriter::error("gff_writer_new: filename is not valid UTF-8");
        }
    };

    let compression = match unsafe { CStr::from_ptr(compression) }.to_str() {
        Ok(compression) => compression,
        Err(_) => {
            return GffWriter::error("gff_writer_new: compression is not valid UTF-8");
        }
    };

    let file = match File::create(filename) {
        Ok(file) => file,
        Err(e) => return GffWriter::error(&format!("gff_writer_new: {}", e)),
    };

    let boxxed_writer = match compression {
        "gzip" => {
            let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            Box::new(BufWriter::new(encoder)) as Box<dyn Write>
        }
        "zstd" => {
            let encoder = match zstd::stream::write::Encoder::new(file, 0) {
                Ok(encoder) => encoder,
                Err(e) => return GffWriter::error(&format!("gff_writer_new: {}", e)),
            };
            Box::new(BufWriter::new(encoder)) as Box<dyn Write>
        }
        _ => {
            // match based on the extension.
            let extension = match Path::new(filename).extension() {
                Some(extension) => match extension.to_str() {
                    Some(extension) => Some(extension),
                    None => {
                        return GffWriter::error(
                            "gff_writer_new: filename extension is not valid UTF-8",
                        )
                    }
                },
                None => None,
            };

            match extension {
                Some("gz") => {
                    let encoder =
                        flate2::write::GzEncoder::new(file, flate2::Compression::default());
                    Box::new(BufWriter::new(encoder)) as Box<dyn Write>
                }
                Some("zst") => {
                    let encoder = zstd::stream::write::Encoder::new(file, 0).unwrap();
                    let auto_finish_encoder = encoder.auto_finish();

                    Box::new(BufWriter::new(auto_finish_encoder)) as Box<dyn Write>
                }
                _ => Box::new(BufWriter::new(file)) as Box<dyn Write>,
            }
        }
    };

    let gff_writer = Writer::new(boxxed_writer);
    GffWriter::from_writer(gff_writer)
}

#[repr(C)]
pub struct GffWriterResult {
    pub result: i32,
    pub error: *const c_char,
}

impl GffWriterResult {
    pub fn new(result: i32, error: *const c_char) -> Self {
        Self { result, error }
    }

    pub fn error(error: &str) -> Self {
        Self {
            result: 1,
            error: CStr::from_bytes_with_nul(error.as_bytes())
                .unwrap()
                .as_ptr(),
        }
    }

    pub fn from_result(result: Result<(), String>) -> Self {
        match result {
            Ok(_) => Self::new(0, std::ptr::null()),
            Err(e) => Self::error(&e),
        }
    }
}

#[no_mangle]
pub extern "C" fn gff_writer_write(
    writer: *mut c_void,
    reference_sequence_name: *const c_char,
    source: *const c_char,
    feature_type: *const c_char,
    start: i32,
    end: i32,
    score: f32,
    strand: *const c_char,
    phase: *const c_char,
    attributes: *const c_char,
) -> GffWriterResult {
    let writer = unsafe { &mut *(writer as *mut Writer<Box<dyn Write>>) };

    let reference_sequence_name = unsafe { CStr::from_ptr(reference_sequence_name) }
        .to_str()
        .unwrap();

    let source = unsafe { CStr::from_ptr(source) }.to_str().unwrap();
    let feature_type = unsafe { CStr::from_ptr(feature_type) }.to_str().unwrap();
    let strand = unsafe { CStr::from_ptr(strand) }.to_str().unwrap();

    let attributes = unsafe { CStr::from_ptr(attributes) }.to_str().unwrap();

    let start_position = Position::try_from(start as usize).unwrap();
    let end_position = Position::try_from(end as usize).unwrap();

    let phase = unsafe { CStr::from_ptr(phase) }.to_str().unwrap();
    let phase_type = match phase {
        "0" => Some(noodles::gff::record::Phase::Zero),
        "1" => Some(noodles::gff::record::Phase::One),
        "2" => Some(noodles::gff::record::Phase::Two),
        _ => None,
    };

    // trim the right semicolon off of attributes if it exists.
    let attributes = if attributes.ends_with(';') {
        &attributes[..attributes.len() - 1]
    } else {
        attributes
    };

    let parsed_attributes = noodles::gff::record::Attributes::from_str(attributes);
    let attrs = match parsed_attributes {
        Ok(attributes) => attributes,
        Err(e) => {
            return GffWriterResult::error(&format!("gff_writer_write: {}", e));
        }
    };

    let mut record_builder = Record::builder()
        .set_reference_sequence_name(String::from(reference_sequence_name))
        .set_source(String::from(source))
        .set_type(String::from(feature_type))
        .set_start(start_position)
        .set_end(end_position)
        .set_strand(strand.parse().unwrap())
        .set_attributes(attrs);

    if let Some(phase_type) = phase_type {
        record_builder = record_builder.set_phase(phase_type)
    }

    if !score.is_nan() {
        record_builder = record_builder.set_score(score)
    }

    let record = record_builder.build();

    match writer.write_record(&record) {
        Ok(_) => GffWriterResult::new(0, std::ptr::null()),
        Err(e) => GffWriterResult::error(&format!("gff_writer_write: {}", e)),
    }
}

#[no_mangle]
pub extern "C" fn gff_writer_destroy(writer: *mut c_void) {
    let _ = unsafe { Box::from_raw(writer as *mut Writer<Box<dyn Write>>) };
}
