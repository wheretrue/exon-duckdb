use std::{
    ffi::{c_char, c_void, CStr},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    str::FromStr,
};

use noodles::core::Position;
use noodles::gff::{Record, Writer};

#[no_mangle]
pub extern "C" fn gff_writer_new(
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
            Box::new(BufWriter::new(encoder)) as Box<dyn Write>
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

    let gff_writer = Writer::new(boxxed_writer);

    Box::into_raw(Box::new(gff_writer)) as *mut c_void
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
) -> i32 {
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

    let parsed_attributes = noodles::gff::record::Attributes::from_str(attributes).unwrap();

    let mut record_builder = Record::builder()
        .set_reference_sequence_name(String::from(reference_sequence_name))
        .set_source(String::from(source))
        .set_type(String::from(feature_type))
        .set_start(start_position)
        .set_end(end_position)
        .set_strand(strand.parse().unwrap())
        .set_attributes(parsed_attributes);

    if let Some(phase_type) = phase_type {
        record_builder = record_builder.set_phase(phase_type)
    }

    if !score.is_nan() {
        record_builder = record_builder.set_score(score)
    }

    let record = record_builder.build();

    match writer.write_record(&record) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

#[no_mangle]
pub extern "C" fn gff_writer_destroy(writer: *mut c_void) {
    let _ = unsafe { Box::from_raw(writer as *mut Writer<Box<dyn Write>>) };
}
