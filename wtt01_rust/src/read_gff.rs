use std::{
    ffi::{c_char, c_float, c_void, CStr, CString},
    io::{BufRead, BufReader},
    path::Path,
    str::FromStr,
    sync::Arc,
};

use arrow::{
    array::{Float32Builder, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use duckdb::ffi::duckdb_data_chunk;
use flate2::read::GzDecoder;
use noodles::gff::{self, Line, Reader};

#[repr(C)]
pub enum WTTPhase {
    None,
    Zero,
    One,
    Two,
}

#[repr(C)]
pub struct GFFRecord {
    pub reference_sequence_name: *const c_char,
    pub source: *const c_char,
    pub annotation_type: *const c_char,
    pub start: u64,
    pub end: u64,
    pub score: c_float,
    pub strand: *const c_char,
    pub phase: WTTPhase,
    pub attributes: *const c_char,
}

// Implement new for Record.
impl GFFRecord {
    pub fn new(
        reference_sequence_name: String,
        source: String,
        annotation_type: String,
        start: u64,
        end: u64,
        score: Option<f32>,
        strand: String,
        phase: WTTPhase,
        attributes: String,
    ) -> Self {
        // If phase not null convert its value to a string.
        let cast_score = match score {
            Some(score) => score,
            None => f32::NAN,
        };

        Self {
            reference_sequence_name: CString::new(reference_sequence_name).unwrap().into_raw(),
            source: CString::new(source).unwrap().into_raw(),
            annotation_type: CString::new(annotation_type).unwrap().into_raw(),
            start,
            end,
            score: cast_score,
            phase: phase,
            strand: CString::new(strand).unwrap().into_raw(),
            attributes: CString::new(attributes).unwrap().into_raw(),
        }
    }
}

// Implement default for Record.
impl Default for GFFRecord {
    fn default() -> Self {
        Self {
            reference_sequence_name: std::ptr::null(),
            source: std::ptr::null(),
            annotation_type: std::ptr::null(),
            start: 0,
            end: 0,
            score: f32::NAN,
            strand: std::ptr::null(),
            phase: WTTPhase::None,
            attributes: std::ptr::null(),
        }
    }
}

// Implement drop for Record.
impl Drop for GFFRecord {
    fn drop(&mut self) {
        unsafe {
            // TODO: fill out the rest of GFFRecord's fields
            let _ = CString::from_raw(self.reference_sequence_name as *mut c_char);
            let _ = CString::from_raw(self.source as *mut c_char);
            let _ = CString::from_raw(self.annotation_type as *mut c_char);
        }
    }
}

#[repr(C)]
pub struct GFFReaderC {
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
pub unsafe extern "C" fn gff_new(
    filename: *const c_char,
    compression: *const c_char,
) -> GFFReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let compression = CStr::from_ptr(compression).to_str().unwrap();

    let reader = build_from_path(filename, compression).unwrap();

    return GFFReaderC {
        inner_reader: Box::into_raw(Box::new(reader)) as *mut c_void,
    };
}

#[no_mangle]
pub unsafe extern "C" fn gff_next_batch(
    gff_reader: &GFFReaderC,
    chunk_ptr: *mut c_void,
    done: &mut bool,
    batch_size: usize,
) {
    let gff_reader_ptr = gff_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    let duckdb_ptr = chunk_ptr as duckdb_data_chunk;
    let mut duckdb_chunk = duckdb::vtab::DataChunk::from(duckdb_ptr);

    let file_schema = Schema::new(vec![
        Field::new("reference_sequence_name", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, true),
        Field::new("annotation_type", DataType::Utf8, false),
        Field::new("start", DataType::UInt64, false),
        Field::new("end", DataType::UInt64, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("phase", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
        // Field::new(
        //     "attributes",
        //     DataType::Map(
        //         Box::new(Field::new(
        //             "entries",
        //             DataType::Struct(vec![
        //                 Field::new("keys", DataType::Utf8, false),
        //                 Field::new("values", DataType::Utf8, true),
        //             ]),
        //             false,
        //         )),
        //         false,
        //     ),
        //     false,
        // ),
    ]);

    match gff_reader_ptr.as_mut() {
        Some(reader) => {
            let mut buffer = String::new();

            // Create new arrow vectors for each field.
            let mut reference_sequence_name_builder = StringBuilder::new();
            let mut source_builder = StringBuilder::new();
            let mut annotation_type_builder = StringBuilder::new();
            let mut start_builder = UInt64Builder::new();
            let mut end_builder = UInt64Builder::new();
            let mut score_builder = Float32Builder::new();
            let mut strand_builder = StringBuilder::new();
            let mut phase_builder = StringBuilder::new();
            let mut attributes_builder = StringBuilder::new();

            // Read in the batch size.
            for _ in 0..batch_size {
                let line_read_result = reader.read_line(&mut buffer);
                if line_read_result.is_err() {
                    *done = true;
                    continue;
                }

                let bytes_read = line_read_result.unwrap();
                if bytes_read == 0 {
                    *done = true;
                    continue;
                }

                let line = Line::from_str(&buffer);
                match line {
                    Ok(line) => match line {
                        Line::Record(record) => {
                            let reference_sequence_name =
                                record.reference_sequence_name().to_string();
                            reference_sequence_name_builder.append_value(reference_sequence_name);

                            let source = record.source().to_string();
                            source_builder.append_value(source);

                            let annotation_type = String::from("test");
                            annotation_type_builder.append_value(annotation_type);

                            let start = record.start().get() as u64;
                            start_builder.append_value(start);

                            let end = record.end().get() as u64;
                            end_builder.append_value(end);

                            let strand = record.strand().to_string();
                            strand_builder.append_value(strand);

                            let attributes = record.attributes().to_string();
                            attributes_builder.append_value(attributes);

                            let score = record.score().unwrap_or(f32::NAN);
                            score_builder.append_value(score);

                            let phase = record.phase();
                            match phase {
                                Some(phase) => {
                                    let phase_string = phase.to_string();
                                    phase_builder.append_value(phase_string);
                                }
                                None => {
                                    phase_builder.append_null();
                                }
                            }
                        }
                        _ => {}
                    },
                    Err(_) => {
                        *done = true;
                        continue;
                    }
                }
            }

            let rb = RecordBatch::try_new(
                Arc::new(file_schema.clone()),
                vec![
                    Arc::new(reference_sequence_name_builder.finish()),
                    Arc::new(source_builder.finish()),
                    Arc::new(annotation_type_builder.finish()),
                    Arc::new(start_builder.finish()),
                    Arc::new(end_builder.finish()),
                    Arc::new(score_builder.finish()),
                    Arc::new(strand_builder.finish()),
                    Arc::new(phase_builder.finish()),
                    Arc::new(attributes_builder.finish()),
                ],
            )
            .unwrap();

            let result = duckdb::vtab::record_batch_to_duckdb_data_chunk(&rb, &mut duckdb_chunk);
            result.unwrap();
        }
        None => {}
    }
}

#[no_mangle]
pub unsafe extern "C" fn gff_next(gff_reader: &GFFReaderC) -> GFFRecord {
    let gff_reader_ptr = gff_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    match gff_reader_ptr.as_mut() {
        Some(reader) => {
            let mut buffer = String::new();

            let line_read_result = reader.read_line(&mut buffer);
            if line_read_result.is_err() {
                return GFFRecord::default();
            }

            let bytes_read = line_read_result.unwrap();
            if bytes_read == 0 {
                return GFFRecord::default();
            }

            let line = Line::from_str(&buffer);

            match line {
                Ok(line) => match line {
                    // TODO: Add score
                    Line::Record(record) => {
                        let reference_sequence_name = record.reference_sequence_name().to_string();
                        let source = record.source().to_string();
                        let annotation_type = String::from("test");
                        let start = record.start().get() as u64;
                        let end = record.end().get() as u64;
                        let strand = record.strand().to_string();
                        let attributes = record.attributes().to_string();

                        let phase = match record.phase() {
                            Some(phase) => match phase {
                                gff::record::Phase::Zero => WTTPhase::Zero,
                                gff::record::Phase::One => WTTPhase::One,
                                gff::record::Phase::Two => WTTPhase::Two,
                            },
                            None => WTTPhase::None,
                        };

                        return GFFRecord::new(
                            reference_sequence_name,
                            source,
                            annotation_type,
                            start,
                            end,
                            record.score(),
                            strand,
                            phase,
                            attributes,
                        );
                    }
                    _ => {
                        eprintln!("gff_next: line is not a record");
                        return GFFRecord::default();
                    }
                },
                Err(e) => {
                    eprintln!("gff_next: parse error={} on line={}", e, buffer);
                    return GFFRecord::default();
                }
            }
        }
        None => {
            return GFFRecord::default();
        }
    }
}
