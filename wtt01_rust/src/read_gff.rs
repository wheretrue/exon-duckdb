use std::{
    ffi::{c_char, c_void, CStr, CString},
    io::{BufRead, BufReader},
    path::Path,
    str::FromStr,
};

use duckdb::{ffi::duckdb_data_chunk, vtab::Inserter};
use flate2::read::GzDecoder;
use noodles::gff::{Line, Reader, Record};

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

pub struct GFFRecordBatch {
    duckdb_chunk: duckdb::vtab::DataChunk,

    reference_sequence_name_vector: duckdb::vtab::FlatVector,
    source_vector: duckdb::vtab::FlatVector,
    annotation_type_vector: duckdb::vtab::FlatVector,
    start_vector: duckdb::vtab::FlatVector,
    end_vector: duckdb::vtab::FlatVector,
    score_vector: duckdb::vtab::FlatVector,
    strand_vector: duckdb::vtab::FlatVector,
    phase_vector: duckdb::vtab::FlatVector,
    attributes_vector: duckdb::vtab::FlatVector,
}

impl GFFRecordBatch {
    pub fn new(duckdb_chunk: duckdb::vtab::DataChunk) -> Self {
        let reference_sequence_name_vector = duckdb_chunk.flat_vector(0);
        let source_vector = duckdb_chunk.flat_vector(1);
        let annotation_type_vector = duckdb_chunk.flat_vector(2);
        let start_vector = duckdb_chunk.flat_vector(3);
        let end_vector = duckdb_chunk.flat_vector(4);
        let score_vector = duckdb_chunk.flat_vector(5);
        let strand_vector = duckdb_chunk.flat_vector(6);
        let phase_vector = duckdb_chunk.flat_vector(7);
        let attributes_vector = duckdb_chunk.flat_vector(8);

        Self {
            duckdb_chunk,
            reference_sequence_name_vector,
            source_vector,
            annotation_type_vector,
            start_vector,
            end_vector,
            score_vector,
            strand_vector,
            phase_vector,
            attributes_vector,
        }
    }

    pub fn add_record(&mut self, i: usize, record: Record) {
        let reference_sequence_name = record.reference_sequence_name();
        let source = record.source();
        let annotation_type = record.ty();
        let start = record.start();
        let end = record.end();
        let score = record.score();
        let strand = record.strand();
        let phase = record.phase();
        let attributes = record.attributes();

        self.reference_sequence_name_vector
            .insert(i, reference_sequence_name);

        self.source_vector.insert(i, source);

        self.annotation_type_vector.insert(i, annotation_type);

        self.start_vector.as_mut_slice::<i64>()[i] = start.get() as i64;
        self.end_vector.as_mut_slice::<i64>()[i] = end.get() as i64;

        if score.is_some() {
            self.score_vector.as_mut_slice::<f32>()[i] = score.unwrap();
        } else {
            self.score_vector.set_null(i);
        }

        self.strand_vector.insert(i, strand.as_ref());

        match phase {
            Some(phase) => {
                self.phase_vector.insert(i, phase.as_ref());
            }
            None => {
                self.phase_vector.set_null(i);
            }
        }

        // build up the attributes_str based on the entries.
        let mut attributes_str = String::new();
        for entry in attributes.iter() {
            let key = entry.key();
            let value = entry.value();
            let entry_str = format!("{}={};", key, value);
            attributes_str.push_str(entry_str.as_str());
        }

        // self.attributes_vector.insert(i, attributes.as_str());
        self.attributes_vector.insert(i, attributes_str.as_str());

        self.duckdb_chunk.set_len(i + 1);
    }
}

#[repr(C)]
pub struct GFFResult {
    pub error: *mut c_char,
    pub done: bool,
}

impl GFFResult {
    pub fn new() -> Self {
        Self {
            error: std::ptr::null_mut(),
            done: false,
        }
    }

    pub fn new_error(error: &str) -> Self {
        let error = CString::new(error).unwrap();
        Self {
            error: error.into_raw(),
            done: true,
        }
    }

    pub fn new_done() -> Self {
        Self {
            error: std::ptr::null_mut(),
            done: true,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn gff_insert_record_batch(
    gff_reader: &GFFReaderC,
    chunk_ptr: *mut c_void,
    batch_size: usize,
) -> GFFResult {
    let gff_reader_ptr = gff_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    if (gff_reader_ptr as usize) == 0 {
        return GFFResult::new_error("gff_reader is null");
    }

    let duckdb_ptr = chunk_ptr as duckdb_data_chunk;
    let mut duckdb_chunk = duckdb::vtab::DataChunk::from(duckdb_ptr);

    let mut gff_record_batch = GFFRecordBatch::new(duckdb_chunk);

    match gff_reader_ptr.as_mut() {
        Some(reader) => {
            let mut buffer = String::new();
            for i in 0..batch_size {
                match reader.read_line(&mut buffer) {
                    Ok(0) => {
                        return GFFResult::new_done();
                    }
                    Ok(_) => {
                        let line = Line::from_str(&buffer);

                        match line {
                            Ok(line) => match line {
                                Line::Record(record) => {
                                    gff_record_batch.add_record(i, record);
                                }
                                _ => {
                                    return GFFResult::new_error("Not a record");
                                }
                            },
                            Err(e) => {
                                return GFFResult::new_error(&e.to_string());
                            }
                        }

                        buffer.clear();
                    }
                    Err(e) => {
                        return GFFResult::new_error(&e.to_string());
                    }
                }
            }
            return GFFResult::new();
        }
        None => {
            return GFFResult::new_error("gff_reader is null");
        }
    }
}
