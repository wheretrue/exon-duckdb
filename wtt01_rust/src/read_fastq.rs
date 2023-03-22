use std::ffi::{c_char, c_void, CStr};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str;

use duckdb::ffi::duckdb_data_chunk;
use duckdb::vtab::Inserter;
use flate2::read::GzDecoder;
use noodles::fastq::Reader;

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

#[repr(C)]
pub struct FASTQReaderC {
    inner_reader: *mut c_void,
}

#[no_mangle]
pub unsafe extern "C" fn fastq_new(
    filename: *const c_char,
    compression: *const c_char,
) -> FASTQReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let compression = CStr::from_ptr(compression).to_str().unwrap();

    let reader = build_from_path(filename, compression).unwrap();

    return FASTQReaderC {
        inner_reader: Box::into_raw(Box::new(reader)) as *mut c_void,
    };
}

#[no_mangle]
pub unsafe extern "C" fn fastq_next(
    fastq_reader: &FASTQReaderC,
    chunk_ptr: *mut c_void,
    done: &mut bool,
    batch_size: usize,
) {
    let reader_ptr = fastq_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    if (reader_ptr as usize) == 0 {
        eprintln!("fastq_next: fastq_reader is null");
    }

    let duckdb_ptr = chunk_ptr as duckdb_data_chunk;
    let duckdb_chunk = duckdb::vtab::DataChunk::from(duckdb_ptr);

    let name_vector = duckdb_chunk.flat_vector(0);
    let description_vector = duckdb_chunk.flat_vector(1);
    let sequence_vector = duckdb_chunk.flat_vector(2);
    let quality_score_vector = duckdb_chunk.flat_vector(3);

    match reader_ptr.as_mut() {
        Some(reader) => {
            for i in 0..batch_size {
                let mut record = noodles::fastq::Record::default();
                let record_read_result = reader.read_record(&mut record);

                match record_read_result {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            *done = true;
                            break;
                        }

                        let name = str::from_utf8(record.name()).unwrap();
                        name_vector.insert(i, name);

                        let description = str::from_utf8(record.description()).unwrap();
                        description_vector.insert(i, description);

                        let sequence = str::from_utf8(record.sequence()).unwrap();
                        sequence_vector.insert(i, sequence);

                        let quality_scores = str::from_utf8(record.quality_scores()).unwrap();
                        quality_score_vector.insert(i, quality_scores);

                        duckdb_chunk.set_len(i + 1);
                    }
                    _ => {
                        *done = true;
                        break;
                    }
                }
            }
        }
        None => {
            *done = true;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn fastq_free(fastq_reader: FASTQReaderC) {
    let reader_ptr = fastq_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    if (reader_ptr as usize) == 0 {
        eprintln!("fastq_free: fastq_reader is null");
    }

    match reader_ptr.as_mut() {
        Some(reader) => {
            let _ = Box::from_raw(reader);
        }
        None => {
            eprintln!("fastq_free: fastq_reader is null");
        }
    }
}
