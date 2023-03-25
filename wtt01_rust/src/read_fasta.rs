use std::{
    ffi::{c_char, c_void, CStr, CString},
    io::{BufRead, BufReader},
    path::Path,
};

use duckdb::{ffi::duckdb_data_chunk, vtab::Inserter};
use flate2::read::GzDecoder;
use noodles::fasta::Reader;

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
pub struct FASTAReaderC {
    inner_reader: *mut c_void,
    error: *const c_char,
}

#[no_mangle]
pub unsafe extern "C" fn fasta_new(
    filename: *const c_char,
    compression: *const c_char,
) -> FASTAReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let compression = CStr::from_ptr(compression).to_str().unwrap();

    match build_from_path(filename, compression) {
        Ok(reader) => {
            let inner_reader = Box::into_raw(Box::new(reader));

            return FASTAReaderC {
                inner_reader: inner_reader as *mut c_void,
                error: std::ptr::null(),
            };
        }
        Err(e) => {
            return FASTAReaderC {
                inner_reader: std::ptr::null_mut(),
                error: CString::new(e.to_string()).unwrap().into_raw(),
            };
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn fasta_next(
    fasta_reader: &FASTAReaderC,
    chunk_ptr: *mut c_void,
    done: &mut bool,
    batch_size: usize,
) {
    let fasta_reader_ptr = fasta_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    if (fasta_reader_ptr as usize) == 0 {
        eprintln!("fasta_next: fasta_reader is null");
    }

    let duckdb_ptr = chunk_ptr as duckdb_data_chunk;
    let duckdb_chunk = duckdb::vtab::DataChunk::from(duckdb_ptr);

    let id_vector = duckdb_chunk.flat_vector(0);
    let mut description_vector = duckdb_chunk.flat_vector(1);
    let sequence_vector = duckdb_chunk.flat_vector(2);

    match fasta_reader_ptr.as_mut() {
        Some(reader) => {
            for i in 0..batch_size {
                let mut full_definition = String::new();
                let mut sequence = Vec::new();

                match reader.read_definition(&mut full_definition) {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            *done = true;
                            return;
                        }
                    }
                    Err(e) => {
                        eprintln!("fasta_next: error reading definition: {}", e);
                        *done = true;
                        return;
                    }
                }

                match reader.read_sequence(&mut sequence) {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            *done = true;
                            return;
                        }
                    }
                    Err(e) => {
                        *done = true;
                        return;
                    }
                }

                match full_definition.strip_prefix(">") {
                    Some(definition) => match definition.split_once(" ") {
                        Some((id, description)) => {
                            let sequence_str = std::str::from_utf8(&sequence).unwrap();
                            sequence_vector.insert(i, sequence_str);

                            id_vector.insert(i, id);
                            description_vector.insert(i, description);

                            duckdb_chunk.set_len(i + 1);
                        }
                        None => {
                            let sequence_str = std::str::from_utf8(&sequence).unwrap();
                            sequence_vector.insert(i, sequence_str);
                            id_vector.insert(i, definition);

                            description_vector.set_null(i);

                            duckdb_chunk.set_len(i + 1);
                        }
                    },
                    None => {
                        *done = true;
                    }
                }
            }
        }
        None => {
            *done = true;
            return;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn fasta_free(fasta_reader: &mut FASTAReaderC) {
    let fasta_reader_ptr = fasta_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    if (fasta_reader_ptr as usize) == 0 {
        eprintln!("fasta_free: fasta_reader is null");
    }

    match fasta_reader_ptr.as_mut() {
        Some(reader) => {
            let _ = Box::from_raw(reader);
        }
        None => {
            eprintln!("fasta_free: fasta_reader is null");
        }
    }
}
