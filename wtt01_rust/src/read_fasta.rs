use std::{
    ffi::{c_char, c_void, CStr, CString},
    io::{BufRead, BufReader},
    path::Path,
};

use flate2::read::GzDecoder;
use noodles::fasta::Reader;

#[repr(C)]
pub struct Record {
    pub id: *const c_char,
    pub description: *const c_char,
    pub sequence: *const c_char,
    pub done: bool,
}

// Implement new for Record.
impl Record {
    pub fn new(id: String, description: String, sequence: String) -> Self {
        Self {
            id: CString::new(id).unwrap().into_raw(),
            description: CString::new(description).unwrap().into_raw(),
            sequence: CString::new(sequence).unwrap().into_raw(),
            done: false,
        }
    }
}

// Implement default for Record.
impl Default for Record {
    fn default() -> Self {
        Self {
            id: std::ptr::null(),
            description: std::ptr::null(),
            sequence: std::ptr::null(),
            done: false,
        }
    }
}

// Implement drop for Record.
impl Drop for Record {
    fn drop(&mut self) {
        unsafe {
            let _ = CString::from_raw(self.id as *mut c_char);
            let _ = CString::from_raw(self.description as *mut c_char);
            let _ = CString::from_raw(self.sequence as *mut c_char);
        }
    }
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
            eprintln!("fasta_new: error={}", e);
            return FASTAReaderC {
                inner_reader: std::ptr::null_mut(),
                error: CString::new(e.to_string()).unwrap().into_raw(),
            };
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn fasta_next(fasta_reader: &FASTAReaderC, record: &mut Record) {
    let fasta_reader_ptr = fasta_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    if (fasta_reader_ptr as usize) == 0 {
        eprintln!("fasta_next: fasta_reader is null");
    }

    match fasta_reader_ptr.as_mut() {
        Some(reader) => {
            let mut full_definition = String::new();

            let mut sequence = Vec::new();

            match reader.read_definition(&mut full_definition) {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        record.done = true;
                        return;
                    }
                }
                Err(e) => {
                    eprintln!("fasta_next: error={}\n", e);
                    record.done = true;
                    return;
                }
            }

            match reader.read_sequence(&mut sequence) {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        record.done = true;
                        return;
                    }
                }
                Err(e) => {
                    eprintln!("fasta_next: error={}", e);
                    record.done = true;
                    return;
                }
            }

            match full_definition.strip_prefix(">") {
                Some(definition) => match definition.split_once(" ") {
                    Some((id, description)) => {
                        record.id = CString::new(id).unwrap().into_raw();
                        record.description = CString::new(description).unwrap().into_raw();
                        record.sequence = CString::new(sequence).unwrap().into_raw();

                        record.done = false;
                    }
                    None => {
                        record.id = CString::new(definition).unwrap().into_raw();
                        record.description = CString::new("").unwrap().into_raw();
                        record.sequence = CString::new(sequence).unwrap().into_raw();

                        record.done = false;
                    }
                },
                None => {
                    record.done = true;
                    return;
                }
            }
        }
        None => {
            record.done = true;
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

#[no_mangle]
pub unsafe extern "C" fn fasta_record_free(record: Record) {
    drop(record)
}
