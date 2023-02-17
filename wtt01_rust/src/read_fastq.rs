use std::ffi::{c_char, c_void, CStr, CString};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str;

use flate2::read::GzDecoder;
use noodles::fastq::Reader;

#[repr(C)]
pub struct FastqRecord {
    pub name: *const c_char,
    pub description: *const c_char,
    pub sequence: *const c_char,
    pub quality_scores: *const c_char,
}

// Implement new for Record.
impl FastqRecord {
    pub fn new(
        name: String,
        description: String,
        sequence: String,
        quality_scores: String,
    ) -> Self {
        Self {
            name: CString::new(name).unwrap().into_raw(),
            description: CString::new(description).unwrap().into_raw(),
            sequence: CString::new(sequence).unwrap().into_raw(),
            quality_scores: CString::new(quality_scores).unwrap().into_raw(),
        }
    }
}

// Implement default for Record.
impl Default for FastqRecord {
    fn default() -> Self {
        Self {
            name: std::ptr::null(),
            description: std::ptr::null(),
            sequence: std::ptr::null(),
            quality_scores: std::ptr::null(),
        }
    }
}

// Implement drop for Record.
impl Drop for FastqRecord {
    fn drop(&mut self) {
        unsafe {
            let _ = CString::from_raw(self.name as *mut c_char);
            let _ = CString::from_raw(self.description as *mut c_char);
            let _ = CString::from_raw(self.sequence as *mut c_char);
            let _ = CString::from_raw(self.quality_scores as *mut c_char);
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
pub unsafe extern "C" fn fastq_next(fastq_reader: &FASTQReaderC) -> FastqRecord {
    let reader_ptr = fastq_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    if (reader_ptr as usize) == 0 {
        eprintln!("fastq_next: fastq_reader is null");
    }

    match reader_ptr.as_mut() {
        Some(reader) => {
            let mut record = noodles::fastq::Record::default();
            let record_read_result = reader.read_record(&mut record);

            match record_read_result {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        return FastqRecord::default();
                    }

                    let name_str = str::from_utf8(record.name()).unwrap().to_string();
                    let description = str::from_utf8(record.description()).unwrap().to_string();
                    let sequence = str::from_utf8(record.sequence()).unwrap().to_string();
                    let quality_scores =
                        str::from_utf8(record.quality_scores()).unwrap().to_string();

                    FastqRecord::new(name_str, description, sequence, quality_scores)
                }
                Err(e) => {
                    eprintln!("fastq_next: error={}", e);
                    FastqRecord::default()
                }
            }
        }
        None => {
            return FastqRecord::default();
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

#[no_mangle]
pub unsafe extern "C" fn fastq_record_free(record: FastqRecord) {
    drop(record)
}