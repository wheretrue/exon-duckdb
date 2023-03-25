use std::ffi::{c_char, c_void, CStr};
use std::fs::File;

use duckdb::ffi::duckdb_data_chunk;
use duckdb::vtab::Inserter;
use noodles::{bam, bgzf};

use noodles::sam::alignment::Record;
use noodles::sam::Header;

#[repr(C)]
pub struct BAMReaderC {
    bam_reader: *mut c_void,
    bam_header: *mut c_void,
    error: *const c_char,
}

#[no_mangle]
pub unsafe extern "C" fn bam_new(filename: *const c_char) -> BAMReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let file = File::open(filename);

    if let Err(e) = file {
        return BAMReaderC {
            bam_reader: std::ptr::null_mut(),
            bam_header: std::ptr::null_mut(),
            error: std::ffi::CString::new(format!("{}", e)).unwrap().into_raw(),
        };
    }
    let file = file.unwrap();

    let mut reader = bam::Reader::new(file);

    let header = match reader.read_header() {
        Ok(header) => header,
        Err(e) => {
            return BAMReaderC {
                bam_reader: std::ptr::null_mut(),
                bam_header: std::ptr::null_mut(),
                error: std::ffi::CString::new(format!("{}", e)).unwrap().into_raw(),
            }
        }
    };

    let rust_header: Header = match header.parse() {
        Ok(header) => header,
        Err(e) => {
            return BAMReaderC {
                bam_reader: std::ptr::null_mut(),
                bam_header: std::ptr::null_mut(),
                error: std::ffi::CString::new(format!("{}", e)).unwrap().into_raw(),
            }
        }
    };

    let reference_sequence = reader.read_reference_sequences();

    BAMReaderC {
        bam_reader: Box::into_raw(Box::new(reader)) as *mut c_void,
        bam_header: Box::into_raw(Box::new(rust_header)) as *mut c_void,
        error: std::ptr::null(),
    }
}

struct BamRecordBatch {
    duckdb_chunk: duckdb::vtab::DataChunk,
    sequence_vector: duckdb::vtab::FlatVector,
    read_name_vector: duckdb::vtab::FlatVector,
    flags_vector: duckdb::vtab::FlatVector,
    alignment_start_vector: duckdb::vtab::FlatVector,
    alignment_end_vector: duckdb::vtab::FlatVector,
    cigar_string_vector: duckdb::vtab::FlatVector,
    quality_scores_vector: duckdb::vtab::FlatVector,
    template_length_vector: duckdb::vtab::FlatVector,
    mapping_quality_vector: duckdb::vtab::FlatVector,
    mate_alignment_start_vector: duckdb::vtab::FlatVector,
}

impl BamRecordBatch {
    fn new(duckdb_chunk: duckdb::vtab::DataChunk) -> Self {
        let sequence_vector = duckdb_chunk.flat_vector(0);
        let read_name_vector = duckdb_chunk.flat_vector(1);
        let flags_vector = duckdb_chunk.flat_vector(2);
        let alignment_start_vector = duckdb_chunk.flat_vector(3);
        let alignment_end_vector = duckdb_chunk.flat_vector(4);
        let cigar_string_vector = duckdb_chunk.flat_vector(5);
        let quality_scores_vector = duckdb_chunk.flat_vector(6);
        let template_length_vector = duckdb_chunk.flat_vector(7);
        let mapping_quality_vector = duckdb_chunk.flat_vector(8);
        let mate_alignment_start_vector = duckdb_chunk.flat_vector(9);

        Self {
            duckdb_chunk,
            sequence_vector,
            read_name_vector,
            flags_vector,
            alignment_start_vector,
            alignment_end_vector,
            cigar_string_vector,
            quality_scores_vector,
            template_length_vector,
            mapping_quality_vector,
            mate_alignment_start_vector,
        }
    }

    fn insert(&mut self, i: usize, record: &Record) {
        let sequence_string = record.sequence().to_string();
        let sequence = sequence_string.as_str();

        self.sequence_vector.insert(i, sequence);

        match record.read_name() {
            Some(read_name) => {
                let read_name = read_name.to_string();
                self.read_name_vector.insert(i, read_name.as_str());
            }
            None => {
                self.read_name_vector.set_null(i);
            }
        }

        let flags = record.flags().bits();
        self.flags_vector.as_mut_slice::<i32>()[i] = flags as i32;

        match record.alignment_start() {
            Some(alignment_start) => {
                let alignment_start = alignment_start.get();
                self.alignment_start_vector.as_mut_slice::<i64>()[i] = alignment_start as i64;
            }
            None => {
                self.alignment_start_vector.set_null(i);
            }
        }

        match record.alignment_end() {
            Some(alignment_end) => {
                let alignment_end = alignment_end.get();
                self.alignment_end_vector.as_mut_slice::<i64>()[i] = alignment_end as i64;
            }
            None => {
                self.alignment_end_vector.set_null(i);
            }
        }

        let cigar = record.cigar();
        match cigar.is_empty() {
            true => {
                self.cigar_string_vector.set_null(i);
            }
            false => {
                let cigar_string = cigar.to_string();
                self.cigar_string_vector.insert(i, cigar_string.as_str());
            }
        }

        let quality_scores = record.quality_scores().to_string();
        match quality_scores.is_empty() {
            true => {
                self.quality_scores_vector.set_null(i);
            }
            false => {
                self.quality_scores_vector
                    .insert(i, quality_scores.as_str());
            }
        }

        self.template_length_vector.as_mut_slice::<i64>()[i] = record.template_length() as i64;

        let mapping_quality = record.mapping_quality();
        match mapping_quality {
            Some(mapping_quality) => {
                let mapping_quality = mapping_quality.get();
                self.mapping_quality_vector.as_mut_slice::<i32>()[i] = mapping_quality as i32;
            }
            None => {
                self.mapping_quality_vector.set_null(i);
            }
        }

        match record.mate_alignment_start() {
            Some(mate_alignment_start) => {
                let mate_alignment_start = mate_alignment_start.get();
                self.mate_alignment_start_vector.as_mut_slice::<i64>()[i] =
                    mate_alignment_start as i64;
            }
            None => {
                self.mate_alignment_start_vector.set_null(i);
            }
        }

        self.duckdb_chunk.set_len(i + 1);
    }
}

#[no_mangle]
pub unsafe extern "C" fn bam_next(
    bam_reader: &mut BAMReaderC,
    chunk_ptr: *mut c_void,
    done: &mut bool,
    chunk_size: usize,
) {
    let bam_reader_ptr = bam_reader.bam_reader as *mut bam::Reader<bgzf::Reader<File>>;

    if (bam_reader_ptr as usize) == 0 {
        eprintln!("bam_next: bam_reader is null");
    }

    let header = bam_reader.bam_header as *const Header;
    let header = &*header;

    let duckdb_ptr = chunk_ptr as duckdb_data_chunk;
    let duckdb_chunk = duckdb::vtab::DataChunk::from(duckdb_ptr);

    let mut bam_record_batch = BamRecordBatch::new(duckdb_chunk);

    match bam_reader_ptr.as_mut() {
        Some(reader) => {
            let mut i = 0;
            let mut record = Record::default();

            while i < chunk_size {
                match reader.read_record(&header, &mut record) {
                    Ok(read_bytes) => {
                        if read_bytes == 0 {
                            *done = true;
                            break;
                        }

                        bam_record_batch.insert(i, &record);

                        i += 1;
                    }
                    Err(e) => {
                        eprintln!("bam_next: {}", e);
                        *done = true;
                        break;
                    }
                }
            }
        }
        None => {
            eprintln!("bam_next: bam_reader is null");
        }
    }
}
