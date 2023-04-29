use std::{
    ffi::{c_char, c_void, CStr},
    fs::File,
    io::{BufRead, BufReader},
};

use noodles::sam::{alignment::Record, Header, Reader};

use duckdb::{
    self,
    ffi::duckdb_data_chunk,
    vtab::{FlatVector, Inserter},
};

#[repr(C)]
pub struct SamRecordC {
    sequence: *const c_char,
    read_name: *const c_char,
    flags: u16,
    alignment_start: i64,
    alignment_end: i64,
    cigar_string: *const c_char,
    quality_scores: *const c_char,
    template_length: i64,
    mapping_quality: i64,
    mate_alignment_start: i64,
}

impl Default for SamRecordC {
    fn default() -> Self {
        Self {
            sequence: std::ptr::null(),
            read_name: std::ptr::null(),
            flags: 0,
            alignment_start: -1,
            alignment_end: -1,
            cigar_string: std::ptr::null(),
            quality_scores: std::ptr::null(),
            template_length: -1,
            mapping_quality: -1,
            mate_alignment_start: -1,
        }
    }
}

#[repr(C)]
pub struct SamRecordReaderC {
    sam_reader: *mut c_void,
    sam_header: *const c_void,
}

#[no_mangle]
pub unsafe extern "C" fn sam_record_new_reader(
    filename: *const c_char,
    compression: *const c_char,
) -> SamRecordReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let _compression = CStr::from_ptr(compression).to_str().unwrap();

    let file = File::open(filename).unwrap();
    let reader = Box::new(BufReader::new(file)) as Box<dyn BufRead>;

    // Other examples don't have this as mutable.
    let mut sam_reader = Reader::new(reader);

    let header: Header = sam_reader.read_header().unwrap().parse().unwrap();

    let boxxed_reader = Box::into_raw(Box::new(sam_reader));

    SamRecordReaderC {
        sam_reader: boxxed_reader as *mut c_void,
        sam_header: Box::into_raw(Box::new(header)) as *mut c_void,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sam_record_read_records_chunk(
    c_reader: &SamRecordReaderC,
    ptr: *mut c_void,
    done: &mut bool,
    batch_size: usize,
) {
    let sam_reader_ptr = c_reader.sam_reader as *mut Reader<Box<dyn BufRead>>;

    let sam_header = c_reader.sam_header as *const Header;
    let sam_header = &*sam_header;

    let duckdb_ptr = ptr as duckdb_data_chunk;
    let duckdb_chunk = duckdb::vtab::DataChunk::from(duckdb_ptr);

    // sequences is a vector of strings
    let sequences = duckdb_chunk.flat_vector(0);
    let read_name: FlatVector = duckdb_chunk.flat_vector(1);
    let mut flags = duckdb_chunk.flat_vector(2);
    let mut alignment_start = duckdb_chunk.flat_vector(3);
    let mut alignment_end = duckdb_chunk.flat_vector(4);
    let mut cigar_vector: FlatVector = duckdb_chunk.flat_vector(5);
    let mut quality_scores_vector: FlatVector = duckdb_chunk.flat_vector(6);
    let mut template_length_vector = duckdb_chunk.flat_vector(7);
    let mut mapping_quality_vector = duckdb_chunk.flat_vector(8);
    let mut mate_alignment_start_vector = duckdb_chunk.flat_vector(9);

    match sam_reader_ptr.as_mut() {
        Some(sam_reader) => {
            for i in 0..batch_size {
                let mut record = Record::default();

                match sam_reader.read_record(&sam_header, &mut record) {
                    Ok(byt) => {
                        if byt == 0 {
                            *done = true;
                            return;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading record. {}", e);
                        *done = true;
                        return;
                    }
                }

                let sequence = record.sequence().to_string();
                sequences.insert(i, sequence.as_str());

                read_name.insert(i, record.read_name().unwrap().as_ref());

                flags.as_mut_slice::<i32>()[i] = record.flags().bits() as i32;

                // let alignment_start = record.alignment_start();
                match record.alignment_start() {
                    Some(alignment_start_position) => {
                        alignment_start.as_mut_slice::<i64>()[i] =
                            alignment_start_position.get() as i64;
                    }
                    None => alignment_start.set_null(i),
                }

                match record.alignment_end() {
                    Some(alignment_end_position) => {
                        alignment_end.as_mut_slice::<i64>()[i] =
                            alignment_end_position.get() as i64;
                    }
                    None => alignment_end.set_null(i),
                }

                let cigar_string = record.cigar();
                match cigar_string.is_empty() {
                    true => cigar_vector.set_null(i),
                    false => {
                        cigar_vector.insert(i, cigar_string.to_string().as_ref());
                    }
                }

                let quality_scores = record.quality_scores().to_string();
                match quality_scores.is_empty() {
                    true => quality_scores_vector.set_null(i),
                    false => {
                        quality_scores_vector.insert(i, quality_scores.as_ref());
                    }
                }

                template_length_vector.as_mut_slice::<i64>()[i] = record.template_length() as i64;

                let mapping_quality = record.mapping_quality();
                match mapping_quality {
                    Some(mapping_quality) => {
                        mapping_quality_vector.as_mut_slice::<i32>()[i] =
                            mapping_quality.get() as i32;
                    }
                    None => mapping_quality_vector.set_null(i),
                }

                let mate_alignment_start = record.mate_alignment_start();
                match mate_alignment_start {
                    Some(mate_alignment_start) => {
                        mate_alignment_start_vector.as_mut_slice::<i64>()[i] =
                            mate_alignment_start.get() as i64;
                    }
                    None => mate_alignment_start_vector.set_null(i),
                }

                duckdb_chunk.set_len(i + 1);
            }
        }
        None => {
            *done = true;
        }
    }
}
