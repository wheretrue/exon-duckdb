use std::{
    ffi::{c_char, c_float, c_void, CStr, CString},
    io::{BufRead, BufReader},
    path::Path,
};

use duckdb::ffi::duckdb_data_chunk;
use duckdb::vtab::Inserter;
use flate2::read::GzDecoder;

use noodles::vcf::{Header, Reader, Record};

#[repr(C)]
pub struct VCFRecord {
    chromosome: *const c_char,
    ids: *const c_char, // needs to be parsed into the list.
    position: u64,
    reference_bases: *const c_char,
    alternate_bases: *const c_char,
    quality_score: c_float,
    filters: *const c_char,
    infos: *const c_char,
    genotypes: *const c_char,
}

// Implement default for Record.
impl Default for VCFRecord {
    fn default() -> Self {
        Self {
            chromosome: std::ptr::null(),
            ids: std::ptr::null(),
            position: 0,
            reference_bases: std::ptr::null(),
            alternate_bases: std::ptr::null(),
            quality_score: f32::NAN,
            infos: std::ptr::null(),
            filters: std::ptr::null(),
            genotypes: std::ptr::null(),
        }
    }
}

#[repr(C)]
pub struct VCFReaderC {
    inner_reader: *mut c_void,
    header: *mut c_void,
    error: *const c_char,
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
pub unsafe extern "C" fn vcf_new(
    filename: *const c_char,
    compression: *const c_char,
) -> VCFReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let compression = CStr::from_ptr(compression).to_str().unwrap();

    let mut reader = build_from_path(filename, compression).unwrap();

    // let header = reader.read_header().unwrap();

    match reader.read_header() {
        Ok(header) => {
            let header: Header = header.parse().unwrap();
            let reader = Box::into_raw(Box::new(reader)) as *mut c_void;

            VCFReaderC {
                inner_reader: reader,
                header: Box::into_raw(Box::new(header)) as *mut c_void,
                error: std::ptr::null(),
            }
        }
        Err(e) => {
            let error = CString::new(e.to_string()).unwrap().into_raw();
            VCFReaderC {
                inner_reader: std::ptr::null_mut(),
                header: std::ptr::null_mut(),
                error,
            }
        }
    }
}

pub struct VCFRecordBatch {
    duckdb_chunk: duckdb::vtab::DataChunk,

    chromosome_vector: duckdb::vtab::FlatVector,
    ids_vector: duckdb::vtab::FlatVector,
    position_vector: duckdb::vtab::FlatVector,
    reference_bases_vector: duckdb::vtab::FlatVector,
    alternate_bases_vector: duckdb::vtab::FlatVector,
    quality_score_vector: duckdb::vtab::FlatVector,
    filters_vector: duckdb::vtab::FlatVector,
    infos_vector: duckdb::vtab::FlatVector,
    genotypes_vector: duckdb::vtab::FlatVector,
}

impl VCFRecordBatch {
    pub fn new(duckdb_chunk: duckdb::vtab::DataChunk) -> Self {
        let chromosome_vector = duckdb_chunk.flat_vector(0);
        let ids_vector = duckdb_chunk.flat_vector(1);
        let position_vector = duckdb_chunk.flat_vector(2);
        let reference_bases_vector = duckdb_chunk.flat_vector(3);
        let alternate_bases_vector = duckdb_chunk.flat_vector(4);
        let quality_score_vector = duckdb_chunk.flat_vector(5);
        let filters_vector = duckdb_chunk.flat_vector(6);
        let infos_vector = duckdb_chunk.flat_vector(7);
        let genotypes_vector = duckdb_chunk.flat_vector(8);

        Self {
            duckdb_chunk,
            chromosome_vector,
            ids_vector,
            position_vector,
            reference_bases_vector,
            alternate_bases_vector,
            quality_score_vector,
            filters_vector,
            infos_vector,
            genotypes_vector,
        }
    }

    pub fn add_record(&mut self, i: usize, record: Record) {
        // CHROM
        let chromosome = record.chromosome().to_string();
        self.chromosome_vector.insert(i, chromosome.as_str());

        // IDs
        let ids = record.ids();
        if ids.is_empty() {
            self.ids_vector.set_null(i);
        } else {
            let str_ids = ids.to_string();
            self.ids_vector.insert(i, str_ids.as_str());
        }

        // POS
        let position = usize::from(record.position());
        self.position_vector.as_mut_slice::<i64>()[i] = position as i64;

        // REF
        let reference_bases = record.reference_bases().to_string();
        self.reference_bases_vector
            .insert(i, reference_bases.as_str());

        // ALT
        let alternate_bases = record.alternate_bases().to_string();
        self.alternate_bases_vector
            .insert(i, alternate_bases.as_str());

        // QUAL
        let quality_score = record.quality_score();
        match record.quality_score() {
            Some(quality_score) => {
                self.quality_score_vector.as_mut_slice::<f32>()[i] = f32::from(quality_score);
            }
            None => {
                self.quality_score_vector.set_null(i);
            }
        }

        // FILTER
        match record.filters() {
            Some(filters) => {
                let filters = filters.to_string();
                self.filters_vector.insert(i, filters.as_str());
            }
            None => {
                self.filters_vector.set_null(i);
            }
        }

        // INFO
        let infos = record.info();
        if infos.is_empty() {
            self.infos_vector.set_null(i);
        } else {
            let infos = infos.to_string();
            self.infos_vector.insert(i, infos.as_str());
        }

        let genotypes = record.genotypes();
        if genotypes.is_empty() {
            self.genotypes_vector.set_null(i);
        } else {
            let genotypes = genotypes.to_string();
            self.genotypes_vector.insert(i, genotypes.as_str());
        }

        self.duckdb_chunk.set_len(i + 1);
    }
}

#[no_mangle]
pub unsafe extern "C" fn vcf_next(
    vcf_reader: &mut VCFReaderC,
    chunk_ptr: *mut c_void,
    done: &mut bool,
    chunk_size: usize,
) {
    let vcf_reader_ptr = vcf_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;
    let header = vcf_reader.header as *mut Header;

    match vcf_reader_ptr.as_mut() {
        None => {
            eprintln!("vcf_next: vcf_reader_ptr is null");
            *done = true;
            return;
        }
        Some(reader) => {
            let duckdb_ptr = chunk_ptr as duckdb_data_chunk;
            let duckdb_chunk = duckdb::vtab::DataChunk::from(duckdb_ptr);
            let mut vcf_record_batch = VCFRecordBatch::new(duckdb_chunk);

            let mut row = 0;

            while row < chunk_size {
                let mut buffer = String::new();
                let line_read_result = reader.read_record(&mut buffer);
                if line_read_result.is_err() {
                    *done = true;
                    return;
                }

                let bytes_read = line_read_result.unwrap();
                if bytes_read == 0 {
                    *done = true;
                    return;
                }

                let record = match Record::try_from_str(&buffer, &*header) {
                    Ok(record) => record,
                    Err(error) => {
                        eprintln!("vcf_next: error parsing record: {}", error);
                        *done = true;
                        return;
                    }
                };

                vcf_record_batch.add_record(row, record);
                row = row + 1;
            }
        }
    }
}
