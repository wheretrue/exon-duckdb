use std::{
    ffi::{c_char, c_float, c_void, CStr, CString},
    io::{BufRead, BufReader},
    path::Path,
    ptr::null,
    str::FromStr,
};

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
    header: *const c_char,
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
    let header = reader.read_header().unwrap();

    return VCFReaderC {
        inner_reader: Box::into_raw(Box::new(reader)) as *mut c_void,
        header: CString::new(header.to_string()).unwrap().into_raw(),
    };
}

#[no_mangle]
pub unsafe extern "C" fn vcf_next(vcf_reader: &VCFReaderC) -> VCFRecord {
    let vcf_reader_ptr = vcf_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    let header_string = CStr::from_ptr(vcf_reader.header).to_str().unwrap();
    let header = Header::from_str(header_string).unwrap();

    match vcf_reader_ptr.as_mut() {
        None => {
            eprintln!("vcf_next: vcf_reader_ptr is null");
            return VCFRecord::default();
        }
        Some(reader) => {
            let mut buffer = String::new();

            let line_read_result = reader.read_record(&mut buffer);

            if line_read_result.is_err() {
                return VCFRecord::default();
            }

            let bytes_read = line_read_result.unwrap();
            if bytes_read == 0 {
                return VCFRecord::default();
            }

            let record = Record::try_from_str(&buffer, &header).unwrap();

            let quality_score = match record.quality_score() {
                Some(quality_score) => f32::from(quality_score),
                None => f32::NAN,
            };

            let filters = match record.filters() {
                Some(filters) => CString::new(filters.to_string()).unwrap().into_raw(),
                None => null(),
            };

            let ids = record.ids();
            let passable_id = if ids.is_empty() {
                null()
            } else {
                CString::new(ids.to_string()).unwrap().into_raw()
            };

            let genotypes = record.genotypes();
            let mut genotypes_string = String::new();

            if genotypes.len() != 0 {
                genotypes_string.push_str(&genotypes.to_string());
            }

            VCFRecord {
                chromosome: CString::new(record.chromosome().to_string())
                    .unwrap()
                    .into_raw(),
                ids: passable_id,
                position: usize::from(record.position()) as u64,
                reference_bases: CString::new(record.reference_bases().to_string())
                    .unwrap()
                    .into_raw(),
                alternate_bases: CString::new(record.alternate_bases().to_string())
                    .unwrap()
                    .into_raw(),
                quality_score,
                filters,
                infos: CString::new(record.info().to_string()).unwrap().into_raw(),
                genotypes: CString::new(genotypes_string).unwrap().into_raw(),
            }
        }
    }
}
