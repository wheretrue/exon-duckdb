use std::{
    ffi::{c_char, c_float, c_void, CStr, CString},
    io::{BufRead, BufReader},
    path::Path,
    ptr::null,
    str::FromStr,
};

use flate2::read::GzDecoder;
use noodles::vcf::{
    header::{
        self,
        record::value::map::Format,
        record::value::{map::Info, Map},
        Formats,
    },
    record::Genotype,
};
use noodles::vcf::{Header, Reader, Record};

mod wt_genotypes;
mod wt_int_key;
mod wt_int_list_key;
mod wt_string_key;
mod wt_string_list_key;

mod wt_float_key;

mod wt_float_list_key;

mod wt_infos;

pub use self::wt_genotypes::WTGenotypes;
use self::wt_infos::WTInfos;

impl Drop for WTGenotypes {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.genotypes as *mut Vec<Genotype>);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn next_genotype(wt_genotypes: &mut WTGenotypes) -> wt_genotypes::WTGenotype {
    let genotypes = unsafe { &mut *(wt_genotypes.genotypes as *mut Vec<Genotype>) };

    if wt_genotypes.genotype_position >= genotypes.len() as i32 {
        return wt_genotypes::WTGenotype::default();
    }

    let genotype = genotypes[wt_genotypes.genotype_position as usize].clone();

    wt_genotypes.genotype_position += 1;

    wt_genotypes::WTGenotype::new(genotype)
}

#[repr(C)]
pub struct VCFRecord {
    chromosome: *const c_char,
    ids: *const c_char, // needs to be parsed into the list.
    position: u64,
    reference_bases: *const c_char,
    alternate_bases: *const c_char,
    quality_score: c_float,
    filters: *const c_char,
    infos: WTInfos,
    genotypes: WTGenotypes,
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
            infos: WTInfos::default(),
            filters: std::ptr::null(),
            genotypes: WTGenotypes::default(),
        }
    }
}

#[repr(C)]
pub struct WTInfoFields {
    info_fields: *mut c_void,
    info_field_position: i32,
    total_fields: i32,
}

impl WTInfoFields {
    pub fn new(info_fields: Vec<Map<Info>>) -> Self {
        Self {
            info_fields: Box::into_raw(Box::new(info_fields.clone())) as *mut c_void,
            info_field_position: 0,
            total_fields: info_fields.len() as i32,
        }
    }
}

#[repr(C)]
pub struct WTInfoField {
    // info_field: *mut c_void,
    id: *const c_char,
    ty: *const c_char,
    number: *const c_char,
}

impl WTInfoField {
    pub fn new(info_field: Map<Info>) -> Self {
        let id = info_field.id().to_string();
        let ty = info_field.ty().to_string();
        let number = info_field.number().to_string();

        Self {
            id: CString::new(id).unwrap().into_raw(),
            ty: CString::new(ty).unwrap().into_raw(),
            number: CString::new(number).unwrap().into_raw(),
        }
    }
}

impl Drop for WTInfoField {
    fn drop(&mut self) {
        unsafe {
            let _ = CString::from_raw(self.id as *mut c_char);
            let _ = CString::from_raw(self.ty as *mut c_char);
            let _ = CString::from_raw(self.number as *mut c_char);
        }
    }
}

impl Default for WTInfoField {
    fn default() -> Self {
        Self {
            id: std::ptr::null(),
            ty: std::ptr::null(),
            number: std::ptr::null(),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn next_info_field(wt_info_fields: &mut WTInfoFields) -> WTInfoField {
    let info_fields = unsafe { &mut *(wt_info_fields.info_fields as *mut Vec<Map<Info>>) };

    if wt_info_fields.info_field_position >= info_fields.len() as i32 {
        return WTInfoField::default();
    }

    let info_field = info_fields[wt_info_fields.info_field_position as usize].clone();

    wt_info_fields.info_field_position += 1;

    WTInfoField::new(info_field)
}

#[repr(C)]
pub struct WTFormats {
    formats: *mut c_void,
    format_position: i32,
    total_fields: i32,
}

#[repr(C)]
pub struct WTTFormat {
    pub id: *const c_char,
    pub number: *const c_char,
    pub ty: *const c_char,
    pub description: *const c_char,
    pub error: *const c_char,
}

impl WTFormats {
    pub fn new(format: Vec<Map<Format>>) -> Self {
        Self {
            formats: Box::into_raw(Box::new(format.clone())) as *mut c_void,
            format_position: 0,
            total_fields: format.len() as i32,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn next_format(wt_formats: &mut WTFormats) -> WTTFormat {
    let formats = unsafe { &mut *(wt_formats.formats as *mut Vec<Map<Format>>) };

    if wt_formats.format_position >= formats.len() as i32 {
        return WTTFormat {
            id: std::ptr::null(),
            number: std::ptr::null(),
            ty: std::ptr::null(),
            description: std::ptr::null(),
            error: std::ptr::null(),
        };
    }

    let format = formats[wt_formats.format_position as usize].clone();
    wt_formats.format_position += 1;

    let id = CString::new(format.id().to_string()).unwrap();
    let number = CString::new(format.number().to_string()).unwrap();
    let ty = CString::new(format.ty().to_string()).unwrap();
    let description = CString::new(format.description().to_string()).unwrap();

    WTTFormat {
        id: id.into_raw(),
        number: number.into_raw(),
        ty: ty.into_raw(),
        description: description.into_raw(),
        error: std::ptr::null(),
    }
}

impl Drop for WTFormats {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.formats as *mut Formats);
        }
    }
}

#[repr(C)]
pub struct VCFReaderC {
    inner_reader: *mut c_void,
    header: *const c_char,
    formats: WTFormats,
    info_fields: WTInfoFields,
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
    let header_two = header.clone();

    let header_obj = header::Header::from_str(header_two.to_string().as_str()).unwrap();

    let formats = header_obj
        .formats()
        .into_iter()
        .map(|(_, v)| v.to_owned())
        .collect();

    let wt_formats = WTFormats::new(formats);

    let info_fields = header_obj
        .infos()
        .into_iter()
        .map(|(_, v)| v.to_owned())
        .collect();
    let wt_info_fields = WTInfoFields::new(info_fields);

    return VCFReaderC {
        inner_reader: Box::into_raw(Box::new(reader)) as *mut c_void,
        header: CString::new(header.to_string()).unwrap().into_raw(),
        formats: wt_formats,
        info_fields: wt_info_fields,
    };
}

#[no_mangle]
pub unsafe extern "C" fn get_sample_names(header: *const c_char) -> *const c_char {
    let header = CStr::from_ptr(header).to_str().unwrap();

    let header = Header::from_str(header).unwrap();

    let sample_names = header.sample_names();

    let sample_names_csv = sample_names
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>()
        .join(",");

    CString::new(sample_names_csv).unwrap().into_raw()
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

            let genotypes = record.genotypes().iter().map(|x| x.to_owned()).collect();

            let ids = record.ids();

            let passable_id = if ids.is_empty() {
                null()
            } else {
                CString::new(ids.to_string()).unwrap().into_raw()
            };

            // let infos = record.info().values().map(|f| f.to_owned()).collect();
            let infos = record.info();

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
                infos: WTInfos::new(infos),
                genotypes: WTGenotypes::new(genotypes),
            }
        }
    }
}
