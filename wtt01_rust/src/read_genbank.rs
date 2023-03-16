use std::{
    ffi::{c_char, c_void, CStr},
    io::{BufRead, BufReader},
    path::Path,
};

use gb_io::{reader::SeqReader, seq::Seq};

#[repr(C)]
pub struct GenbankReader {
    inner_reader: *mut c_void,
}

pub fn build_from_path<P>(src: P) -> std::io::Result<SeqReader<Box<dyn BufRead>>>
where
    P: AsRef<Path>,
{
    let src = src.as_ref();
    let file = std::fs::File::open(src)?;
    let reader = Box::new(BufReader::new(file)) as Box<dyn BufRead>;

    Ok(SeqReader::new(reader))
}

#[no_mangle]
pub unsafe extern "C" fn genbank_new(filename: *const c_char) -> GenbankReader {
    let filename = CStr::from_ptr(filename).to_str().unwrap();

    let reader = build_from_path(filename).unwrap();

    return GenbankReader {
        inner_reader: Box::into_raw(Box::new(reader)) as *mut c_void,
    };
}

#[no_mangle]
pub unsafe extern "C" fn genbank_free(reader: GenbankReader) {
    let _ = Box::from_raw(reader.inner_reader as *mut SeqReader<Box<dyn BufRead>>);
}

#[repr(C)]
pub struct GenbankRecord {
    pub seq: *mut c_char,
    pub accession: *mut c_char,
    pub comments: *mut c_char,
    pub contig: *mut c_char,
    pub date: *mut c_char,
    pub dblink: *mut c_char,
    pub definition: *mut c_char,
    pub division: *mut c_char,
    pub keywords: *mut c_char,
    pub molecule_type: *mut c_char,
    pub name: *mut c_char,
    pub titles: *mut c_char,
    pub source: *mut c_char,
    pub version: *mut c_char,
    pub topology: *mut c_char,
}

impl GenbankRecord {
    pub fn new() -> GenbankRecord {
        GenbankRecord {
            seq: std::ptr::null_mut(),
            accession: std::ptr::null_mut(),
            comments: std::ptr::null_mut(),
            contig: std::ptr::null_mut(),
            date: std::ptr::null_mut(),
            dblink: std::ptr::null_mut(),
            definition: std::ptr::null_mut(),
            division: std::ptr::null_mut(),
            keywords: std::ptr::null_mut(),
            molecule_type: std::ptr::null_mut(),
            name: std::ptr::null_mut(),
            titles: std::ptr::null_mut(),
            source: std::ptr::null_mut(),
            version: std::ptr::null_mut(),
            topology: std::ptr::null_mut(),
        }
    }

    pub fn from_seqrecord(record: Seq) -> GenbankRecord {
        let seq = record.seq;
        let seq = std::ffi::CString::new(seq).unwrap();

        let accession = record.accession;
        let accession = if let Some(accession) = accession {
            std::ffi::CString::new(accession).unwrap()
        } else {
            std::ffi::CString::new("").unwrap()
        };

        let comments = record.comments;
        let comments = std::ffi::CString::new(comments.join(","));

        let contig = match record.contig {
            Some(contig) => {
                let string_contig = contig.to_string();
                std::ffi::CString::new(string_contig).unwrap()
            }
            None => std::ffi::CString::new("").unwrap(),
        };

        let date = match record.date {
            Some(date) => {
                let date_string = date.to_string();
                std::ffi::CString::new(date_string).unwrap()
            }
            None => std::ffi::CString::new("").unwrap(),
        };

        let dblink = match record.dblink {
            Some(dblink) => {
                let dblink_string = dblink.to_string();
                std::ffi::CString::new(dblink_string).unwrap()
            }
            None => std::ffi::CString::new("").unwrap(),
        };

        let definition = match record.definition {
            Some(definition) => {
                let definition_string = definition.to_string();
                std::ffi::CString::new(definition_string).unwrap()
            }
            None => std::ffi::CString::new("").unwrap(),
        };

        let division = std::ffi::CString::new(record.division).unwrap();

        let keywords = match record.keywords {
            Some(keywords) => {
                let keywords_string = keywords.to_string();
                std::ffi::CString::new(keywords_string).unwrap()
            }
            None => std::ffi::CString::new("").unwrap(),
        };

        let molecule_type = match record.molecule_type {
            Some(molecule_type) => {
                let molecule_type_string = molecule_type.to_string();
                std::ffi::CString::new(molecule_type_string).unwrap()
            }
            None => std::ffi::CString::new("").unwrap(),
        };

        let name = record.name.unwrap_or(String::new());
        let name = std::ffi::CString::new(name).unwrap();

        let titles = record
            .references
            .iter()
            .map(|f| f.title.to_owned())
            .collect::<Vec<_>>()
            .join(",");
        let titles = std::ffi::CString::new(titles).unwrap();

        let source = record.source.map_or(String::new(), |f| f.source);
        let source = std::ffi::CString::new(source).unwrap();

        let version = record.version.unwrap_or(String::new());
        let version = std::ffi::CString::new(version).unwrap();

        let top = record.topology.to_string();
        let top = std::ffi::CString::new(top).unwrap();

        Self {
            accession: accession.into_raw(),
            comments: comments.unwrap().into_raw(),
            contig: contig.into_raw(),
            date: date.into_raw(),
            dblink: dblink.into_raw(),
            definition: definition.into_raw(),
            division: division.into_raw(),
            keywords: keywords.into_raw(),
            molecule_type: molecule_type.into_raw(),
            name: name.into_raw(),
            titles: titles.into_raw(),
            source: source.into_raw(),
            version: version.into_raw(),
            topology: top.into_raw(),
            seq: seq.into_raw(),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn genbank_next(reader: &GenbankReader) -> GenbankRecord {
    let reader = reader.inner_reader as *mut SeqReader<Box<dyn BufRead>>;

    match reader.as_mut() {
        Some(reader) => match reader.next() {
            Some(record) => match record {
                Ok(record) => GenbankRecord::from_seqrecord(record),
                Err(_) => GenbankRecord::new(),
            },
            _ => GenbankRecord::new(),
        },
        _ => GenbankRecord::new(),
    }
}
