use std::{
    ffi::{c_char, c_void, CStr, CString},
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

use noodles::sam::{Header, Reader};

#[repr(C)]
pub struct HeaderRecord {
    pub record_type: String,
    pub tag: Option<String>,
    pub value: String,
}

#[repr(C)]
pub struct HeaderRecordC {
    pub record_type: *const c_char,
    pub tag: *const c_char,
    pub value: *const c_char,
}

impl Default for HeaderRecordC {
    fn default() -> Self {
        Self {
            record_type: std::ptr::null(),
            tag: std::ptr::null(),
            value: std::ptr::null(),
        }
    }
}

#[repr(C)]
pub struct SamHeaderReaderC {
    inner_reader: *mut c_void,
}

#[no_mangle]
pub unsafe extern "C" fn sam_header_new_reader(
    filename: *const c_char,
    compression: *const c_char,
) -> SamHeaderReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let _compression = CStr::from_ptr(compression).to_str().unwrap();

    let reader = Box::new(File::open(filename).map(BufReader::new).unwrap()) as Box<dyn BufRead>;

    let mut sam_reader = Reader::new(reader);

    let header = sam_reader.read_header().unwrap();
    let parsed_header = Header::from_str(header.as_str()).unwrap();

    let mut records = vec![];

    // TODO: what is .header

    // Grab the reference sequences.
    for (reference_sequence_name, reference_sequence_value) in parsed_header.reference_sequences() {
        records.push(HeaderRecord {
            record_type: "SQ".to_string(),
            tag: None, // TODO, should be name?
            value: reference_sequence_name.to_string(),
        });
    }

    // Grab the read groups.
    for read_group in parsed_header.read_groups() {
        records.push(HeaderRecord {
            record_type: "RG".to_string(),
            tag: None, // TODO, should be name?
            value: read_group.0.to_string(),
        });
    }

    // Grab the program.
    for program in parsed_header.programs() {
        records.push(HeaderRecord {
            record_type: "PG".to_string(),
            tag: None, // TODO, should be name?
            value: program.0.to_string(),
        });
    }

    // Grab the comments.
    let comments = parsed_header.comments();
    for c in comments {
        records.push(HeaderRecord {
            record_type: "CO".to_string(),
            tag: None,
            value: c.to_string(),
        });
    }

    // Return the records through SamHeaderReaderC.
    return SamHeaderReaderC {
        inner_reader: Box::into_raw(Box::new(records.into_iter())) as *mut c_void,
    };
}

#[no_mangle]
pub unsafe extern "C" fn sam_header_read_records(c_reader: &SamHeaderReaderC) -> HeaderRecordC {
    let reader = &mut *(c_reader.inner_reader as *mut std::vec::IntoIter<HeaderRecord>);

    let record = reader.next();

    if record.is_none() {
        return HeaderRecordC::default();
    }

    let record = record.unwrap();

    let c_record = HeaderRecordC {
        record_type: CString::new(record.record_type).unwrap().into_raw(),
        tag: match record.tag {
            Some(t) => CString::new(t).unwrap().into_raw(),
            None => std::ptr::null(),
        },
        value: CString::new(record.value).unwrap().into_raw(),
    };

    c_record
}
