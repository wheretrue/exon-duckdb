use std::{
    ffi::{c_char, c_void, CStr, CString},
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

use noodles::sam::{alignment::Record, Header, Reader};

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
    sam_header: *const c_char,
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

    let header = sam_reader.read_header().unwrap();

    let sam_header = CString::new(header.to_string()).unwrap();

    let boxxed_reader = Box::into_raw(Box::new(sam_reader));

    SamRecordReaderC {
        sam_reader: boxxed_reader as *mut c_void,
        sam_header: sam_header.into_raw(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sam_record_read_records(c_reader: &SamRecordReaderC) -> SamRecordC {
    let sam_reader_ptr = c_reader.sam_reader as *mut Reader<Box<dyn BufRead>>;

    let sam_header_str = CStr::from_ptr(c_reader.sam_header)
        .to_str()
        .unwrap_or_else(|_| {
            eprintln!("Error: sam_header is not a valid UTF-8 string");
            std::process::exit(1);
        });

    let sam_header = Header::from_str(sam_header_str).unwrap();

    match sam_reader_ptr.as_mut() {
        Some(sam_reader) => {
            let mut record = Record::default();

            match sam_reader.read_record(&sam_header, &mut record) {
                Ok(byt) => {
                    if byt == 0 {
                        return SamRecordC::default();
                    }
                }
                Err(_) => {
                    return SamRecordC::default();
                }
            }

            let sequence = record.sequence().to_string();

            let read_name = match record.read_name() {
                Some(name) => name.to_string(),
                None => "".to_string(),
            };

            let flags = record.flags();
            let flag_bits = flags.bits();

            let alignment_start = match record.alignment_start() {
                Some(start) => start.get() as i64,
                None => -1,
            };

            let alignment_end = match record.alignment_end() {
                Some(end) => end.get() as i64,
                None => -1,
            };

            let cigar = record.cigar();
            let cigar_string = match cigar.is_empty() {
                true => std::ptr::null(),
                false => CString::new(cigar.to_string()).unwrap().into_raw(),
            };

            let quality_score = record
                .quality_scores()
                .as_ref()
                .into_iter()
                .map(|q| q.to_string())
                .collect::<Vec<String>>()
                .join("");

            let template_length = record.template_length() as i64;

            let mapping_quality = match record.mapping_quality() {
                Some(q) => q.get(),
                None => 0,
            } as i64;

            let mate_alignment_start = match record.mate_alignment_start() {
                Some(start) => start.get() as i64,
                None => -1,
            };

            // TODO: Add reference id
            // let mate_reference_sequence_id = match record.mate_reference_sequence_id() {
            //     Some(id) => id.get(),
            //     None => -1,
            // };
            // record.data()

            SamRecordC {
                read_name: CString::new(read_name).unwrap().into_raw(),
                sequence: CString::new(sequence).unwrap().into_raw(),
                flags: flag_bits,
                alignment_start,
                alignment_end,
                cigar_string,
                quality_scores: CString::new(quality_score).unwrap().into_raw(),
                template_length,
                mapping_quality,
                mate_alignment_start,
            }
        }
        None => SamRecordC::default(),
    }
}
