use std::{
    ffi::{c_char, c_void, CStr, CString},
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

use noodles::{
    bam::Reader,
    sam::{alignment::Record, Header},
};

#[repr(C)]
pub struct BamRecordC {
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

impl Default for BamRecordC {
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
pub struct BamRecordReaderC {
    bam_reader: *mut c_void,
    bam_header: *const c_char,
}

#[no_mangle]
pub unsafe extern "C" fn bam_record_new_reader(
    filename: *const c_char,
    compression: *const c_char,
) -> BamRecordReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let _compression = CStr::from_ptr(compression).to_str().unwrap();

    let file = File::open(filename).unwrap();
    let reader = Box::new(BufReader::new(file)) as Box<dyn BufRead>;

    // Other examples don't have this as mutable.
    let mut bam_reader = Reader::new(reader);

    let header = bam_reader.read_header().unwrap();

    let bam_header = CString::new(header.to_string()).unwrap();

    let boxxed_reader = Box::into_raw(Box::new(bam_reader));

    BamRecordReaderC {
        bam_reader: boxxed_reader as *mut c_void,
        bam_header: bam_header.into_raw(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn bam_record_read_records(c_reader: &BamRecordReaderC) -> BamRecordC {
    let bam_reader_ptr = c_reader.bam_reader as *mut Reader<Box<dyn BufRead>>;

    let header = CStr::from_ptr(c_reader.bam_header).to_str().unwrap();
    let sam_header = Header::from_str(header).unwrap();

    match bam_reader_ptr.as_mut() {
        Some(reader) => {
            let mut record = Record::default();

            let bytes_read_result = reader.read_record(&sam_header, &mut record);

            eprintln!("Read bytes: {:?}", bytes_read_result);
            eprintln!("Read record: {:?}", record);

            match bytes_read_result {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        return BamRecordC::default();
                    }

                    let sequence = record.sequence().to_string();

                    let flags = record.flags();
                    let flag_bits = flags.bits();

                    let read_name = match record.read_name() {
                        Some(name) => name.to_string(),
                        None => "".to_string(),
                    };

                    let alignment_start = match record.alignment_start() {
                        Some(alignment_start) => alignment_start.get() as i64,
                        None => -1,
                    };

                    let alignment_end = match record.alignment_end() {
                        Some(alignment_end) => alignment_end.get() as i64,
                        None => -1,
                    };

                    let cigar_string = record.cigar().to_string();

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

                    return BamRecordC {
                        read_name: CString::new(read_name).unwrap().into_raw(),
                        sequence: CString::new(sequence).unwrap().into_raw(),
                        flags: flag_bits,
                        alignment_start,
                        alignment_end,
                        cigar_string: CString::new(cigar_string).unwrap().into_raw(),
                        quality_scores: CString::new(quality_score).unwrap().into_raw(),
                        template_length,
                        mapping_quality,
                        mate_alignment_start,
                    };
                }
                Err(_) => {
                    eprintln!("Error reading record.");

                    BamRecordC::default()
                }
            }
        }
        None => BamRecordC::default(),
    }
}
