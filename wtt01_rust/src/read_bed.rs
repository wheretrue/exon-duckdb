use flate2::read::GzDecoder;
use std::{
    ffi::{c_char, c_void, CStr, CString},
    io::{BufRead, BufReader},
    path::Path,
    str::FromStr,
};

use noodles::{
    bed::{
        record::{Name, Score, Strand},
        Reader, Record,
    },
    core::Position,
};

#[repr(C)]
pub struct BEDRecordC {
    reference_sequence_name: *const c_char,
    start: usize,
    end: usize,
    name: *const c_char,
    score: i64,
    strand: *const c_char,
    thick_start: usize,
    thick_end: usize,
    color: *const c_char,
    block_count: usize,
    block_sizes: *const c_char,
    block_starts: *const c_char,
}

impl Default for BEDRecordC {
    fn default() -> Self {
        Self {
            reference_sequence_name: std::ptr::null(),
            start: 0,
            end: 0,
            name: std::ptr::null(),
            score: 0,
            strand: std::ptr::null(),
            thick_start: 0,
            thick_end: 0,
            color: std::ptr::null(),
            block_count: 0,
            block_sizes: std::ptr::null(),
            block_starts: std::ptr::null(),
        }
    }
}

pub struct BEDRecordCBuilder {
    reference_sequence_name: *const c_char,
    start: usize,
    end: usize,
    name: *const c_char,
    score: i64,
    strand: *const c_char,
    thick_start: usize,
    thick_end: usize,
    color: *const c_char,
    block_count: usize,
    block_sizes: *const c_char,
    block_starts: *const c_char,
}

impl BEDRecordCBuilder {
    pub fn new() -> BEDRecordCBuilder {
        BEDRecordCBuilder {
            reference_sequence_name: std::ptr::null(),
            start: 0,
            end: 0,
            name: std::ptr::null(),
            score: 0,
            strand: std::ptr::null(),
            thick_start: 0,
            thick_end: 0,
            color: std::ptr::null(),
            block_count: 0,
            block_sizes: std::ptr::null(),
            block_starts: std::ptr::null(),
        }
    }

    pub fn reference_sequence_name(mut self, reference_sequence_name: &str) -> BEDRecordCBuilder {
        self.reference_sequence_name = CString::new(reference_sequence_name).unwrap().into_raw();
        self
    }

    pub fn start(mut self, start: Position) -> BEDRecordCBuilder {
        self.start = start.get();
        self
    }

    pub fn end(mut self, end: Position) -> BEDRecordCBuilder {
        self.end = end.get();
        self
    }

    pub fn name(mut self, name: Option<&Name>) -> BEDRecordCBuilder {
        match name {
            Some(name) => {
                self.name = CString::new(name.to_string()).unwrap().into_raw();
            }
            _ => {
                self.name = std::ptr::null();
            }
        }

        self
    }

    pub fn score(mut self, score: Option<Score>) -> BEDRecordCBuilder {
        match score {
            Some(score) => {
                let uscore = u16::from(score);
                self.score = uscore as i64;
            }
            _ => {
                self.score = -1;
            }
        }

        self
    }

    pub fn strand(mut self, strand: Option<Strand>) -> BEDRecordCBuilder {
        match strand {
            Some(strand) => {
                self.strand = CString::new(strand.to_string()).unwrap().into_raw();
            }
            _ => {
                self.strand = std::ptr::null();
            }
        }

        self
    }

    pub fn thick_start(mut self, thick_start: Position) -> BEDRecordCBuilder {
        self.thick_start = thick_start.get();
        self
    }

    pub fn thick_end(mut self, thick_end: Position) -> BEDRecordCBuilder {
        self.thick_end = thick_end.get();
        self
    }

    pub fn color(mut self, color: Option<String>) -> BEDRecordCBuilder {
        match color {
            Some(color) => {
                self.color = CString::new(color).unwrap().into_raw();
            }
            _ => {
                self.color = std::ptr::null();
            }
        }

        self
    }

    pub fn block_count(mut self, block_count: usize) -> BEDRecordCBuilder {
        self.block_count = block_count;
        self
    }

    pub fn block_sizes(mut self, block_sizes: String) -> BEDRecordCBuilder {
        self.block_sizes = CString::new(block_sizes).unwrap().into_raw();
        self
    }

    pub fn block_starts(mut self, block_starts: String) -> BEDRecordCBuilder {
        self.block_starts = CString::new(block_starts).unwrap().into_raw();
        self
    }

    pub fn build(self) -> BEDRecordC {
        BEDRecordC {
            reference_sequence_name: self.reference_sequence_name,
            start: self.start,
            end: self.end,
            name: self.name,
            score: self.score,
            strand: self.strand,
            thick_start: self.thick_start,
            thick_end: self.thick_end,
            color: self.color,
            block_count: self.block_count,
            block_sizes: self.block_sizes,
            block_starts: self.block_starts,
        }
    }
}

impl From<Record<3>> for BEDRecordC {
    fn from(record: Record<3>) -> Self {
        let builder = BEDRecordCBuilder::new();

        builder
            .reference_sequence_name(record.reference_sequence_name())
            .start(record.start_position())
            .end(record.end_position())
            .build()
    }
}

impl From<Record<4>> for BEDRecordC {
    fn from(record: Record<4>) -> Self {
        let builder = BEDRecordCBuilder::new();

        builder
            .reference_sequence_name(record.reference_sequence_name())
            .start(record.start_position())
            .end(record.end_position())
            .name(record.name())
            .build()
    }
}

impl From<Record<5>> for BEDRecordC {
    fn from(record: Record<5>) -> Self {
        let builder = BEDRecordCBuilder::new();

        builder
            .reference_sequence_name(record.reference_sequence_name())
            .start(record.start_position())
            .end(record.end_position())
            .name(record.name())
            .score(record.score())
            .build()
    }
}

impl From<Record<6>> for BEDRecordC {
    fn from(record: Record<6>) -> Self {
        let builder = BEDRecordCBuilder::new();

        builder
            .reference_sequence_name(record.reference_sequence_name())
            .start(record.start_position())
            .end(record.end_position())
            .name(record.name())
            .score(record.score())
            .strand(record.strand())
            .build()
    }
}

impl From<Record<7>> for BEDRecordC {
    fn from(record: Record<7>) -> Self {
        let builder = BEDRecordCBuilder::new();

        builder
            .reference_sequence_name(record.reference_sequence_name())
            .start(record.start_position())
            .end(record.end_position())
            .name(record.name())
            .score(record.score())
            .strand(record.strand())
            .thick_start(record.thick_start())
            .build()
    }
}

impl From<Record<8>> for BEDRecordC {
    fn from(record: Record<8>) -> Self {
        let builder = BEDRecordCBuilder::new();

        builder
            .reference_sequence_name(record.reference_sequence_name())
            .start(record.start_position())
            .end(record.end_position())
            .name(record.name())
            .score(record.score())
            .strand(record.strand())
            .thick_start(record.thick_start())
            .thick_end(record.thick_end())
            .build()
    }
}

impl From<Record<9>> for BEDRecordC {
    fn from(record: Record<9>) -> Self {
        let builder = BEDRecordCBuilder::new();

        builder
            .reference_sequence_name(record.reference_sequence_name())
            .start(record.start_position())
            .end(record.end_position())
            .name(record.name())
            .score(record.score())
            .strand(record.strand())
            .thick_start(record.thick_start())
            .thick_end(record.thick_end())
            .color(record.color().and_then(|f| Some(f.to_string())))
            .build()
    }
}

impl From<Record<12>> for BEDRecordC {
    fn from(record: Record<12>) -> Self {
        let builder = BEDRecordCBuilder::new();

        let mut block_starts = Vec::new();
        let mut block_sizes = Vec::new();

        record.blocks().iter().for_each(|(start, size)| {
            block_starts.push(start.to_string());
            block_sizes.push(size.to_string());
        });

        let block_start_csv = block_starts.join(",");
        let block_size_csv = block_sizes.join(",");

        builder
            .reference_sequence_name(record.reference_sequence_name())
            .start(record.start_position())
            .end(record.end_position())
            .name(record.name())
            .score(record.score())
            .strand(record.strand())
            .thick_start(record.thick_start())
            .thick_end(record.thick_end())
            .color(record.color().and_then(|f| Some(f.to_string())))
            .block_count(record.blocks().len())
            .block_sizes(block_size_csv)
            .block_starts(block_start_csv)
            .build()
    }
}

#[repr(C)]
pub struct BEDReaderC {
    inner_reader: *mut c_void,
    n_columns: u8, // How many columns are in the BED file. From 3 to 12.
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
pub unsafe extern "C" fn bed_new(
    filename: *const c_char,
    n_columns: u8,
    compression: *const c_char,
) -> BEDReaderC {
    let filename = CStr::from_ptr(filename).to_str().unwrap();
    let compression = CStr::from_ptr(compression).to_str().unwrap();

    let mut reader = build_from_path(filename, compression).unwrap();

    return BEDReaderC {
        inner_reader: Box::into_raw(Box::new(reader)) as *mut c_void,
        n_columns,
    };
}

#[no_mangle]
pub unsafe extern "C" fn bed_next(bam_reader: &BEDReaderC, n_columns: u8) -> BEDRecordC {
    let bam_reader_ptr = bam_reader.inner_reader as *mut Reader<Box<dyn BufRead>>;

    match bam_reader_ptr.as_mut() {
        None => {
            eprintln!("bam_next: bam_reader_ptr is null");
            return BEDRecordC::default();
        }
        Some(reader) => {
            let mut buffer = String::new();

            let line_read_result = reader.read_line(&mut buffer);
            if line_read_result.is_err() {
                return BEDRecordC::default();
            }

            let bytes_read = line_read_result.unwrap();
            if bytes_read == 0 {
                return BEDRecordC::default();
            }

            match n_columns {
                3 => {
                    let line: Record<3> = Record::from_str(&buffer).unwrap();

                    let c_record = BEDRecordC::from(line);
                    return c_record;
                }
                4 => {
                    let line: Record<4> = Record::from_str(&buffer).unwrap();

                    let c_record = BEDRecordC::from(line);
                    return c_record;
                }
                5 => {
                    let line: Record<5> = Record::from_str(&buffer).unwrap();

                    let c_record = BEDRecordC::from(line);
                    return c_record;
                }
                6 => {
                    let line: Record<6> = Record::from_str(&buffer).unwrap();

                    let c_record = BEDRecordC::from(line);
                    return c_record;
                }
                7 => {
                    let line: Record<7> = Record::from_str(&buffer).unwrap();

                    let c_record = BEDRecordC::from(line);
                    return c_record;
                }
                8 => {
                    let line: Record<8> = Record::from_str(&buffer).unwrap();

                    let c_record = BEDRecordC::from(line);
                    return c_record;
                }
                9 => {
                    let line: Record<9> = Record::from_str(&buffer).unwrap();

                    let c_record = BEDRecordC::from(line);
                    return c_record;
                }
                12 => {
                    let line: Record<12> = Record::from_str(&buffer).unwrap();

                    let c_record = BEDRecordC::from(line);
                    return c_record;
                }
                _ => {
                    eprintln!("bed_next: n_columns must be between 3 and 9, or 12");
                    return BEDRecordC::default();
                }
            }
        }
    }
}
