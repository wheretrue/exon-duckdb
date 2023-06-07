use std::{
    ffi::{c_char, CStr, CString},
    ptr::null,
    str::FromStr,
    sync::Arc,
};

use arrow::ffi_stream::FFI_ArrowArrayStream as ArrowArrayStream;
use datafusion::{
    datasource::file_format::file_type::FileCompressionType, prelude::SessionContext,
};
use object_store::aws::AmazonS3Builder;
use tca::{
    context::TCASessionExt, datasources::TCAFileType,
    ffi::create_dataset_stream_from_table_provider,
};
use tokio::runtime::Runtime;
use url::Url;

#[repr(C)]
pub struct ReaderResult {
    error: *const c_char,
}

#[no_mangle]
pub unsafe extern "C" fn new_reader(
    stream_ptr: *mut ArrowArrayStream,
    uri: *const c_char,
    batch_size: usize,
    compression: *const c_char,
    file_format: *const c_char,
) -> ReaderResult {
    let uri = CStr::from_ptr(uri).to_str().unwrap();
    let rt = Arc::new(Runtime::new().unwrap());

    // if compression is null, try to infer from file extension
    let compression_type = if compression.is_null() {
        let extension = uri.split('.').last().unwrap();
        match extension {
            "gz" => FileCompressionType::GZIP,
            "zst" => FileCompressionType::ZSTD,
            _ => FileCompressionType::UNCOMPRESSED,
        }
    } else {
        let compression = CStr::from_ptr(compression).to_str().unwrap();
        let compression =
            FileCompressionType::from_str(compression).unwrap_or(FileCompressionType::UNCOMPRESSED);

        compression
    };

    let file_type = CStr::from_ptr(file_format).to_str().unwrap();
    let file_type = match TCAFileType::from_str(file_type) {
        Ok(file_type) => file_type,
        Err(_) => {
            let error = CString::new(format!("could not parse file_format {}", file_type)).unwrap();
            return ReaderResult {
                error: error.into_raw(),
            };
        }
    };

    let ctx = SessionContext::new();

    // handle s3
    if uri.starts_with("s3://") {
        let url_from_uri = match Url::parse(uri) {
            Ok(url) => url,
            Err(e) => {
                let error = CString::new(format!("could not parse uri: {}", e)).unwrap();
                return ReaderResult {
                    error: error.into_raw(),
                };
            }
        };

        let host_str = match url_from_uri.host_str() {
            Some(host_str) => host_str,
            None => {
                let error = CString::new("could not parse host_str").unwrap();
                return ReaderResult {
                    error: error.into_raw(),
                };
            }
        };

        let s3 = match AmazonS3Builder::from_env()
            .with_bucket_name(host_str)
            .build()
        {
            Ok(s3) => s3,
            Err(e) => {
                let error = CString::new(format!("could not create s3 client: {}", e)).unwrap();
                return ReaderResult {
                    error: error.into_raw(),
                };
            }
        };

        let path = format!("s3://{}", host_str);
        let s3_url = Url::parse(&path).unwrap();
        ctx.runtime_env()
            .register_object_store(&s3_url, Arc::new(s3));
    }

    rt.block_on(async {
        let df = match ctx
            .read_tca_table(uri, file_type, Some(compression_type))
            .await
        {
            Ok(df) => df,
            Err(e) => {
                let error = CString::new(format!("could not read table: {}", e)).unwrap();
                return ReaderResult {
                    error: error.into_raw(),
                };
            }
        };

        create_dataset_stream_from_table_provider(df, rt.clone(), stream_ptr).await;
        ReaderResult {
            error: std::ptr::null(),
        }
    })
}

#[repr(C)]
pub struct ReplacementScanResult {
    file_type: *const c_char,
}

#[no_mangle]
pub unsafe extern "C" fn replacement_scan(uri: *const c_char) -> ReplacementScanResult {
    let uri = CStr::from_ptr(uri).to_str().unwrap();
    let mut exts = uri.rsplit('.');
    let mut splitted = exts.next().unwrap_or("");

    let file_compression_type =
        FileCompressionType::from_str(splitted).unwrap_or(FileCompressionType::UNCOMPRESSED);

    if file_compression_type.is_compressed() {
        splitted = exts.next().unwrap_or("");
    }

    match TCAFileType::from_str(splitted) {
        Ok(file_type) => {
            let ft_string = file_type.to_string();
            return ReplacementScanResult {
                file_type: CString::new(ft_string).unwrap().into_raw(),
            };
        }
        Err(_) => {
            return ReplacementScanResult { file_type: null() };
        }
    }
}
