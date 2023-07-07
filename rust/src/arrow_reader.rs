// Copyright 2023 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    ffi::{c_char, CStr, CString},
    ptr::null,
    str::FromStr,
    sync::Arc,
};

use arrow::ffi_stream::FFI_ArrowArrayStream as ArrowArrayStream;
use datafusion::{
    datasource::file_format::file_type::FileCompressionType,
    prelude::{SessionConfig, SessionContext},
};
use exon::{
    context::ExonSessionExt,
    datasources::{ExonFileType, ExonReadOptions},
    ffi::create_dataset_stream_from_table_provider,
    runtime_env::ExonRuntimeEnvExt,
};
use tokio::runtime::Runtime;

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
    filters: *const c_char,
) -> ReaderResult {
    let uri = match CStr::from_ptr(uri).to_str() {
        Ok(uri) => uri,
        Err(e) => {
            let error = CString::new(format!("could not parse uri: {}", e)).unwrap();
            return ReaderResult {
                error: error.into_raw(),
            };
        }
    };

    let rt = Arc::new(Runtime::new().unwrap());

    // if compression is null, try to infer from file extension
    let compression_type = if compression.is_null() {
        let extension = match uri.split('.').last() {
            Some(extension) => extension,
            None => {
                let error = CString::new("could not parse extension").unwrap();
                return ReaderResult {
                    error: error.into_raw(),
                };
            }
        };

        match extension {
            "gz" => FileCompressionType::GZIP,
            "zst" => FileCompressionType::ZSTD,
            _ => FileCompressionType::UNCOMPRESSED,
        }
    } else {
        let compression = match CStr::from_ptr(compression).to_str() {
            Ok(compression) => compression,
            Err(e) => {
                let error = CString::new(format!("could not parse compression: {}", e)).unwrap();
                return ReaderResult {
                    error: error.into_raw(),
                };
            }
        };

        let compression =
            FileCompressionType::from_str(compression).unwrap_or(FileCompressionType::UNCOMPRESSED);

        compression
    };

    let file_type = CStr::from_ptr(file_format).to_str().unwrap();
    let file_type = match ExonFileType::from_str(file_type) {
        Ok(file_type) => file_type,
        Err(_) => {
            let error = CString::new(format!("could not parse file_format {}", file_type)).unwrap();
            return ReaderResult {
                error: error.into_raw(),
            };
        }
    };

    let config = SessionConfig::new().with_batch_size(batch_size);
    let ctx = SessionContext::with_config_exon(config);

    rt.block_on(async {
        if let Err(e) = ctx.runtime_env().exon_register_object_store_uri(uri).await {
            return ReaderResult {
                error: CString::new(format!("could not register object store: {}", e))
                    .unwrap()
                    .into_raw(),
            };
        }

        let options = ExonReadOptions::new(file_type).with_compression(compression_type);

        if let Err(e) = ctx.register_exon_table("exon_table", uri, options).await {
            let error = CString::new(format!("could not register table: {}", e)).unwrap();
            return ReaderResult {
                error: error.into_raw(),
            };
        }

        let mut select_string = format!("SELECT * FROM exon_table");

        if !filters.is_null() {
            let filters_str = match CStr::from_ptr(filters).to_str() {
                Ok(filters_str) => filters_str,
                Err(e) => {
                    let error = CString::new(format!("could not parse filters: {}", e)).unwrap();
                    return ReaderResult {
                        error: error.into_raw(),
                    };
                }
            };

            if filters_str != "" {
                select_string.push_str(format!(" WHERE {}", filters_str).as_str());
            }
        }

        let df = match ctx.sql(&select_string).await {
            Ok(df) => df,
            Err(e) => {
                let error = CString::new(format!("could not execute sql: {}", e)).unwrap();
                return ReaderResult {
                    error: error.into_raw(),
                };
            }
        };

        match create_dataset_stream_from_table_provider(df, rt.clone(), stream_ptr).await {
            Ok(_) => ReaderResult {
                error: std::ptr::null(),
            },
            Err(e) => {
                let error =
                    CString::new(format!("could not create dataset stream: {}", e)).unwrap();
                return ReaderResult {
                    error: error.into_raw(),
                };
            }
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

    match ExonFileType::from_str(splitted) {
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
