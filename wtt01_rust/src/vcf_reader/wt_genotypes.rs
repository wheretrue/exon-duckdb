use std::ffi::c_void;

use noodles::vcf::record::Genotype;

#[repr(C)]
pub struct WTGenotypes {
    pub genotypes: *mut c_void,
    pub number_of_genotypes: i32,
    pub genotype_position: i32,
}

impl Default for WTGenotypes {
    fn default() -> Self {
        Self {
            genotypes: std::ptr::null_mut(),
            number_of_genotypes: 0,
            genotype_position: 0,
        }
    }
}

impl WTGenotypes {
    pub fn new(genotypes: Vec<Genotype>) -> Self {
        Self {
            genotypes: Box::into_raw(Box::new(genotypes.clone())) as *mut c_void,
            number_of_genotypes: genotypes.len() as i32,
            genotype_position: 0,
        }
    }
}

#[repr(C)]
pub struct WTGenotype {
    pub genotype: *mut c_void,
}

impl WTGenotype {
    pub fn new(genotype: Genotype) -> Self {
        Self {
            genotype: Box::into_raw(Box::new(genotype.clone())) as *mut c_void,
        }
    }
}

impl Default for WTGenotype {
    fn default() -> Self {
        Self {
            genotype: std::ptr::null_mut(),
        }
    }
}