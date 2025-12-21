use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};

use crate::map::GeoType;

#[derive(Serialize, Deserialize)]
pub(crate) struct FileHash {
    pub sha256: String,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Manifest {
    pack_id: String,
    version: String,
    crs: String,
    levels: Vec<String>,
    counts: BTreeMap<String, usize>,
    files: BTreeMap<String, FileHash>,
}

impl Manifest {
    pub(crate) fn new(
        path: &Path,
        counts: BTreeMap<&'static str, usize>,
        files: BTreeMap<String, FileHash>,
    ) -> Self {
        Self {
            pack_id: path.file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown-pack")
                .to_string(),
            version: "1".into(),
            crs: "EPSG:4269".into(),
            levels: GeoType::ALL.iter().map(|ty| ty.to_str().into()).collect(),
            counts: counts.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            files: files,
        }
    }
}
