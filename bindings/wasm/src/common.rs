use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use js_sys::{Object, Reflect, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};

pub(crate) fn js_err(e: impl ToString) -> JsValue {
    JsValue::from_str(&e.to_string())
}

/// Convert a JS object { "rel/path": Uint8Array, ... } to MemPack files.
pub(crate) fn js_files_to_mempack(files: JsValue) -> Result<openmander_core::MemPack> {
    let obj: Object = files.dyn_into().map_err(|_| anyhow!("files must be an object"))?;
    let keys = Object::keys(&obj);

    let mut map: HashMap<String, Arc<[u8]>> = HashMap::with_capacity(keys.length() as usize);

    for i in 0..keys.length() {
        let k = keys.get(i).as_string().ok_or_else(|| anyhow!("non-string key"))?;
        let v = Reflect::get(&obj, &JsValue::from_str(&k))
            .map_err(|e| anyhow!("error getting property '{}': {:?}", k, e))?;

        // Accept Uint8Array-like values
        let u8arr = Uint8Array::new(&v);
        let mut buf = vec![0u8; u8arr.length() as usize];
        u8arr.copy_to(&mut buf[..]);

        map.insert(k, Arc::from(buf));
    }

    Ok(openmander_core::MemPack::new(map))
}

pub(crate) fn parse_layer(layer: Option<String>) -> Result<openmander_core::GeoType> {
    let layer = layer.as_deref().unwrap_or("block");
    openmander_core::GeoType::from_str(layer).ok_or_else(|| {
        anyhow!(
            "Unknown layer {:?}. Expected one of: state, county, tract, group, vtd, block",
            layer
        )
    })
}
