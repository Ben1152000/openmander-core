use std::{collections::BTreeMap, fs::File, path::Path};

use anyhow::{anyhow, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{common::{data::*, fs::*}, types::*};


#[derive(Serialize, Deserialize)]
struct Manifest {
    pack_id: String,
    version: String,
    crs: String,
    levels: Vec<String>,
    counts: BTreeMap<String, usize>,
    files: BTreeMap<String, FileHash>,
}

#[derive(Serialize, Deserialize)]
struct FileHash {
    sha256: String,
}

fn entities_to_df(layer: &MapLayer) -> Result<DataFrame> {
    let geotype: Vec<&str> = (0..layer.entities.len()).map(|_| layer.ty.to_str()).collect();
    let geoid: Vec<String> = layer.entities.iter()
        .map(|e| e.geo_id.id.to_string())
        .collect();
    let name: Vec<Option<String>> = layer.entities.iter()
        .map(|e| e.name.as_ref().map(|s| s.to_string()))
        .collect();
    let area_m2: Vec<Option<f64>> = layer.entities.iter().map(|e| e.area_m2).collect();
    let lon: Vec<Option<f64>> = layer.entities.iter()
        .map(|e| e.centroid.as_ref().map(|p| p.x()))
        .collect();
    let lat: Vec<Option<f64>> = layer.entities.iter()
        .map(|e| e.centroid.as_ref().map(|p| p.y()))
        .collect();

    let parent = |pick: fn(&ParentRefs) -> Result<&Option<GeoId>>| -> Vec<Option<String>> {
        layer.parents.iter()
            .map(|p| pick(p).ok().and_then(|g| g.as_ref().map(|x| x.id.to_string())))
            .collect()
    };

    let parent_state = parent(|p| p.get(GeoType::State));
    let parent_county = parent(|p| p.get(GeoType::County));
    let parent_tract = parent(|p| p.get(GeoType::Tract));
    let parent_group = parent(|p| p.get(GeoType::Group));
    let parent_vtd = parent(|p| p.get(GeoType::VTD));

    let df = df![
        "geotype" => geotype,
        "geoid" => geoid,
        "name" => name,
        "area_m2" => area_m2,
        "lon" => lon,
        "lat" => lat,
        "parent_state" => parent_state,
        "parent_county" => parent_county,
        "parent_tract" => parent_tract,
        "parent_group" => parent_group,
        "parent_vtd" => parent_vtd,
    ]?;
    Ok(df)
}

fn df_to_entities(df: &DataFrame, ty: GeoType) -> Result<(Vec<Entity>, Vec<ParentRefs>)> {
    let geoid = df.column("geoid")?.str()?;
    let name = df.column("name").ok().and_then(|c| c.str().ok());
    let area_m2 = df.column("area_m2").ok().and_then(|c| c.f64().ok());
    let lon = df.column("lon").ok().and_then(|c| c.f64().ok());
    let lat = df.column("lat").ok().and_then(|c| c.f64().ok());

    let ps = df.column("parent_state").ok().and_then(|c| c.str().ok());
    let pc = df.column("parent_county").ok().and_then(|c| c.str().ok());
    let pt = df.column("parent_tract").ok().and_then(|c| c.str().ok());
    let pg = df.column("parent_group").ok().and_then(|c| c.str().ok());
    let pv = df.column("parent_vtd").ok().and_then(|c| c.str().ok());

    let len = geoid.len();

    let mut entities = Vec::with_capacity(len);
    let mut parents = Vec::with_capacity(len);

    for i in 0..len {
        let id_txt = geoid.get(i).ok_or_else(|| anyhow!("missing geoid"))?;
        let nm = name.as_ref().and_then(|c| c.get(i)).map(|s| s.to_string());
        let a = area_m2.as_ref().and_then(|c| c.get(i));
        let lo = lon.as_ref().and_then(|c| c.get(i));
        let la = lat.as_ref().and_then(|c| c.get(i));

        let centroid = match (lo, la) {
            (Some(x), Some(y)) => Some(geo::Point::new(x, y)),
            _ => None,
        };

        entities.push(Entity {
            geo_id: GeoId {
                ty,
                id: id_txt.into(),
            },
            name: nm.map(Into::into),
            area_m2: a,
            centroid,
        });

        let mut p = ParentRefs::default();
        let mk = |txt: Option<&str>, pty: GeoType| -> Option<GeoId> {
            txt.map(|s| GeoId { ty: pty, id: s.into() })
        };

        p.set(GeoType::State, ps.as_ref().and_then(|c| c.get(i)).and_then(|s| mk(Some(s), GeoType::State))).ok();
        p.set(GeoType::County, pc.as_ref().and_then(|c| c.get(i)).and_then(|s| mk(Some(s), GeoType::County))).ok();
        p.set(GeoType::Tract,  pt.as_ref().and_then(|c| c.get(i)).and_then(|s| mk(Some(s), GeoType::Tract))).ok();
        p.set(GeoType::Group,  pg.as_ref().and_then(|c| c.get(i)).and_then(|s| mk(Some(s), GeoType::Group))).ok();
        p.set(GeoType::VTD,    pv.as_ref().and_then(|c| c.get(i)).and_then(|s| mk(Some(s), GeoType::VTD))).ok();

        parents.push(p);
    }

    Ok((entities, parents))
}

pub fn write_pack(path: &Path, map_data: &MapData) -> Result<()> {
    // Pack format:
    //   NE_2020_pack/
    //     download/ (temp dir)
    //     entities/
    //       <layer>.parquet
    //     elections/
    //       <layer>.parquet
    //     demographics/
    //       <layer>.parquet
    //     geometries/
    //       <layer>.fgb
    //     relations/
    //       county.csr.bin
    //       tract.csr.bin
    //       group.csr.bin
    //       vtd.csr.bin
    //       block.csr.bin
    //       block_to_vtd.parquet
    //     meta/
    //       manifest.json
    ensure_dirs(path, &["entities", "elections", "demographics", "relations", "geometries", "meta"])?;

    let mut files_written: BTreeMap<String, FileHash> = BTreeMap::new();
    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

    // Entities / Elections / Demographics / Geometries
    for ty in GeoType::order() {
        let layer = map_data.get_layer(ty);
        let name = ty.to_str();

        // entities
        let ent_df = entities_to_df(layer)?;
        let ent_rel = format!("entities/{}.parquet", name);
        write_to_parquet(&path.join(&ent_rel), &ent_df)?;
        let (k, h) = sha256_file(&ent_rel, path)?;
        files_written.insert(k, FileHash { sha256: h });

        counts.insert(name, layer.entities.len());

        // elections
        if let Some(df) = &layer.elec_data {
            let rel = format!("elections/{}.parquet", name);
            write_to_parquet(&path.join(&rel), df)?;
            let (k, h) = sha256_file(&rel, path)?;
            files_written.insert(k, FileHash { sha256: h });
        }

        // demographics
        if let Some(df) = &layer.demo_data {
            let rel = format!("demographics/{}.parquet", name);
            write_to_parquet(&path.join(&rel), df)?;
            let (k, h) = sha256_file(&rel, path)?;
            files_written.insert(k, FileHash { sha256: h });
        }

/*
        // geometry
        if let Some(geom) = &layer.geoms {
            let rel = match ty {
                GeoType::State => "geometries/state.fgb",
                GeoType::County => "geometries/counties.fgb",
                GeoType::Tract => "geometries/tracts.fgb",
                GeoType::Group => "geometries/groups.fgb",
                GeoType::VTD => "geometries/vtds.fgb",
                GeoType::Block => "geometries/blocks.fgb",
            };
            // adjust to your IO function
            geom.write_fgb(&path.join(rel))?;
            let (k, h) = sha256_file(rel, path)?;
            files_written.insert(k, FileHash { sha256: h });
        }
*/

        // relations (CSR) — uncomment when you have adjacency IO
        // if let Some(adj_path_rel) = maybe_write_layer_csr(layer, name, path)? {
        //     let (k, h) = sha256_file(&adj_path_rel, path)?;
        //     files_written.insert(adj_path_rel, FileHash { sha256: h });
        // }
    }

    // Example: block_to_vtd.parquet if you maintain that mapping as a DF somewhere
    // if let Some(df) = &map_data.blocks_to_vtd_df { ... }

    // Manifest
    let manifest = Manifest {
        pack_id: path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown-pack")
            .to_string(),
        version: "1".into(),
        crs: "EPSG:4269".into(),
        levels: GeoType::order().iter().map(|ty| (*ty.to_str()).to_string()).collect(),
        counts: counts.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        files: files_written,
    };

    let meta_path = path.join("meta/manifest.json");
    let mut f = File::create(&meta_path)?;
    serde_json::to_writer_pretty(&mut f, &manifest)?;
    Ok(())
}

pub fn read_pack(path: &Path) -> Result<MapData> {
    let mut md = MapData::default();

    // Load per-level content
    for ty in GeoType::order() {
        let layer = md.get_layer_mut(ty);
        let name = ty.to_str();

        // entities
        let ent_p = path.join(format!("entities/{}.parquet", name));
        if ent_p.exists() {
            let df = read_from_parquet(&ent_p)?;
            let (entities, parents) = df_to_entities(&df, ty)?;
            layer.entities = entities;
            layer.parents = parents;

            // rebuild index
            layer.index.clear();
            for (i, e) in layer.entities.iter().enumerate() {
                layer.index.insert(e.geo_id.clone(), i as u32);
            }
        }

        // elections
        let el_p = path.join(format!("elections/{}.parquet", name));
        if el_p.exists() {
            layer.elec_data = Some(read_from_parquet(&el_p)?);
        }

        // demographics
        let dm_p = path.join(format!("demographics/{}.parquet", name));
        if dm_p.exists() {
            layer.demo_data = Some(read_from_parquet(&dm_p)?);
        }

/*
        // geometry
        let geom_rel = match ty {
            GeoType::State => "geometries/state.fgb",
            GeoType::County => "geometries/counties.fgb",
            GeoType::Tract => "geometries/tracts.fgb",
            GeoType::Group => "geometries/groups.fgb",
            GeoType::VTD => "geometries/vtds.fgb",
            GeoType::Block => "geometries/blocks.fgb",
        };
        let gp = path.join(geom_rel);
        if gp.exists() {
            // adjust to your IO function
            layer.geoms = Some(crate::geometry::PlanarPartition::read_fgb(&gp)?);
        }
*/

        // relations (CSR) — add when available
        // read_layer_csr_into(layer, path)?;
    }

    Ok(md)
}
