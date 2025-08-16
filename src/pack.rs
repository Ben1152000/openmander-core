use std::{collections::BTreeMap, fs::File, io::{BufReader, BufWriter}, path::Path};

use anyhow::{anyhow, Ok, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use flatgeobuf::{FallibleStreamingIterator, FgbCrs, FgbReader, FgbWriter, FgbWriterOptions, GeometryType};
use geozero::{FeatureProcessor, geo_types::process_geom, ToGeo};
use geo::{Geometry, MultiPolygon};

use crate::{common::{data::*, fs::*}, geometry::PlanarPartition, types::*};

#[derive(Serialize, Deserialize)]
struct Manifest {
    pack_id: String,
    version: String,
    crs: String,
    levels: Vec<String>,
    counts: BTreeMap<String, usize>,
    files: BTreeMap<String, FileHash>,
}

impl Manifest {
    pub fn new() -> Self {
        todo!()
    }
}

#[derive(Serialize, Deserialize)]
struct FileHash {
    sha256: String,
}

impl PlanarPartition {
    /// Write as a FlatGeobuf (MultiPolygon layer, EPSG:4269).
    fn write_fgb(&self, path: &Path) -> Result<()> {
        let mut fgb = FgbWriter::create_with_options(
            "geoms",
            GeometryType::MultiPolygon,
            FgbWriterOptions {
                crs: FgbCrs { code: 4269, ..Default::default() },
                ..Default::default()
            },
        )?;

        for (i, mp) in self.geoms.iter().enumerate() {
            fgb.feature_begin(i as u64)?;
            let g: Geometry<f64> = Geometry::MultiPolygon(mp.clone());
            process_geom(&g, &mut fgb)?; // stream geo-types coords into writer
            fgb.feature_end(i as u64)?;
        }

        let mut out = BufWriter::new(std::fs::File::create(path)?);
        fgb.write(&mut out)?;
        Ok(())
    }

    /// Read a FlatGeobuf (MultiPolygon/Polygon) into a PlanarPartition.
    fn read_fgb(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut feat_iter = FgbReader::open(BufReader::new(file))?.select_all()?;

        let mut polys: Vec<MultiPolygon<f64>> = Vec::new();
        while let Some(feat) = feat_iter.next()? {
            let geom: geo::Geometry<f64> = feat.to_geo()?; // convert feature to geo-types
            match geom {
                geo::Geometry::MultiPolygon(mp) => polys.push(mp),
                geo::Geometry::Polygon(p) => polys.push(MultiPolygon(vec![p])), // be lenient
                other => return Err(anyhow!("Unexpected geometry type in FGB: {:?}", other)),
            }
        }

        Ok(PlanarPartition::new(polys))
    }


}

impl MapLayer {
    fn write_pack(&self, path: &Path,
        counts: &mut BTreeMap<&'static str, usize>,
        hashes: &mut BTreeMap<String, FileHash>
    ) -> Result<()> {
        let name: &'static str = self.ty.to_str();

        #[inline]
        fn write_to_parquet_with_hash(root: &Path, rel_path: &str, df: &DataFrame, 
            hashes: &mut BTreeMap<String, FileHash>
        ) -> Result<()> {
            write_to_parquet(&root.join(rel_path), df)?;
            let (k, h) = sha256_file(rel_path, root)?;
            hashes.insert(k, FileHash { sha256: h });
            Ok(())
        }

        // entities
        write_to_parquet_with_hash(
            path,
            &format!("entities/{}.parquet", name),
            &entities_to_df(self)?,
            hashes
        )?;

        counts.insert(name.into(), self.entities.len());

        // elections
        if let Some(df) = &self.elec_data {
            write_to_parquet_with_hash(path, &format!("elections/{name}.parquet"), df, hashes)?;
        }

        // demographics
        if let Some(df) = &self.demo_data {
            write_to_parquet_with_hash(path, &format!("demographics/{name}.parquet"), df, hashes)?;
        }

        // geometries
        if let Some(geom) = &self.geoms {
            let rel = &format!("geometries/{name}.fgb");
            geom.write_fgb(&path.join(rel))?;
            let (k, h) = sha256_file(rel, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        // relations (CSR) — uncomment when you have adjacency IO
        // if let Some(adj_path_rel) = maybe_write_layer_csr(self, name, path)? {
        //     let (k, h) = sha256_file(&adj_path_rel, path)?;
        //     file_hashes.insert(adj_path_rel, FileHash { sha256: h });
        // }
        Ok(())
    }

    fn read_pack(path: &Path) -> Result<Self> { todo!() }
}

impl MapData {
    fn write_pack(&self, path: &Path) -> Result<()> { todo!() }
    fn read_pack(path: &Path) -> Result<Self> { todo!() }
}

fn entities_to_df(layer: &MapLayer) -> Result<DataFrame> {
    let parent = |pick: fn(&ParentRefs) -> Result<&Option<GeoId>>| -> Vec<Option<String>> {
        layer.parents.iter()
            .map(|p| pick(p).ok().and_then(|g| g.as_ref().map(|x| x.id.to_string())))
            .collect()
    };

    Ok(df![
        "geotype" => (0..layer.entities.len()).map(|_| layer.ty.to_str()).collect::<Vec<_>>(),
        "geoid" => layer.entities.iter().map(|e| e.geo_id.id.to_string()).collect::<Vec<_>>(),
        "name" => layer.entities.iter().map(|e| e.name.as_ref().map(|s| s.to_string())).collect::<Vec<_>>(),
        "area_m2" => layer.entities.iter().map(|e| e.area_m2).collect::<Vec<_>>(),
        "lon" => layer.entities.iter().map(|e| e.centroid.as_ref().map(|p| p.x())).collect::<Vec<_>>(),
        "lat" => layer.entities.iter().map(|e| e.centroid.as_ref().map(|p| p.y())).collect::<Vec<_>>(),
        "parent_state" => parent(|p| p.get(GeoType::State)),
        "parent_county" => parent(|p| p.get(GeoType::County)),
        "parent_tract" => parent(|p| p.get(GeoType::Tract)),
        "parent_group" => parent(|p| p.get(GeoType::Group)),
        "parent_vtd" => parent(|p| p.get(GeoType::VTD)),
    ]?)
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

// Pack format:
//   NE_2020_pack/
//     download/ (temp dir)
//     entities/<layers>.parquet
//     elections/<layers>.parquet
//     demographics/<layers>.parquet
//     geometries/<layers>.fgb
//     relations/
//       county.csr.bin
//       tract.csr.bin
//       group.csr.bin
//       vtd.csr.bin
//       block.csr.bin
//       block_to_vtd.parquet
//     meta/
//       manifest.json

pub fn write_pack(path: &Path, map_data: &MapData) -> Result<()> {
    let dirs = ["entities", "elections", "demographics", "relations", "geometries", "meta"];
    ensure_dirs(path, &dirs)?;

    let mut file_hashes: BTreeMap<String, FileHash> = BTreeMap::new();
    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

    for ty in GeoType::order() {
        map_data.get_layer(ty).write_pack(path, &mut counts, &mut file_hashes)?;
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
        levels: GeoType::order().iter().map(|ty| ty.to_str().into()).collect(),
        counts: counts.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        files: file_hashes,
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
        let el_p = path.join(format!("elections/{name}.parquet"));
        if el_p.exists() {
            layer.elec_data = Some(read_from_parquet(&el_p)?);
        }

        // demographics
        let dm_p = path.join(format!("demographics/{name}.parquet"));
        if dm_p.exists() {
            layer.demo_data = Some(read_from_parquet(&dm_p)?);
        }

        // geometries
        let geom_rel = &format!("geometries/{name}.fgb");
        let gp = path.join(geom_rel);
        if gp.exists() {
            layer.geoms = Some(crate::geometry::PlanarPartition::read_fgb(&gp)?);
        }

        // relations (CSR) — add when available
        // read_layer_csr_into(layer, path)?;
    }

    Ok(md)
}
