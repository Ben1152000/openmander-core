use std::{collections::BTreeMap, fs::File, path::Path};

use anyhow::{anyhow, Ok, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{common::{data::*, fs::*, polygon::*}, geometry::PlanarPartition, map::*};

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
    pub fn new(
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
            levels: GeoType::order().iter().map(|ty| ty.to_str().into()).collect(),
            counts: counts.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            files: files,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct FileHash {
    sha256: String,
}

impl MapLayer {
    fn entities_to_df(&self) -> Result<DataFrame> {
        let parent = |pick: fn(&ParentRefs) -> Result<&Option<GeoId>>| -> Vec<Option<String>> {
            self.parents.iter()
                .map(|p| pick(p).ok().and_then(|g| g.as_ref().map(|x| x.id.to_string())))
                .collect()
        };
    
        Ok(df![
            "geotype" => (0..self.entities.len()).map(|_| self.ty.to_str()).collect::<Vec<_>>(),
            "geoid" => self.entities.iter().map(|e| e.geo_id.id.to_string()).collect::<Vec<_>>(),
            "name" => self.entities.iter().map(|e| e.name.as_ref().map(|s| s.to_string())).collect::<Vec<_>>(),
            "area_m2" => self.entities.iter().map(|e| e.area_m2).collect::<Vec<_>>(),
            "lon" => self.entities.iter().map(|e| e.centroid.as_ref().map(|p| p.x())).collect::<Vec<_>>(),
            "lat" => self.entities.iter().map(|e| e.centroid.as_ref().map(|p| p.y())).collect::<Vec<_>>(),
            "parent_state" => parent(|p| p.get(GeoType::State)),
            "parent_county" => parent(|p| p.get(GeoType::County)),
            "parent_tract" => parent(|p| p.get(GeoType::Tract)),
            "parent_group" => parent(|p| p.get(GeoType::Group)),
            "parent_vtd" => parent(|p| p.get(GeoType::VTD)),
        ]?)
    }

    fn df_to_entities(&mut self, df: &DataFrame, ty: GeoType) -> Result<()> {
        let geoid = df.column("geoid")?.str()?;
        let name = df.column("name")?.str()?;
        let area_m2 = df.column("area_m2")?.f64()?;
        let lon = df.column("lon")?.f64()?;
        let lat = df.column("lat")?.f64()?;
        let ps = df.column("parent_state")?.str()?;
        let pc = df.column("parent_county")?.str()?;
        let pt = df.column("parent_tract")?.str()?;
        let pg = df.column("parent_group")?.str()?;
        let pv = df.column("parent_vtd")?.str()?;

        let len = geoid.len();

        let mut entities = Vec::with_capacity(len);
        let mut parents = Vec::with_capacity(len);

        for i in 0..len {
            entities.push(Entity {
                geo_id: GeoId {
                    ty,
                    id: geoid.get(i).ok_or_else(|| anyhow!("missing geoid"))?.into(),
                },
                name: name.get(i).map(Into::into),
                area_m2: area_m2.get(i),
                centroid: match (lon.get(i), lat.get(i)) {
                    (Some(x), Some(y)) => Some(geo::Point::new(x, y)),
                    _ => None,
                },
            });

            let mut p = ParentRefs::default();
            let mk = |txt: Option<&str>, pty: GeoType| -> Option<GeoId> {
                txt.map(|s| GeoId { ty: pty, id: s.into() })
            };

            p.set(GeoType::State, ps.get(i).and_then(|s| mk(Some(s), GeoType::State))).ok();
            p.set(GeoType::County, pc.get(i).and_then(|s| mk(Some(s), GeoType::County))).ok();
            p.set(GeoType::Tract,  pt.get(i).and_then(|s| mk(Some(s), GeoType::Tract))).ok();
            p.set(GeoType::Group,  pg.get(i).and_then(|s| mk(Some(s), GeoType::Group))).ok();
            p.set(GeoType::VTD,    pv.get(i).and_then(|s| mk(Some(s), GeoType::VTD))).ok();

            parents.push(p);
        }

        self.entities = entities;
        self.parents = parents;

        Ok(())
    }

    fn write_to_pack(&self, path: &Path,
        counts: &mut BTreeMap<&'static str, usize>,
        hashes: &mut BTreeMap<String, FileHash>
    ) -> Result<()> {
        let name: &'static str = self.ty.to_str();
        let entity_path = &format!("entities/{name}.parquet");
        let elec_path = &format!("elections/{name}.parquet");
        let demo_path = &format!("demographics/{name}.parquet");
        let geom_path = &format!("geometries/{name}.geoparquet");
        let adj_path = &format!("geometries/adjacencies/{name}.csr.bin");

        counts.insert(name.into(), self.entities.len());

        // entities
        write_to_parquet(&path.join(entity_path), &self.entities_to_df()?)?;
        let (k, h) = sha256_file(entity_path, path)?;
        hashes.insert(k, FileHash { sha256: h });

        // elections
        if let Some(df) = &self.elec_data {
            write_to_parquet(&path.join(elec_path), df)?;
            let (k, h) = sha256_file(elec_path, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        // demographics
        if let Some(df) = &self.demo_data {
            write_to_parquet(&path.join(demo_path), df)?;
            let (k, h) = sha256_file(demo_path, path)?;
            hashes.insert(k, FileHash { sha256: h });
        }

        // geometries
        if let Some(geom) = &self.geoms {
            write_to_geoparquet(&path.join(geom_path), &geom.geoms)?;
            let (k, h) = sha256_file(geom_path, path)?;
            hashes.insert(k, FileHash { sha256: h });

            // adjacencies (CSR)
            if self.ty != GeoType::State {
                write_to_adjacency_csr(&path.join(adj_path), &geom.adj_list)?;
                let (k, h) = sha256_file(&adj_path, path)?;
                hashes.insert(k, FileHash { sha256: h });
            }
        }

        Ok(())
    }

    fn read_from_pack(&mut self, path: &Path) -> Result<()> {
        let name = self.ty.to_str();
        let entity_path = path.join(format!("entities/{}.parquet", name));
        let elec_path = path.join(format!("elections/{name}.parquet"));
        let demo_path = path.join(format!("demographics/{name}.parquet"));
        let geom_path = path.join(format!("geometries/{name}.fgb"));
        let adj_path = path.join(format!("geometries/adjacencies/{name}.fgb"));

        if entity_path.exists() {
            let df = read_from_parquet(&entity_path)?;
            self.df_to_entities(&df, self.ty)?;

            // rebuild index
            self.index.clear();
            for (i, e) in self.entities.iter().enumerate() {
                self.index.insert(e.geo_id.clone(), i as u32);
            }
        }
        if elec_path.exists() { self.elec_data = Some(read_from_parquet(&elec_path)?); }
        if demo_path.exists() { self.demo_data = Some(read_from_parquet(&demo_path)?); }
        if geom_path.exists() { 
            self.geoms = Some(PlanarPartition::new(read_from_geoparquet(&geom_path)?));
            self.geoms.as_mut().unwrap().adj_list = read_from_adjacency_csr(&adj_path)?
        }

        Ok(())
    }
}

impl Map {
    pub fn write_to_pack(&self, path: &Path) -> Result<()> {
        let dirs = [
            "entities",
            "elections",
            "demographics",
            "geometries",
            "geometries/adjacencies",
            "meta",
        ];
        ensure_dirs(path, &dirs)?;

        let mut file_hashes: BTreeMap<String, FileHash> = BTreeMap::new();
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();

        for ty in GeoType::order() {
            self.get_layer(ty).write_to_pack(path, &mut counts, &mut file_hashes)?;
        }

        // Manifest
        let meta_path = path.join("meta/manifest.json");
        let manifest = Manifest::new(path, counts, file_hashes);
        let mut f = File::create(&meta_path)?;
        serde_json::to_writer_pretty(&mut f, &manifest)?;

        Ok(())
    }

    pub fn read_from_pack(path: &Path) -> Result<Self> {
        let mut map_data = Self::default();
    
        // Load per-level content
        for ty in GeoType::order() {
            map_data.get_layer_mut(ty).read_from_pack(path)?;
        }
    
        Ok(map_data)
    }
}
