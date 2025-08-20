use std::{path::Path};

use anyhow::{anyhow, Ok, Result};
use polars::prelude::*;

use openmander_geometry::PlanarPartition;

use crate::{common::{data::*, polygon::*}, types::*};

impl MapLayer {
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
    pub fn read_from_pack(path: &Path) -> Result<Self> {
        let mut map_data = Self::default();
    
        // Load per-level content
        for ty in GeoType::order() {
            map_data.get_layer_mut(ty).read_from_pack(path)?;
        }
    
        Ok(map_data)
    }
}
