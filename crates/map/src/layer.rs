use std::{collections::HashMap, fmt};

use polars::{frame::DataFrame};

use super::{geo_id::{GeoId, GeoType}, geom::Geometries};

/// Quick way to access parent entities across levels.
#[derive(Debug, Clone, Default)]
pub struct ParentRefs {
    pub state: Option<GeoId>,
    pub county: Option<GeoId>,
    pub tract: Option<GeoId>,
    pub group: Option<GeoId>,
    pub vtd: Option<GeoId>,
}

impl ParentRefs {
    pub fn get(&self, ty: GeoType) -> Option<&GeoId> {
        match ty {
            GeoType::State => self.state.as_ref(),
            GeoType::County => self.county.as_ref(),
            GeoType::Tract => self.tract.as_ref(),
            GeoType::Group => self.group.as_ref(),
            GeoType::VTD => self.vtd.as_ref(),
            GeoType::Block => None
        }
    }

    pub fn set(&mut self, ty: GeoType, value: Option<GeoId>) {
        match ty {
            GeoType::State => self.state = value,
            GeoType::County => self.county = value,
            GeoType::Tract => self.tract = value,
            GeoType::Group => self.group = value,
            GeoType::VTD => self.vtd = value,
            GeoType::Block => ()
        }
    }
}

/// A single planar partition Layer of the map, containing entities and their relationships.
pub struct MapLayer {
    pub ty: GeoType,
    pub geo_ids: Vec<GeoId>,
    pub index: HashMap<GeoId, u32>, // Map between geo_ids and per-level contiguous indices
    pub parents: Vec<ParentRefs>, // References to parent entities (higher level types)
    pub data: DataFrame, // Entity data (incl. name, centroid, geographic data, election data)
    pub adjacencies: Vec<Vec<u32>>,
    pub shared_perimeters: Vec<Vec<f64>>,
    pub geoms: Option<Geometries>, // Per-level geometry store, indexed by entities
}

impl MapLayer {
    pub fn new(ty: GeoType) -> Self {
        Self {
            ty,
            geo_ids: Vec::new(),
            index: HashMap::new(),
            parents: Vec::new(),
            data: DataFrame::empty(),
            adjacencies: Vec::new(),
            shared_perimeters: Vec::new(),
            geoms: None,
        }
    }

    #[inline] pub fn len(&self) -> usize { self.geo_ids.len() }
}

impl fmt::Debug for MapLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n_entities = self.geo_ids.len();
        let n_index    = self.index.len();
        let n_parents  = self.parents.len();

        // DataFrame stats
        let df_rows = self.data.height();
        let df_cols = self.data.width();
        let col_names = self.data.get_column_names();
        let dtypes = self.data.dtypes();
        let cols_fmt: Vec<String> = col_names
            .iter()
            .zip(dtypes.iter())
            .map(|(n, dt)| format!("{n}: {:?}", dt))
            .collect();

        // Show up to 16 column descriptors unless pretty-printed with {:#?}
        let show_all = f.alternate();
        let max_cols = if show_all { cols_fmt.len() } else { cols_fmt.len().min(16) };
        let cols_preview = &cols_fmt[..max_cols];
        let cols_more = cols_fmt.len().saturating_sub(max_cols);

        // Adjacency stats
        let (deg_sum, max_deg) = self.adjacencies.iter().fold((0usize, 0usize), |(sum, mx), row| {
            let d = row.len();
            (sum + d, mx.max(d))
        });
        let avg_deg = if n_entities > 0 { (deg_sum as f64) / (n_entities as f64) } else { 0.0 };

        // Shared-perimeter stats
        let mut sp_nnz = 0usize;
        let mut sp_sum = 0.0_f64;
        let mut sp_max = 0.0_f64;
        for row in &self.shared_perimeters {
            sp_nnz += row.len();
            for &w in row {
                sp_sum += w;
                if w > sp_max { sp_max = w; }
            }
        }

        // Parent coverage
        let mut cnt_state = 0usize;
        let mut cnt_county = 0usize;
        let mut cnt_tract = 0usize;
        let mut cnt_group = 0usize;
        let mut cnt_vtd = 0usize;
        for p in &self.parents {
            if p.state.is_some()  { cnt_state += 1; }
            if p.county.is_some() { cnt_county += 1; }
            if p.tract.is_some()  { cnt_tract += 1; }
            if p.group.is_some()  { cnt_group += 1; }
            if p.vtd.is_some()    { cnt_vtd += 1; }
        }

        // Geometry presence (don’t assume internals of Geometries)
        let geoms_present = self.geoms.is_some();

        // Optional small preview of first few IDs in pretty mode
        let geo_id_preview: Vec<&str> = if show_all {
            self.geo_ids.iter().take(5).map(|g| g.id.as_ref()).collect()
        } else {
            Vec::new()
        };

        let mut dbg = f.debug_struct("MapLayer");
        dbg.field("ty", &self.ty)
            .field("entities", &n_entities)
            .field("index_size", &n_index)
            .field("parents_rows", &n_parents)
            .field("data_rows", &df_rows)
            .field("data_cols", &df_cols)
            .field("data_cols_preview", &cols_preview)
            .field("data_cols_more", &cols_more)
            .field("adjacency_nnz", &deg_sum)
            .field("avg_degree", &avg_deg)
            .field("max_degree", &max_deg)
            .field("shared_perimeters_nnz", &sp_nnz)
            .field("shared_perimeters_sum", &sp_sum)
            .field("shared_perimeters_max", &sp_max)
            .field("parents_coverage", &format_args!(
                "state {}/{}; county {}/{}; tract {}/{}; group {}/{}; vtd {}/{}",
                cnt_state, n_entities, cnt_county, n_entities, cnt_tract, n_entities,
                cnt_group, n_entities, cnt_vtd, n_entities,
            ))
            .field("geoms_present", &geoms_present);

        if show_all {
            dbg.field("geo_id_preview", &geo_id_preview);
        }

        dbg.finish()
    }
}
