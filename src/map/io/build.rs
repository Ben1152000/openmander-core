use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use shapefile::dbase::{FieldValue, Record};

use crate::{
    ParentRefs,
    map::{GeoId, GeoType, Map, MapLayer, util},
};

/// Intermediate data for one layer during the build phase.
///
/// Replaces `unit_data: Option<DataFrame>`.  Numeric columns are separated by
/// type to avoid mixed-type dispatch; string columns beyond `geo_ids` and
/// `unit_names` are not stored (they become `parents` on `MapLayer`).
pub(crate) struct BuildLayerData {
    pub geo_ids:    Vec<String>,   // join key; mirrors MapLayer.geo_ids order
    pub unit_names: Vec<String>,   // "name" column
    pub i64_cols:   Vec<(String, Vec<i32>)>,
    pub f64_cols:   Vec<(String, Vec<f64>)>,
}

/// Incoming block-level data (from a CSV/txt file or computed values).
/// Contains an id column plus typed numeric columns.
struct IncomingData {
    id_col:   Vec<String>,   // GEOID or similar join key
    i64_cols: Vec<(String, Vec<i32>)>,
    f64_cols: Vec<(String, Vec<f64>)>,
}

impl BuildLayerData {
    fn len(&self) -> usize { self.geo_ids.len() }

    /// Append columns from `incoming` to this layer by inner-joining on geo_id.
    ///
    /// For each unit in self, looks up its geo_id in `incoming.id_col` and
    /// copies the matching numeric values.  Units with no match are left as 0.
    fn merge_data(&mut self, incoming: &IncomingData, id_col_name: &str) {
        // Build lookup: incoming id → row index.
        let id_to_row: HashMap<&str, usize> = incoming.id_col.iter()
            .enumerate()
            .map(|(i, s)| (s.as_str(), i))
            .collect();

        let n = self.len();

        for (name, values) in &incoming.i64_cols {
            let mut col = vec![0i32; n];
            for (row, geo_id) in self.geo_ids.iter().enumerate() {
                if let Some(&src_row) = id_to_row.get(geo_id.as_str()) {
                    col[row] = values[src_row];
                }
            }
            self.i64_cols.push((name.clone(), col));
        }

        for (name, values) in &incoming.f64_cols {
            let mut col = vec![0.0f64; n];
            for (row, geo_id) in self.geo_ids.iter().enumerate() {
                if let Some(&src_row) = id_to_row.get(geo_id.as_str()) {
                    col[row] = values[src_row];
                }
            }
            self.f64_cols.push((name.clone(), col));
        }

        // Suppress unused variable warning when the caller provides id_col_name.
        let _ = id_col_name;
    }

    /// Convert to `WeightMatrix` + `unit_names`, consuming self.
    fn finalize(self) -> (Vec<String>, crate::graph::WeightMatrix) {
        use ndarray::Array2;

        let n = self.geo_ids.len();
        let n_i64 = self.i64_cols.len();
        let n_f64 = self.f64_cols.len();

        let mut i64_data = Array2::<i32>::zeros((n, n_i64));
        let mut f64_data = Array2::<f64>::zeros((n, n_f64));

        for (c, (_, values)) in self.i64_cols.iter().enumerate() {
            for (r, &v) in values.iter().enumerate() {
                i64_data[(r, c)] = v;
            }
        }
        for (c, (_, values)) in self.f64_cols.iter().enumerate() {
            for (r, &v) in values.iter().enumerate() {
                f64_data[(r, c)] = v;
            }
        }

        let i64_series = self.i64_cols.into_iter().map(|(name, _)| name).collect();
        let f64_series = self.f64_cols.into_iter().map(|(name, _)| name).collect();

        let weights = crate::graph::WeightMatrix::from_arrays(i64_series, i64_data, f64_series, f64_data);
        (self.unit_names, weights)
    }
}

impl MapLayer {
    /// Load layer geometries from a TIGER/PL shapefile, returning the layer and
    /// its initial `BuildLayerData` (shapefile attribute columns only).
    fn from_tiger_shapefile(ty: GeoType, path: &Path) -> Result<(Self, BuildLayerData)> {
        let (shapes, records) = crate::io::shp::read_shapefile(path)?;

        fn get_character_field(record: &Record, field: &str) -> Result<String> {
            match record.get(field) {
                Some(FieldValue::Character(Some(s))) => Ok(s.trim().to_string()),
                _ => bail!("missing or invalid character field: {}", field),
            }
        }

        fn get_numeric_field(record: &Record, field: &str) -> Result<f64> {
            match record.get(field) {
                Some(FieldValue::Numeric(Some(n))) => Ok(*n),
                _ => bail!("missing or invalid numeric field: {}", field),
            }
        }

        let n = records.len();
        let mut geo_id_strs:     Vec<String> = Vec::with_capacity(n);
        let mut unit_names:      Vec<String> = Vec::with_capacity(n);
        let mut centroid_lons:   Vec<f64>    = Vec::with_capacity(n);
        let mut centroid_lats:   Vec<f64>    = Vec::with_capacity(n);
        let mut area_m2_vals:    Vec<f64>    = Vec::with_capacity(n);
        let mut land_m2_vals:    Vec<f64>    = Vec::with_capacity(n);
        let mut water_m2_vals:   Vec<f64>    = Vec::with_capacity(n);

        for record in &records {
            geo_id_strs.push(get_character_field(record, "GEOID20")?);
            unit_names.push(match ty {
                GeoType::County | GeoType::Group => get_character_field(record, "NAMELSAD20")?,
                _ => get_character_field(record, "NAME20")?,
            });
            centroid_lons.push(get_character_field(record, "INTPTLON20")?.trim().parse()?);
            centroid_lats.push(get_character_field(record, "INTPTLAT20")?.trim().parse()?);
            let aland  = get_numeric_field(record, "ALAND20")?;
            let awater = get_numeric_field(record, "AWATER20")?;
            area_m2_vals.push(aland + awater);
            land_m2_vals.push(aland);
            water_m2_vals.push(awater);
        }

        let multipolygons: Vec<geo::MultiPolygon<f64>> = shapes.into_iter()
            .map(crate::io::shp::shape_to_multipolygon)
            .collect::<Result<Vec<_>>>()
            .with_context(|| format!(
                "Error converting shapes to multipolygons in shapefile: {}",
                path.display()
            ))?;

        let region = geograph::Region::new(multipolygons, None)
            .map_err(|e| anyhow!("Region construction failed for {:?}: {}: {:?}", ty, path.display(), e))?;

        let geo_ids: Vec<GeoId> = geo_id_strs.iter()
            .map(|s| GeoId::new(ty, s))
            .collect();
        let index = geo_ids.iter().enumerate()
            .map(|(i, g)| (g.clone(), i as u32))
            .collect();
        let parents = vec![ParentRefs::default(); n];

        // Initial BuildLayerData: shapefile columns only.
        let build_data = BuildLayerData {
            geo_ids: geo_id_strs,
            unit_names,
            i64_cols: vec![],
            f64_cols: vec![
                ("centroid_lon".to_string(), centroid_lons),
                ("centroid_lat".to_string(), centroid_lats),
                ("area_m2".to_string(),      area_m2_vals),
                ("land_m2".to_string(),      land_m2_vals),
                ("water_m2".to_string(),     water_m2_vals),
            ],
        };

        // Initial WeightMatrix from shapefile columns only (updated in finalize_build).
        let (_, initial_weights) = BuildLayerData {
            geo_ids: build_data.geo_ids.clone(),
            unit_names: build_data.unit_names.clone(),
            i64_cols: build_data.i64_cols.clone(),
            f64_cols: build_data.f64_cols.clone(),
        }.finalize();

        let layer = MapLayer::new(
            ty, geo_ids, index, parents,
            build_data.unit_names.clone(),
            Arc::new(initial_weights),
            Arc::new(region),
        );

        Ok((layer, build_data))
    }

    /// Assign parent references for each entity, based on their truncated geo_id.
    fn assign_parents(&mut self, parent_ty: GeoType) {
        self.geo_ids.iter().enumerate()
            .map(|(i, geo_id)| self.parents[i].set(parent_ty, Some(geo_id.to_parent(parent_ty))))
            .collect()
    }

    /// Assign VTD parent references from a crosswalk map, with a spatial fallback for any
    /// blocks absent from the crosswalk.
    ///
    /// The Census block/VTD crosswalk files are occasionally incomplete: some blocks have no
    /// listed VTD.  For those blocks, this method finds the containing VTD by testing the
    /// block's pre-cached centroid against the VTD layer's spatial index.
    ///
    /// `vtd_region` and `vtd_geo_ids` must be from the VTD `MapLayer` and indexed in the same
    /// order (i.e. `vtd_geo_ids[uid.0]` is the geo_id for VTD unit `uid`).
    fn assign_vtd_parents(
        &mut self,
        parent_map: HashMap<GeoId, GeoId>,
        vtd_region: &geograph::Region,
        vtd_geo_ids: &[GeoId],
        verbose: u8,
    ) -> Result<()> {
        // Pre-locate the Census interior-point columns (INTPTLON20/INTPTLAT20 from the
        // shapefile, stored as "centroid_lon"/"centroid_lat").  These are guaranteed to
        // lie inside the polygon, unlike the DCEL centroid which is a vertex average.
        let f64_names = self.unit_weights.f64_series_names();
        let lon_col = f64_names.iter().position(|&n| n == "centroid_lon");
        let lat_col = f64_names.iter().position(|&n| n == "centroid_lat");

        let mut fallback_count = 0;
        self.geo_ids.iter().enumerate()
            .try_for_each(|(i, geo_id)| -> Result<()> {
                if let Some(parent) = parent_map.get(geo_id) {
                    self.parents[i].set(GeoType::VTD, Some(parent.clone()));
                    return Ok(());
                }
                // Crosswalk miss: use the Census interior point if available, otherwise
                // fall back to the DCEL centroid.
                let probe = match (lon_col, lat_col) {
                    (Some(li), Some(la)) => {
                        let row = self.unit_weights.f64_row(i);
                        geo::Coord { x: row[li], y: row[la] }
                    }
                    _ => self.region.centroid(geograph::UnitId(i as u32)),
                };
                let vtd_uid = vtd_region.unit_at(probe)
                    .ok_or_else(|| anyhow!(
                        "block {:?} missing from VTD crosswalk and interior point ({:.6}, {:.6}) \
                         does not fall inside any VTD polygon",
                        geo_id, probe.x, probe.y
                    ))?;
                self.parents[i].set(GeoType::VTD, Some(vtd_geo_ids[vtd_uid.0 as usize].clone()));
                fallback_count += 1;
                Ok(())
            })?;
        if verbose > 0 && fallback_count > 0 {
            eprintln!(
                "[build_pack] {} block(s) missing from VTD crosswalk — assigned by centroid fallback",
                fallback_count
            );
        }
        Ok(())
    }

    /// Bake manual island-bridge patches into the block Region.
    fn patch_region(&mut self) -> Result<()> {
        let patches = [
            // Washington County, Rhode Island
            (GeoId::new_block("440099902000001"), GeoId::new_block("440099901000017")),
            // Monroe County, Florida
            (GeoId::new_block("120879801001000"), GeoId::new_block("120879900000030")),
            // San Francisco County, California
            (GeoId::new_block("060759804011000"), GeoId::new_block("060759901000001")),
            // Ventura County, California
            (GeoId::new_block("061119901000013"), GeoId::new_block("061119901000008")),
            (GeoId::new_block("060839900000034"), GeoId::new_block("061119901000008")),
            (GeoId::new_block("061119901000011"), GeoId::new_block("060839900000034")),
            // Los Angeles County, California
            (GeoId::new_block("060375991002000"), GeoId::new_block("060379903000010")),
            (GeoId::new_block("060375991001000"), GeoId::new_block("060375991002015")),
            // Fulton County, Kentucky
            (GeoId::new_block("210759602004105"), GeoId::new_block("210759602004000")),
            // New York County, New York
            (GeoId::new_block("360610001001000"), GeoId::new_block("360610005000003")),
            (GeoId::new_block("360610001001001"), GeoId::new_block("360610005000003")),
            // Hawaii County & Maui County, Hawaii
            (GeoId::new_block("150019912000001"), GeoId::new_block("150099902000018")),
            // Maui County, Hawaii
            (GeoId::new_block("150099902000009"), GeoId::new_block("150099900000006")),
            (GeoId::new_block("150099912000001"), GeoId::new_block("150099900000007")),
            // Maui County & Honolulu County, Hawaii
            (GeoId::new_block("150099900000001"), GeoId::new_block("150039900010039")),
            // Honolulu County & Kauai County, Hawaii
            (GeoId::new_block("150079901000004"), GeoId::new_block("150039900010008")),
            // Kauai County, Hawaii
            (GeoId::new_block("150079902000001"), GeoId::new_block("150079901000014")),
            (GeoId::new_block("150079903000002"), GeoId::new_block("150079902000001")),
            // Northwestern Hawaiian Islands
            (GeoId::new_block("150039812001003"), GeoId::new_block("150079903000002")),
            (GeoId::new_block("150039812001005"), GeoId::new_block("150039812001003")),
            (GeoId::new_block("150039812001008"), GeoId::new_block("150039812001005")),
            (GeoId::new_block("150039812001025"), GeoId::new_block("150039812001008")),
            (GeoId::new_block("150039812001015"), GeoId::new_block("150039812001025")),
            (GeoId::new_block("150039812001016"), GeoId::new_block("150039812001015")),
            (GeoId::new_block("150039812001018"), GeoId::new_block("150039812001016")),
            (GeoId::new_block("150039812001021"), GeoId::new_block("150039812001018")),
            (GeoId::new_block("150039812001000"), GeoId::new_block("150039812001021")),
            // Chugach Census Area, Alaska
            (GeoId::new_block("020630002001316"), GeoId::new_block("020630003005430")),
            // Kodiak Island Borough, Alaska
            (GeoId::new_block("021500001002002"), GeoId::new_block("021220012001250")),
            (GeoId::new_block("021500001002084"), GeoId::new_block("021220012001250")),
            (GeoId::new_block("021500001001349"), GeoId::new_block("021500001001246")),
            (GeoId::new_block("021500001001293"), GeoId::new_block("021640001001354")),
            // Aleutians East Borough, Alaska
            (GeoId::new_block("020130001003109"), GeoId::new_block("020130001003103")),
            (GeoId::new_block("020130001002200"), GeoId::new_block("020130001002182")),
            (GeoId::new_block("020130001001291"), GeoId::new_block("020130001001068")),
            (GeoId::new_block("020130001001280"), GeoId::new_block("020130001002194")),
            (GeoId::new_block("020130001001212"), GeoId::new_block("020130001001132")),
            (GeoId::new_block("020130001001290"), GeoId::new_block("020130001001001")),
            // Aleutians West Census Area, Alaska
            (GeoId::new_block("020160001001475"), GeoId::new_block("020160001001242")),
            (GeoId::new_block("020160001001345"), GeoId::new_block("020160001001242")),
            (GeoId::new_block("020160001001000"), GeoId::new_block("020160001001242")),
            (GeoId::new_block("020160001001297"), GeoId::new_block("020160001001242")),
            (GeoId::new_block("020160001001302"), GeoId::new_block("020160001001237")),
            (GeoId::new_block("020160001001350"), GeoId::new_block("020160001001310")),
            (GeoId::new_block("020160001001429"), GeoId::new_block("020160001001351")),
            (GeoId::new_block("020160001001391"), GeoId::new_block("020160001001432")),
            (GeoId::new_block("020160001001137"), GeoId::new_block("020160001001390")),
            (GeoId::new_block("020160001001439"), GeoId::new_block("020160001001051")),
            (GeoId::new_block("020160001001472"), GeoId::new_block("020160001001051")),
            (GeoId::new_block("020160001001435"), GeoId::new_block("020160001001177")),
            (GeoId::new_block("020160001001334"), GeoId::new_block("020160001001187")),
            (GeoId::new_block("020160001001317"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001447"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001469"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001275"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001293"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001312"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001315"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001320"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001348"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001364"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001393"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001427"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001454"), GeoId::new_block("020160001001182")),
            (GeoId::new_block("020160001001463"), GeoId::new_block("020160001001182")),
            // Dillingham Census Area, Alaska
            (GeoId::new_block("020700001001241"), GeoId::new_block("020700001001059")),
            (GeoId::new_block("020700001001268"), GeoId::new_block("020700001001059")),
            // Bethel Census Area, Alaska
            (GeoId::new_block("020500001001141"), GeoId::new_block("020500001001052")),
            (GeoId::new_block("020500001001061"), GeoId::new_block("020500001001113")),
            (GeoId::new_block("020500001001161"), GeoId::new_block("020500001001113")),
            // Nome Census Area, Alaska
            (GeoId::new_block("021800001001600"), GeoId::new_block("021800001001319")),
            (GeoId::new_block("021800001001605"), GeoId::new_block("021800001001297")),
            (GeoId::new_block("021800001001590"), GeoId::new_block("021800001001184")),
            (GeoId::new_block("021800001001608"), GeoId::new_block("021800001001184")),
            (GeoId::new_block("021800001001597"), GeoId::new_block("021800001001180")),
            (GeoId::new_block("021800001001582"), GeoId::new_block("021800001001180")),
        ];

        let unit_pairs: Vec<(geograph::UnitId, geograph::UnitId)> = patches.iter()
            .filter_map(|(left, right)| {
                let a = self.index.get(left).copied()?;
                let b = self.index.get(right).copied()?;
                Some((geograph::UnitId(a), geograph::UnitId(b)))
            })
            .collect();

        if unit_pairs.is_empty() { return Ok(()); }

        let region = (*self.region).clone();
        self.region = Arc::new(region.with_forced_adjacencies(&unit_pairs));

        Ok(())
    }

    /// Compute outer perimeters from the block Region.
    fn compute_outer_perimeters_from_region(&self) -> IncomingData {
        let region = &*self.region;
        let geo_ids: Vec<String> = self.geo_ids.iter()
            .map(|g| g.id().to_string())
            .collect();
        let outer_perimeters: Vec<f64> = (0..self.len())
            .map(|i| region.exterior_boundary_length(geograph::UnitId(i as u32)))
            .collect();

        IncomingData {
            id_col:   geo_ids,
            i64_cols: vec![],
            f64_cols: vec![("outer_perimeter_m".to_string(), outer_perimeters)],
        }
    }
}

impl Map {
    /// Aggregate `incoming` from `child_ty` up to `parent_ty` by summing numeric columns.
    fn aggregate_data(
        &self,
        incoming: &IncomingData,
        child_ty: GeoType,
        parent_ty: GeoType,
    ) -> Result<IncomingData> {
        let layer = self.layer(child_ty)
            .ok_or_else(|| anyhow!("[Map.aggregate_data] Missing layer {:?}", child_ty))?;

        // Map each incoming id to its parent geo_id, then group and sum.
        // Rows whose geo_id doesn't exist in the block index are skipped with a warning
        // (Census election files sometimes contain null-block sentinel rows like "XXYY0000000000").
        let mut parent_to_row: HashMap<String, usize> = HashMap::new();
        let mut result_ids:  Vec<String>       = Vec::new();
        let mut i64_sums:    Vec<Vec<i32>>     = vec![vec![]; incoming.i64_cols.len()];
        let mut f64_sums:    Vec<Vec<f64>>     = vec![vec![]; incoming.f64_cols.len()];

        for (row, id) in incoming.id_col.iter().enumerate() {
            let geo_id = GeoId::new(child_ty, id);
            let Some(&unit_idx) = layer.index.get(&geo_id) else {
                eprintln!("[aggregate_data] skipping {:?} geo_id {:?} — not found in block index", child_ty, id);
                continue;
            };
            let parent = layer.parents[unit_idx as usize]
                .get(parent_ty)
                .ok_or_else(|| anyhow!(
                    "parent ref {:?} not set for geo_id {:?} in {:?}",
                    parent_ty, id, child_ty
                ))?;
            let parent_id = parent.id().to_string();

            let result_row = match parent_to_row.get(&parent_id) {
                Some(&r) => r,
                None => {
                    let r = result_ids.len();
                    result_ids.push(parent_id.clone());
                    parent_to_row.insert(parent_id, r);
                    for s in i64_sums.iter_mut() { s.push(0); }
                    for s in f64_sums.iter_mut() { s.push(0.0); }
                    r
                }
            };
            for (c, (_, vals)) in incoming.i64_cols.iter().enumerate() {
                i64_sums[c][result_row] += vals[row];
            }
            for (c, (_, vals)) in incoming.f64_cols.iter().enumerate() {
                f64_sums[c][result_row] += vals[row];
            }
        }

        Ok(IncomingData {
            id_col:   result_ids,
            i64_cols: incoming.i64_cols.iter().zip(i64_sums)
                .map(|((name, _), sums)| (name.clone(), sums))
                .collect(),
            f64_cols: incoming.f64_cols.iter().zip(f64_sums)
                .map(|((name, _), sums)| (name.clone(), sums))
                .collect(),
        })
    }

    /// Merge block-level data into all layers, aggregating to higher levels.
    fn merge_block_data(
        &self,
        build_data: &mut HashMap<GeoType, BuildLayerData>,
        incoming: IncomingData,
        id_col_name: &str,
    ) -> Result<()> {
        // Aggregate and merge into each non-block layer.
        for &ty in GeoType::ALL.iter().filter(|&&ty| ty != GeoType::Block) {
            if let Some(data) = build_data.get_mut(&ty) {
                let aggregated = self.aggregate_data(&incoming, GeoType::Block, ty)?;
                data.merge_data(&aggregated, id_col_name);
            }
        }

        // Merge into the block layer directly.
        if let Some(data) = build_data.get_mut(&GeoType::Block) {
            data.merge_data(&incoming, id_col_name);
        }

        Ok(())
    }

    /// Build a map pack from the download files in `input_dir`.
    #[cfg(feature = "download")]
    pub(crate) fn build_pack(
        input_dir: &Path,
        state_code: &str,
        fips: &str,
        has_vtd: bool,
        verbose: u8,
    ) -> Result<Self> {
        util::require_dir_exists(input_dir)?;

        let mut map = Self::default();
        let mut build_data: HashMap<GeoType, BuildLayerData> = HashMap::new();

        macro_rules! load_layer {
            ($ty:expr, $path:expr) => {{
                if verbose > 0 { eprintln!("[build_pack] loading {} shapes", $ty.to_str()); }
                let (layer, data) = MapLayer::from_tiger_shapefile($ty, &input_dir.join($path))?;
                build_data.insert($ty, data);
                map.insert(layer);
            }};
        }

        load_layer!(GeoType::State,
            format!("tl_2020_{fips}_state20/tl_2020_{fips}_state20.shp"));
        load_layer!(GeoType::County,
            format!("tl_2020_{fips}_county20/tl_2020_{fips}_county20.shp"));
        load_layer!(GeoType::Tract,
            format!("tl_2020_{fips}_tract20/tl_2020_{fips}_tract20.shp"));
        load_layer!(GeoType::Group,
            format!("tl_2020_{fips}_bg20/tl_2020_{fips}_bg20.shp"));

        if has_vtd {
            if verbose > 0 { eprintln!("[build_pack] loading vtd shapes"); }
            let (layer, data) = MapLayer::from_tiger_shapefile(
                GeoType::VTD,
                &input_dir.join(format!("tl_2020_{fips}_vtd20/tl_2020_{fips}_vtd20.shp")),
            )?;
            build_data.insert(GeoType::VTD, data);
            map.insert(layer);
        }

        load_layer!(GeoType::Block,
            format!("tl_2020_{fips}_tabblock20/tl_2020_{fips}_tabblock20.shp"));

        // Compute parent references for all layers.
        if verbose > 0 { eprintln!("[build_pack] computing crosswalks"); }
        if let Some(layer) = map.layer_mut(GeoType::County) {
            layer.assign_parents(GeoType::State);
        }
        if let Some(layer) = map.layer_mut(GeoType::Tract) {
            layer.assign_parents(GeoType::State);
            layer.assign_parents(GeoType::County);
        }
        if let Some(layer) = map.layer_mut(GeoType::Group) {
            layer.assign_parents(GeoType::State);
            layer.assign_parents(GeoType::County);
            layer.assign_parents(GeoType::Tract);
        }
        if let Some(layer) = map.layer_mut(GeoType::VTD) {
            layer.assign_parents(GeoType::State);
            layer.assign_parents(GeoType::County);
        }
        if let Some(layer) = map.layer_mut(GeoType::Block) {
            layer.assign_parents(GeoType::State);
            layer.assign_parents(GeoType::County);
            layer.assign_parents(GeoType::Tract);
            layer.assign_parents(GeoType::Group);
        }

        if has_vtd {
            if verbose > 0 { eprintln!("[build_pack] loading block -> vtd crosswalks"); }
            let crosswalk_path = input_dir.join(format!(
                "BlockAssign_ST{fips}_{state_code}/BlockAssign_ST{fips}_{state_code}_VTD.txt"
            ));
            let parent_map = read_crosswalk_txt(&crosswalk_path)?;
            // Clone the VTD region Arc and geo_ids before taking a mutable borrow on block.
            let vtd_region = map.layer(GeoType::VTD)
                .map(|l| Arc::clone(&l.region));
            let vtd_geo_ids = map.layer(GeoType::VTD)
                .map(|l| l.geo_ids.clone());
            if let (Some(layer), Some(vtd_region), Some(vtd_geo_ids)) =
                (map.layer_mut(GeoType::Block), vtd_region, vtd_geo_ids)
            {
                layer.assign_vtd_parents(parent_map, &vtd_region, &vtd_geo_ids, verbose)?;
            }
        }

        if verbose > 0 { eprintln!("[build_pack] loading demographic data"); }
        let demo_path = input_dir.join(format!(
            "Demographic_Data_Block_{state_code}/demographic_data_block_{state_code}.v06.csv"
        ));
        let demo_data = read_block_csv(&demo_path, "GEOID")?;
        map.merge_block_data(&mut build_data, demo_data, "GEOID")?;

        if verbose > 0 { eprintln!("[build_pack] loading election data"); }
        let elec_path = input_dir.join(format!(
            "Election_Data_Block_{state_code}/election_data_block_{state_code}.v06.csv"
        ));
        let elec_data = read_block_csv(&elec_path, "GEOID")?;
        map.merge_block_data(&mut build_data, elec_data, "GEOID")?;

        // Bake island-bridge patches.
        if verbose > 0 { eprintln!("[build_pack] patching island bridges"); }
        if let Some(block_layer) = map.layer_mut(GeoType::Block) {
            block_layer.patch_region()?;
        }

        // Compute outer perimeters and merge.
        if verbose > 0 { eprintln!("[build_pack] computing outer perimeters"); }
        let outer_perimeters = map.layer(GeoType::Block)
            .ok_or_else(|| anyhow!("Missing block layer"))?
            .compute_outer_perimeters_from_region();
        map.merge_block_data(&mut build_data, outer_perimeters, "GEOID")?;

        // Finalize: convert BuildLayerData → WeightMatrix + unit_names.
        if verbose > 0 { eprintln!("[build_pack] finalizing weights"); }
        for (ty, data) in build_data.drain() {
            let (unit_names, weights) = data.finalize();
            if let Some(layer) = map.layer_mut(ty) {
                layer.unit_names    = unit_names;
                layer.unit_weights  = Arc::new(weights);
            }
        }

        Ok(map)
    }
}

/// Read a block-level CSV (demographic or election data) into `IncomingData`.
///
/// Expects a header row with a GEOID column and any number of numeric columns.
/// If GEOID looks like an integer (no leading zeros to preserve), it is
/// zero-padded to 15 digits.
#[cfg(feature = "download")]
fn read_block_csv(path: &Path, id_col_name: &str) -> Result<IncomingData> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open CSV: {}", path.display()))?;

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    let headers: Vec<String> = rdr
        .headers()
        .context("failed to read CSV headers")?
        .iter()
        .map(|s| s.to_string())
        .collect();

    // Find the id column index.
    let id_col_idx = headers.iter().position(|h| h == id_col_name)
        .ok_or_else(|| anyhow!("CSV missing id column '{}'", id_col_name))?;

    // Sniff numeric column types from the first data row.
    let first_record = rdr.records().next()
        .context("CSV has no data rows")?
        .context("failed to read first CSV record")?;

    enum ColKind { Id, I64, F64, Skip }

    let col_kinds: Vec<ColKind> = headers.iter().enumerate().map(|(ci, _name)| {
        if ci == id_col_idx {
            return ColKind::Id;
        }
        let val = first_record.get(ci).unwrap_or("");
        if val.is_empty() {
            return ColKind::I64; // default to i64 for empty
        }
        // Try i64 first; if it fails or contains '.', use f64.
        if val.contains('.') || val.contains('e') || val.contains('E') {
            ColKind::F64
        } else if val.parse::<i64>().is_ok() {
            ColKind::I64
        } else {
            ColKind::Skip
        }
    }).collect();

    let i64_col_indices: Vec<usize> = col_kinds.iter().enumerate()
        .filter_map(|(ci, k)| if matches!(k, ColKind::I64) { Some(ci) } else { None })
        .collect();
    let f64_col_indices: Vec<usize> = col_kinds.iter().enumerate()
        .filter_map(|(ci, k)| if matches!(k, ColKind::F64) { Some(ci) } else { None })
        .collect();

    let mut id_col:   Vec<String>              = Vec::new();
    let mut i64_cols: Vec<(String, Vec<i32>)>  = i64_col_indices.iter()
        .map(|&ci| (headers[ci].clone(), Vec::new()))
        .collect();
    let mut f64_cols: Vec<(String, Vec<f64>)>  = f64_col_indices.iter()
        .map(|&ci| (headers[ci].clone(), Vec::new()))
        .collect();

    // Helper to zero-pad GEOID to 15 digits if it looks numeric.
    let pad_geoid = |s: &str| -> String {
        if s.len() < 15 && s.chars().all(|c| c.is_ascii_digit()) {
            format!("{:0>15}", s)
        } else {
            s.to_string()
        }
    };

    // Process the first record (already read for sniffing).
    {
        let id_val = first_record.get(id_col_idx).unwrap_or("");
        id_col.push(pad_geoid(id_val));
        for (out_idx, &ci) in i64_col_indices.iter().enumerate() {
            let v = first_record.get(ci).unwrap_or("0");
            i64_cols[out_idx].1.push(v.parse::<i32>().unwrap_or(0));
        }
        for (out_idx, &ci) in f64_col_indices.iter().enumerate() {
            let v = first_record.get(ci).unwrap_or("0");
            f64_cols[out_idx].1.push(v.parse::<f64>().unwrap_or(0.0));
        }
    }

    // Process remaining records.
    let mut record = csv::StringRecord::new();
    while rdr.read_record(&mut record).context("failed to read CSV record")? {
        let id_val = record.get(id_col_idx).unwrap_or("");
        id_col.push(pad_geoid(id_val));
        for (out_idx, &ci) in i64_col_indices.iter().enumerate() {
            let v = record.get(ci).unwrap_or("0");
            i64_cols[out_idx].1.push(v.parse::<i32>().unwrap_or(0));
        }
        for (out_idx, &ci) in f64_col_indices.iter().enumerate() {
            let v = record.get(ci).unwrap_or("0");
            f64_cols[out_idx].1.push(v.parse::<f64>().unwrap_or(0.0));
        }
    }

    Ok(IncomingData { id_col, i64_cols, f64_cols })
}

/// Read a pipe-delimited VTD crosswalk file and return a block → VTD geo_id map.
#[cfg(feature = "download")]
fn read_crosswalk_txt(path: &Path) -> Result<HashMap<GeoId, GeoId>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open crosswalk file: {}", path.display()))?;

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b'|')
        .from_reader(file);

    // The file has columns: BLOCKID (15-char block geo_id) and DISTRICT (VTD suffix)
    // VTD geo_id = first 5 chars of BLOCKID (county FIPS) + DISTRICT
    let blockid_col = "BLOCKID";
    let district_col = "DISTRICT";

    let headers: Vec<String> = rdr
        .headers()
        .context("failed to read crosswalk headers")?
        .iter()
        .map(|s| s.to_string())
        .collect();

    let blockid_idx = headers.iter().position(|h| h == blockid_col)
        .ok_or_else(|| anyhow!("crosswalk missing '{}' column", blockid_col))?;
    let district_idx = headers.iter().position(|h| h == district_col)
        .ok_or_else(|| anyhow!("crosswalk missing '{}' column", district_col))?;

    let mut map: HashMap<GeoId, GeoId> = HashMap::new();
    let mut record = csv::StringRecord::new();

    while rdr.read_record(&mut record).context("failed to read crosswalk record")? {
        let block_str  = record.get(blockid_idx).unwrap_or("").trim();
        let district_str = record.get(district_idx).unwrap_or("").trim();

        if block_str.is_empty() || district_str.is_empty() {
            continue;
        }

        let vtd_id = format!("{}{}", &block_str[..5], district_str);
        map.insert(
            GeoId::new(GeoType::Block, block_str),
            GeoId::new(GeoType::VTD, &vtd_id),
        );
    }

    Ok(map)
}
