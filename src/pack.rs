use std::{collections::BTreeMap, fs::File, io::{BufReader, BufWriter, Read, Write}, path::Path, sync::Arc};

use anyhow::{anyhow, bail, Ok, Result};
use geo::{Geometry, MultiPolygon};
use geozero::{FeatureProcessor, geo_types::process_geom, ToGeo};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use flatgeobuf::{FallibleStreamingIterator, FgbCrs, FgbReader, FgbWriter, FgbWriterOptions, GeometryType};

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

    /// Write geometries to a single-column GeoParquet file named `geometry`.
    pub fn write_geoparquet(&self, path: &Path) -> Result<()> {
        use arrow_array::{ArrayRef, RecordBatch};
        use arrow_schema::Schema;
        use geoarrow::array::MultiPolygonArray;
        use geoarrow_array::{builder::MultiPolygonBuilder, GeoArrowArray};
        use geoarrow_schema::{Dimension, MultiPolygonType};
        use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
        use parquet::{arrow::ArrowWriter, basic::ZstdLevel};
        use parquet::file::properties::WriterProperties;

        // 1) Build a GeoArrow MultiPolygon array from geo-types
        let geom_type = MultiPolygonType::new(Dimension::XY, Default::default());
        let field = geom_type.to_field("geometry", /*nullable=*/ false);

        let mut builder = MultiPolygonBuilder::new(geom_type);
        builder.extend_from_geometry_iter(self.geoms.iter().map(|geom: &MultiPolygon| Some(geom)))?;
        let polygons: MultiPolygonArray = builder.finish();

        // 2) Wrap in RecordBatch with a proper GeoArrow extension Field
        let schema = Arc::new(Schema::new(vec![field]));
        let columns: Vec<ArrayRef> = vec![polygons.to_array_ref()];
        let batch = RecordBatch::try_new(schema.clone(), columns)?;

        // 3) Encode GeoParquet + write with Parquet writer props (ZSTD is a good default)
        let gp_opts = GeoParquetWriterOptions::default();
        let mut gp_encoder = GeoParquetRecordBatchEncoder::try_new(schema.as_ref(), &gp_opts)?;
        let writer_props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::try_new(9)?))
            .build();

        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, gp_encoder.target_schema(), Some(writer_props))?;

        let encoded = gp_encoder.encode_record_batch(&batch)?;
        writer.write(&encoded)?;

        // Attach GeoParquet metadata (column encodings, bbox, CRS, etc.)
        writer.append_key_value_metadata(gp_encoder.into_keyvalue()?);

        writer.finish()?;
        Ok(())
    }

    /// Read a GeoParquet file (single `geometry` column) back into a PlanarPartition.
    pub fn read_geoparquet(path: &Path) -> Result<Self> {
        use geoarrow::array::MultiPolygonArray;
        use geoarrow_array::GeoArrowArrayAccessor;
        use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
        use geo_traits::to_geo::ToGeoMultiPolygon;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Parse GeoParquet -> infer a GeoArrow schema. `true` = parse WKB to native arrays.
        let gp_meta = builder
            .geoparquet_metadata()
            .ok_or_else(|| anyhow!("Not a GeoParquet file (missing 'geo' metadata)"))??;
        let ga_schema = builder.geoarrow_schema(&gp_meta, /*parse_to_geoarrow=*/ true, Default::default())?;

        // Build the reader and wrap it so geometry columns are exposed as GeoArrow arrays
        let parquet_reader = builder.with_batch_size(64 * 1024).build()?;
        let mut geo_reader = GeoParquetRecordBatchReader::try_new(parquet_reader, ga_schema)?;

        let mut polys: Vec<MultiPolygon<f64>> = Vec::new();
        while let Some(batch) = geo_reader.next() {
            let batch = batch?;
            // Expect a single geometry column named "geometry"
            let geom_idx = 0; // or batch.schema().index_of("geometry")?
            let arr = batch.column(geom_idx).as_ref();
            let schema = batch.schema();
            let field = schema.field(geom_idx);

            // Convert the Arrow column + Field to a typed GeoArrow array; convert each scalar to geo-types
            polys.extend(MultiPolygonArray::try_from((arr, field))?.iter()
                .filter_map(|opt| opt.and_then(Result::ok))
                .map(|scalar| scalar.to_multi_polygon()));
        }

        Ok(PlanarPartition::new(polys))
    }

    /// Write adjacency list to a simple CSR binary file.
    /// Layout: "CSR1" | n(u64) | nnz(u64) | indptr[u64; n+1] | indices[u32; nnz]
    pub fn write_adjacency_csr(&self, path: &Path) -> Result<()> {
        let n = self.adj_list.len();

        // Build indptr (prefix sums) and count nnz
        let mut indptr: Vec<u64> = Vec::with_capacity(n + 1);
        indptr.push(0);
        let mut nnz: u64 = 0;
        for row in &self.adj_list {
            nnz += row.len() as u64;
            indptr.push(nnz);
        }

        let mut w = BufWriter::new(File::create(path)?);

        // Header
        w.write_all(b"CSR1")?;
        w.write_all(&(n as u64).to_le_bytes())?;
        w.write_all(&nnz.to_le_bytes())?;

        // indptr
        for &o in &indptr {
            w.write_all(&o.to_le_bytes())?;
        }

        // indices (flattened)
        for row in &self.adj_list {
            for &j in row {
                w.write_all(&j.to_le_bytes())?;
            }
        }

        w.flush()?;
        Ok(())
    }

    /// Read adjacency from a CSR binary file written by `write_adjacency_csr`.
    pub fn read_adjacency_csr(&mut self, path: &Path) -> Result<()> {
        let mut r = BufReader::new(File::open(path)?);

        // Header
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if &magic != b"CSR1" {
            bail!("Invalid CSR magic: expected 'CSR1'");
        }

        let mut buf8 = [0u8; 8];
        r.read_exact(&mut buf8)?;
        let n = u64::from_le_bytes(buf8) as usize;

        r.read_exact(&mut buf8)?;
        let nnz_hdr = u64::from_le_bytes(buf8) as usize;

        // indptr
        let mut indptr = vec![0u64; n + 1];
        for i in 0..=n {
            r.read_exact(&mut buf8)?;
            indptr[i] = u64::from_le_bytes(buf8);
        }

        let nnz = indptr[n] as usize;
        if nnz != nnz_hdr {
            bail!("CSR nnz mismatch: header {} vs indptr {}", nnz_hdr, nnz);
        }

        // indices
        let mut indices = vec![0u32; nnz];
        for i in 0..nnz {
            let mut b4 = [0u8; 4];
            r.read_exact(&mut b4)?;
            indices[i] = u32::from_le_bytes(b4);
        }

        // Rebuild adj_list
        self.adj_list.clear();
        self.adj_list.reserve(n);
        for i in 0..n {
            let s = indptr[i] as usize;
            let e = indptr[i + 1] as usize;
            self.adj_list.push(indices[s..e].to_vec());
        }

        Ok(())
    }
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

    fn write_pack(&self, path: &Path,
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
            geom.write_geoparquet(&path.join(geom_path))?;
            let (k, h) = sha256_file(geom_path, path)?;
            hashes.insert(k, FileHash { sha256: h });

            // adjacencies (CSR)
            if self.ty != GeoType::State {
                geom.write_adjacency_csr(&path.join(adj_path))?;
                let (k, h) = sha256_file(&adj_path, path)?;
                hashes.insert(k, FileHash { sha256: h });
            }
        }

        Ok(())
    }

    fn read_pack(&mut self, path: &Path) -> Result<()> {
        let name = self.ty.to_str();
        let entity_path = path.join(format!("entities/{}.parquet", name));
        let elec_path = path.join(format!("elections/{name}.parquet"));
        let demo_path = path.join(format!("demographics/{name}.parquet"));
        let geom_path = path.join(format!("geometries/{name}.fgb"));

        if entity_path.exists() {
            let df = read_from_parquet(&entity_path)?;
            let (entities, parents) = df_to_entities(&df, self.ty)?;
            self.entities = entities;
            self.parents = parents;

            // rebuild index
            self.index.clear();
            for (i, e) in self.entities.iter().enumerate() {
                self.index.insert(e.geo_id.clone(), i as u32);
            }
        }
        if elec_path.exists() { self.elec_data = Some(read_from_parquet(&elec_path)?); }
        if demo_path.exists() { self.demo_data = Some(read_from_parquet(&demo_path)?); }
        if geom_path.exists() { self.geoms = Some(crate::geometry::PlanarPartition::read_fgb(&geom_path)?); }

        // relations (CSR) — add when available
        // read_layer_csr_into(layer, path)?;

        Ok(())
    }
}

impl MapData {
    pub fn write_pack(&self, path: &Path) -> Result<()> {
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
            self.get_layer(ty).write_pack(path, &mut counts, &mut file_hashes)?;
        }

        // Manifest
        let meta_path = path.join("meta/manifest.json");
        let manifest = Manifest::new(path, counts, file_hashes);
        let mut f = File::create(&meta_path)?;
        serde_json::to_writer_pretty(&mut f, &manifest)?;

        Ok(())
    }

    pub fn read_pack(path: &Path) -> Result<Self> {
        let mut map_data = Self::default();
    
        // Load per-level content
        for ty in GeoType::order() {
            map_data.get_layer_mut(ty).read_pack(path)?;
        }
    
        Ok(map_data)
    }
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
