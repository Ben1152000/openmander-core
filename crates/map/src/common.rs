use std::{fs::{File, create_dir_all}, io::{BufReader, BufWriter, Read, Write}, path::Path, sync::Arc};

use anyhow::{anyhow, bail, Context, Result};
use polars::{frame::DataFrame, io::SerReader, prelude::{CsvReadOptions, CsvReader, ParquetReader, ParquetWriter}};
use sha2::{Digest, Sha256};
use shapefile as shp;

/// Create the directory if it doesn’t exist; error if a non-directory exists there.
pub fn ensure_dir_exists(path: &Path) -> Result<()> {
    if path.exists() {
        if !path.is_dir() { bail!("Path exists but is not a directory: {}", path.display()); }
    } else {
        create_dir_all(path)
            .with_context(|| format!("Failed to create directory {}", path.display()))?;
    }
    Ok(())
}

/// Create multiple directories under a base path.
pub fn ensure_dirs(base: &Path, dirs: &[&str]) -> Result<()> {
    for &dir in dirs {
        ensure_dir_exists(&base.join(dir))?;
    }
    Ok(())
}

/// Error unless the directory already exists.
pub fn require_dir_exists(path: &Path) -> Result<()> {
    if !path.exists() { bail!("Directory does not exist: {}", path.display()); }
    if !path.is_dir() { bail!("Path exists but is not a directory: {}", path.display()); }
    Ok(())
}

/// Computes the SHA-256 hash of a file located at `root/rel_path`.
pub fn sha256_file(rel_path: &str, root: &Path) -> Result<(String, String)> {
    let full = root.join(rel_path);
    let mut file = File::open(&full)
        .with_context(|| format!("open for hash {}", full.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 1 << 16];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let hex = hex::encode(hasher.finalize());
    Ok((rel_path.to_string(), hex))
}

/// Reads a CSV file from `path` into a Polars DataFrame.
pub fn read_from_csv(path: &Path) -> Result<DataFrame> {
    let file = File::open(&path)?;
    let df = CsvReader::new(file)
        .finish()?;
    Ok(df)
}

/// Reads a pipe-delimited `.txt` file with a header row into a Polars DataFrame.
pub fn read_from_pipe_delimited_txt(path: &Path) -> Result<DataFrame> {
    let file = File::open(path)?;
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .map_parse_options(|po| po
            .with_separator(b'|'))
            .with_infer_schema_length(Some(0))
        .into_reader_with_file_handle(file).finish()?;
    Ok(df)
}

/// Writes a Polars DataFrame to a Parquet file at `path`.
pub fn write_to_parquet(path: &Path, df: &DataFrame) -> Result<()> {
    let file = File::create(&path)?;
    let writer: BufWriter<File> = BufWriter::new(file);
    ParquetWriter::new(writer).finish(&mut df.clone())?;
    Ok(())
}

/// Reads a Polars DataFrame from a Parquet file at `path`.
pub fn read_from_parquet(path: &Path) -> Result<DataFrame> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to read parquet file: {}", path.display()))?;
    Ok(ParquetReader::new(&mut file).finish()?)
}

/// Convert shapefile::Polygon to geo::MultiPolygon<f64>
pub fn shp_to_geo(p: &shp::Polygon) -> geo::MultiPolygon<f64> {
    /// Ensure first and last are the same for geo::LineString coords
    fn ensure_closed(coords: &mut Vec<geo::Coord<f64>>) {
        if !coords.is_empty() {
            if coords[0] != coords[coords.len() - 1] {
                coords.push(coords[0])
            }
        }
    }

    /// Get the signed area of a geo::Coord list (negative for hole)
    fn signed_area(pts: &[geo::Coord<f64>]) -> f64 {
        let mut a = 0.0;
        for w in pts.windows(2) {
            a += w[0].x * w[1].y - w[1].x * w[0].y;
        }
        a / 2.0
    }

    // 1) Convert each ring into a LineString (ensure closed)
    let mut ls_rings: Vec<(geo::LineString<f64>, bool /*is_exterior*/)> = Vec::with_capacity(p.rings().len());
    for ring in p.rings().iter() {
        let mut coords: Vec<geo::Coord<f64>> = ring.points().iter().map(|pt| geo::Coord { x: pt.x, y: pt.y }).collect();
        ensure_closed(&mut coords);
        let ls = geo::LineString(coords);
        // Prefer explicit API if your ring exposes it; otherwise infer by orientation (CW => exterior in Shapefile).
        let is_exterior = signed_area(&ls.0) < 0.0;
        ls_rings.push((ls, is_exterior));
    }

    // 2) Group: each exterior with its following holes (Shapefile stores rings in this order)
    let mut polys: Vec<geo::Polygon<f64>> = Vec::new();
    let mut current_exterior: Option<geo::LineString<f64>> = None;
    let mut current_holes: Vec<geo::LineString<f64>> = Vec::new();

    for (ls, is_exterior) in ls_rings {
        if is_exterior {
            // flush previous polygon
            if let Some(ext) = current_exterior.take() {
                polys.push(geo::Polygon::new(ext, current_holes));
                current_holes = Vec::new();
            }
            current_exterior = Some(ls);
        } else {
            current_holes.push(ls);
        }
    }
    if let Some(ext) = current_exterior {
        polys.push(geo::Polygon::new(ext, current_holes));
    }

    geo::MultiPolygon(polys)
}

/// Convert geo::MultiPolygon<f64> to shapefile::Polygon
pub fn _geo_to_shp(mp: &geo::MultiPolygon<f64>) -> shp::Polygon {
    /// Create a shapefile::Point
    #[inline] fn shp_point(x: f64, y: f64) -> shp::Point { shp::Point { x, y } }

    /// Close a ring of shapefile::Point
    fn ensure_closed(pts: &mut Vec<shp::Point>) {
        if !pts.is_empty() {
            if pts[0].x != pts[pts.len() - 1].x || pts[0].y != pts[pts.len() - 1].y {
                pts.push(pts[0]);
            }
        }
    }

    /// Get the signed area of a shapefile::Point list (negative for hole)
    fn signed_area(pts: &[shp::Point]) -> f64 {
        let mut a = 0.0;
        for w in pts.windows(2) {
            a += w[0].x * w[1].y - w[1].x * w[0].y;
        }
        a / 2.0
    }

    // Build a flat list of rings in Shapefile ordering:
    // [ext CW, hole CCW, hole CCW, ..., next ext CW, ...]
    let mut rings: Vec<shp::PolygonRing<shp::Point>> = Vec::new();

    for poly in &mp.0 {
        // Exterior: force CW (Shapefile convention), ensure closed
        let mut ext_pts = poly.exterior().points().map(|c| shp_point(c.x(), c.y())).collect::<Vec<_>>();
        ensure_closed(&mut ext_pts);
        if signed_area(&ext_pts) > 0.0 {
            ext_pts.reverse(); // make CW
        }
        rings.push(shp::PolygonRing::Outer(ext_pts));

        // Holes: force CCW, ensure closed
        for hole in poly.interiors() {
            let mut hole_pts = hole.points().map(|c| shp_point(c.x(), c.y())).collect::<Vec<_>>();
            ensure_closed(&mut hole_pts);
            if signed_area(&hole_pts) < 0.0 {
                hole_pts.reverse(); // make CCW
            }
            rings.push(shp::PolygonRing::Inner(hole_pts));
        }
    }

    shp::Polygon::with_rings(rings)
}

/// Write geometries to a single-column GeoParquet file named `geometry`.
pub fn write_to_geoparquet(path: &Path, geoms: &Vec<geo::MultiPolygon<f64>>) -> Result<()> {
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
    builder.extend_from_geometry_iter(geoms.iter().map(|geom: &geo::MultiPolygon| Some(geom)))?;
    let polygons: MultiPolygonArray = builder.finish();

    // 2) Wrap in RecordBatch with a proper GeoArrow extension Field
    let schema = Arc::new(Schema::new(vec![field]));
    let columns: Vec<ArrayRef> = vec![polygons.to_array_ref()];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    // 3) Encode GeoParquet + write with Parquet writer props (ZSTD is a good default)
    let gp_opts = GeoParquetWriterOptions::default();
    let mut gp_encoder = GeoParquetRecordBatchEncoder::try_new(schema.as_ref(), &gp_opts)?;
    let writer_props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::try_new(4)?))
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
pub fn read_from_geoparquet(path: &Path) -> Result<Vec<geo::MultiPolygon<f64>>> {
    use geoarrow::array::MultiPolygonArray;
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
    use geo_traits::to_geo::ToGeoMultiPolygon;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)
        .with_context(|| format!("Failed to read geoparquet file: {}", path.display()))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Parse GeoParquet -> infer a GeoArrow schema. `true` = parse WKB to native arrays.
    let gp_meta = builder
        .geoparquet_metadata()
        .ok_or_else(|| anyhow!("Not a GeoParquet file (missing 'geo' metadata)"))??;
    let ga_schema = builder.geoarrow_schema(&gp_meta, /*parse_to_geoarrow=*/ true, Default::default())?;

    // Build the reader and wrap it so geometry columns are exposed as GeoArrow arrays
    let parquet_reader = builder.with_batch_size(64 * 1024).build()?;
    let mut geo_reader = GeoParquetRecordBatchReader::try_new(parquet_reader, ga_schema)?;

    let mut polys: Vec<geo::MultiPolygon<f64>> = Vec::new();
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

    Ok(polys)
}

/// Write adjacency list to a simple CSR binary file.
/// Layout: "CSR1" | n(u64) | nnz(u64) | indptr[u64; n+1] | indices[u32; nnz]
pub fn _write_to_adjacency_csr(path: &Path, adj_list: &Vec<Vec<u32>>) -> Result<()> {
    let n = adj_list.len();

    // Build indptr (prefix sums) and count nnz
    let mut indptr: Vec<u64> = Vec::with_capacity(n + 1);
    indptr.push(0);
    let mut nnz: u64 = 0;
    for row in adj_list {
        nnz += row.len() as u64;
        indptr.push(nnz);
    }

    let mut writer = BufWriter::new(File::create(path)?);

    // Header
    writer.write_all(b"CSR1")?;
    writer.write_all(&(n as u64).to_le_bytes())?;
    writer.write_all(&nnz.to_le_bytes())?;

    // indptr
    for &o in &indptr {
        writer.write_all(&o.to_le_bytes())?;
    }

    // indices (flattened)
    for row in adj_list {
        for &j in row {
            writer.write_all(&j.to_le_bytes())?;
        }
    }

    writer.flush()?;
    Ok(())
}

/// Read adjacency from a CSR binary file written by `write_adjacency_csr`.
pub fn _read_from_adjacency_csr(path: &Path) -> Result<Vec<Vec<u32>>> {
    let file = File::open(path)
        .with_context(|| format!("Failed to read csr file: {}", path.display()))?;
    let mut reader = BufReader::new(file);

    // Header
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if &magic != b"CSR1" {
        bail!("Invalid CSR magic: expected 'CSR1'");
    }

    let mut buf8 = [0u8; 8];
    reader.read_exact(&mut buf8)?;
    let n = u64::from_le_bytes(buf8) as usize;

    reader.read_exact(&mut buf8)?;
    let nnz_hdr = u64::from_le_bytes(buf8) as usize;

    // indptr
    let mut indptr = vec![0u64; n + 1];
    for i in 0..=n {
        reader.read_exact(&mut buf8)?;
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
        reader.read_exact(&mut b4)?;
        indices[i] = u32::from_le_bytes(b4);
    }

    Ok((0..n).map(|i| indices[indptr[i] as usize..indptr[i + 1] as usize].to_vec()).collect())
}

/// Write weighted adjacency list to a CSR binary file.
pub fn write_to_weighted_csr(path: &Path, adjacencies: &[Vec<u32>], weights: &[Vec<f64>]) -> Result<()> {
    let n = adjacencies.len();
    if weights.len() != n { bail!("weights len ({}) != adj_list len ({})", weights.len(), n); }

    // Validate row shapes and build prefix sums
    let mut indptr: Vec<u64> = Vec::with_capacity(n + 1);
    indptr.push(0);
    let mut nnz: u64 = 0;
    for (row_i, (nbrs, wts)) in adjacencies.iter().zip(weights).enumerate() {
        if nbrs.len() != wts.len() {
            bail!("row {}: neighbors len ({}) != weights len ({})", row_i, nbrs.len(), wts.len());
        }
        nnz += nbrs.len() as u64;
        indptr.push(nnz);
    }

    let mut writer = BufWriter::new(File::create(path)?);

    // Header
    writer.write_all(b"CSRW")?;
    writer.write_all(&(n as u64).to_le_bytes())?;
    writer.write_all(&nnz.to_le_bytes())?;

    // indptr
    for &o in &indptr {
        writer.write_all(&o.to_le_bytes())?;
    }

    // indices (flattened)
    for row in adjacencies {
        for &j in row {
            writer.write_all(&j.to_le_bytes())?;
        }
    }

    // data (flattened, f64)
    for row_w in weights {
        for &val in row_w {
            writer.write_all(&val.to_le_bytes())?;
        }
    }

    writer.flush()?;
    Ok(())
}

/// Read weighted adjacency from a CSR binary file written by `write_weighted_csr`.
pub fn read_from_weighted_csr(path: &Path) -> Result<(Vec<Vec<u32>>, Vec<Vec<f64>>)> {
    let file = File::open(path)
        .with_context(|| format!("Failed to read csr file: {}", path.display()))?;
    let mut reader = BufReader::new(file);

    // Header
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if &magic != b"CSRW" { bail!("Invalid CSR magic: expected 'CSRW'"); }

    let mut b8 = [0u8; 8];
    reader.read_exact(&mut b8)?;
    let n = u64::from_le_bytes(b8) as usize;

    reader.read_exact(&mut b8)?;
    let nnz = u64::from_le_bytes(b8) as usize;

    // indptr
    let mut indptr = vec![0u64; n + 1];
    for o in &mut indptr {
        reader.read_exact(&mut b8)?;
        *o = u64::from_le_bytes(b8);
    }
    if indptr[n] as usize != nnz { bail!("CSR nnz mismatch: header {} vs indptr {}", nnz, indptr[n]); }

    // indices
    let mut indices = vec![0u32; nnz];
    for x in &mut indices {
        let mut b4 = [0u8; 4];
        reader.read_exact(&mut b4)?;
        *x = u32::from_le_bytes(b4);
    }

    // data
    let mut data = vec![0f64; nnz];
    for x in &mut data {
        let mut b8 = [0u8; 8];
        reader.read_exact(&mut b8)?;
        *x = f64::from_le_bytes(b8);
    }

    // Rebuild per-row vectors (adjacency + weights)
    let mut adj: Vec<Vec<u32>> = Vec::with_capacity(n);
    let mut wts: Vec<Vec<f64>> = Vec::with_capacity(n);
    for i in 0..n {
        let s = indptr[i] as usize;
        let e = indptr[i + 1] as usize;
        adj.push(indices[s..e].to_vec());
        wts.push(data[s..e].to_vec());
    }

    Ok((adj, wts))
}
