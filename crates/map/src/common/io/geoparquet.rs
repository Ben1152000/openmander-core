use std::{fs::File, path::Path, sync::Arc};

use anyhow::{anyhow, Context, Result};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema;
use geoarrow::array::MultiPolygonArray;
use geoarrow_array::{builder::MultiPolygonBuilder, GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{Dimension, MultiPolygonType};
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
use geo_traits::to_geo::ToGeoMultiPolygon;
use parquet::{arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder}, basic::ZstdLevel};
use parquet::file::properties::WriterProperties;

/// Write geometries to a single-column GeoParquet file named `geometry`.
pub fn write_to_geoparquet(path: &Path, geoms: &Vec<geo::MultiPolygon<f64>>) -> Result<()> {
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
