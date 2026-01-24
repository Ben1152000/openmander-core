use std::{fs::File, io::Write, path::Path, sync::Arc};

use anyhow::{anyhow, Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use bytes::Bytes;
use geo_traits::to_geo::ToGeoMultiPolygon;
use geoarrow_array::{
    array::MultiPolygonArray,
    builder::MultiPolygonBuilder,
    GeoArrowArray,
    GeoArrowArrayAccessor,
};
use geoarrow_schema::{Dimension, MultiPolygonType};
use geoparquet::{
    reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader},
    writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions},
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::{properties::WriterProperties, reader::ChunkReader},
    format::FileMetaData,
};

/// Write geometries to a single-column GeoParquet file named `geometry`.
pub(crate) fn write_to_geoparquet_file(path: &Path, geoms: &[geo::MultiPolygon<f64>]) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("Failed to create geoparquet file: {}", path.display()))?;

    // finish() returns metadata in your parquet version; ignore it.
    let _meta = write_to_geoparquet(file, geoms)?;
    Ok(())
}

/// Write geometries to GeoParquet bytes.
pub(crate) fn write_to_geoparquet_bytes(geoms: &[geo::MultiPolygon<f64>]) -> Result<Vec<u8>> {
    let mut out = Vec::new();

    // Write into `out` directly. `&mut Vec<u8>` implements Write, and we keep `out` in scope.
    let _meta = write_to_geoparquet(&mut out, geoms)?;
    Ok(out)
}

/// Read a GeoParquet file (single `geometry` column) back into MultiPolygons.
pub(crate) fn read_from_geoparquet_file(path: &Path) -> Result<Vec<geo::MultiPolygon<f64>>> {
    let file = File::open(path)
        .with_context(|| format!("Failed to read geoparquet file: {}", path.display()))?;
    read_from_geoparquet(file)
}

/// Read GeoParquet from bytes.
pub(crate) fn read_from_geoparquet_bytes(bytes: &[u8]) -> Result<Vec<geo::MultiPolygon<f64>>> {
    let bytes = Bytes::copy_from_slice(bytes);
    read_from_geoparquet(bytes)
}

/// Internal helper: write to any ArrowWriter-compatible sink.
fn write_to_geoparquet<W: Write + Send>(writer: W, geoms: &[geo::MultiPolygon<f64>]) -> Result<FileMetaData> {
    // 1) Build a GeoArrow MultiPolygon array from geo-types
    let geom_type = MultiPolygonType::new(Dimension::XY, Default::default());
    let field = geom_type.to_field("geometry", false);

    let mut builder = MultiPolygonBuilder::new(geom_type);
    builder.extend_from_geometry_iter(geoms.iter().map(|polygon| Some(polygon)))?;
    let polygons = builder.finish();

    // 2) Wrap in RecordBatch
    let schema = Arc::new(Schema::new(vec![field]));
    let columns = vec![polygons.to_array_ref()];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    // 3) Encode GeoParquet + Parquet writer props
    let gp_opts = GeoParquetWriterOptions::default();
    let mut gp_encoder = GeoParquetRecordBatchEncoder::try_new(schema.as_ref(), &gp_opts)?;

    let writer_props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(4)?))
        .build();

    let mut writer = ArrowWriter::try_new(writer, gp_encoder.target_schema(), Some(writer_props))?;

    let encoded = gp_encoder.encode_record_batch(&batch)?;
    writer.write(&encoded)?;

    writer.append_key_value_metadata(gp_encoder.into_keyvalue()?);

    // In your parquet version this returns FileMetaData (not the writer).
    let meta = writer.finish()?;
    Ok(meta)
}

/// Internal helper: read from any ChunkReader-compatible source.
fn read_from_geoparquet<R: ChunkReader + 'static>(reader: R) -> Result<Vec<geo::MultiPolygon<f64>>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;

    let gp_meta = builder
        .geoparquet_metadata()
        .ok_or_else(|| anyhow!("Not a GeoParquet file (missing 'geo' metadata)"))??;

    let ga_schema =
        builder.geoarrow_schema(&gp_meta, /*parse_to_geoarrow=*/ true, Default::default())?;

    let parquet_reader = builder.with_batch_size(64 * 1024).build()?;
    let mut geo_reader = GeoParquetRecordBatchReader::try_new(parquet_reader, ga_schema)?;

    let mut polys = Vec::new();
    while let Some(batch) = geo_reader.next() {
        let batch = batch?;

        // Avoid borrowing from a temporary Arc<Schema>.
        let schema = batch.schema();
        let geom_idx = 0;
        let arr = batch.column(geom_idx).as_ref();
        let field = schema.field(geom_idx);

        polys.extend(
            MultiPolygonArray::try_from((arr, field))?
                .iter()
                .filter_map(|opt| opt.and_then(Result::ok))
                .map(|scalar| scalar.to_multi_polygon()),
        );
    }

    Ok(polys)
}
