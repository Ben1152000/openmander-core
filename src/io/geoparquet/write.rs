//! GeoParquet writing operations.

use std::{io::Write, sync::Arc};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use geo::Polygon;
use geoarrow_array::{builder::MultiPolygonBuilder, GeoArrowArray};
use geoarrow_schema::{Dimension, MultiPolygonType};
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
    format::FileMetaData,
};

/// Write geometries to GeoParquet bytes.
pub(crate) fn write_geoparquet_bytes(geoms: &[geo::MultiPolygon<f64>]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let _meta = write_geoparquet(&mut out, geoms)?;
    Ok(out)
}

/// Internal helper: write to any ArrowWriter-compatible sink.
fn write_geoparquet<W: Write + Send>(writer: W, geoms: &[geo::MultiPolygon<f64>]) -> Result<FileMetaData> {
    if geoms.is_empty() {
        return Err(anyhow::anyhow!("[io::geoparquet::write] Cannot write empty geometry array to GeoParquet"));
    }
    
    // Build a GeoArrow MultiPolygon array from geo-types
    let geom_type = MultiPolygonType::new(Dimension::XY, Default::default());
    let field = geom_type.to_field("geometry", false);

    let mut builder = MultiPolygonBuilder::new(geom_type);
    builder.extend_from_geometry_iter(geoms.iter().map(|polygon| Some(polygon)))
        .context("[io::geoparquet::write] Failed to build MultiPolygon array")?;
    let polygons = builder.finish();

    // Wrap in RecordBatch
    let schema = Arc::new(Schema::new(vec![field]));
    let columns = vec![polygons.to_array_ref()];
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .context("[io::geoparquet::write] Failed to create RecordBatch")?;

    // Encode GeoParquet + Parquet writer props
    let gp_opts = GeoParquetWriterOptions::default();
    let mut gp_encoder = GeoParquetRecordBatchEncoder::try_new(schema.as_ref(), &gp_opts)
        .context("[io::geoparquet::write] Failed to create encoder")?;

    let writer_props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(4)?))
        .build();

    let mut writer = ArrowWriter::try_new(writer, gp_encoder.target_schema(), Some(writer_props))
        .context("[io::geoparquet::write] Failed to create ArrowWriter")?;

    let encoded = gp_encoder.encode_record_batch(&batch)
        .context("[io::geoparquet::write] Failed to encode batch")?;
    writer.write(&encoded)
        .context("[io::geoparquet::write] Failed to write batch")?;

    writer.append_key_value_metadata(gp_encoder.into_keyvalue()?);

    let meta = writer.finish()
        .context("[io::geoparquet::write] Failed to finish writing")?;
    Ok(meta)
}

/// Write hulls (Polygons) to GeoParquet bytes.
pub(crate) fn write_hulls_to_geoparquet_bytes(hulls: &[Polygon<f64>]) -> Result<Vec<u8>> {
    let multipolygons: Vec<geo::MultiPolygon<f64>> = hulls.iter()
        .map(|poly| geo::MultiPolygon(vec![poly.clone()]))
        .collect();
    write_geoparquet_bytes(&multipolygons)
}
