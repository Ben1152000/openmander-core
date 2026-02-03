//! GeoParquet reading operations.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use geo::Polygon;
use geo_traits::to_geo::ToGeoMultiPolygon;
use geoarrow_array::{array::MultiPolygonArray, GeoArrowArrayAccessor};
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::reader::ChunkReader};

/// Read GeoParquet from bytes.
pub(crate) fn read_geoparquet_bytes(bytes: &[u8]) -> Result<Vec<geo::MultiPolygon<f64>>> {
    let bytes = Bytes::copy_from_slice(bytes);
    read_geoparquet(bytes)
}

/// Internal helper: read from any ChunkReader-compatible source.
fn read_geoparquet<R: ChunkReader + 'static>(reader: R) -> Result<Vec<geo::MultiPolygon<f64>>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;

    let gp_meta = builder
        .geoparquet_metadata()
        .ok_or_else(|| anyhow!("[io::geoparquet::read] Not a GeoParquet file (missing 'geo' metadata)"))??;

    let ga_schema =
        builder.geoarrow_schema(&gp_meta, /*parse_to_geoarrow=*/ true, Default::default())?;

    let parquet_reader = builder.with_batch_size(64 * 1024).build()?;
    let mut geo_reader = GeoParquetRecordBatchReader::try_new(parquet_reader, ga_schema)?;

    let mut polys = Vec::new();
    while let Some(batch) = geo_reader.next() {
        let batch = batch?;

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

/// Read hulls (Polygons) from GeoParquet bytes.
pub(crate) fn read_hulls_from_geoparquet_bytes(bytes: &[u8]) -> Result<Vec<Polygon<f64>>> {
    let multipolygons = read_geoparquet_bytes(bytes)?;
    let hulls: Vec<Polygon<f64>> = multipolygons.into_iter()
        .filter_map(|mp| mp.0.into_iter().next())
        .collect();
    Ok(hulls)
}
