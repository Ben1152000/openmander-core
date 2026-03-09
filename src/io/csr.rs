//! Compressed Sparse Row (CSR) format for adjacency graphs.

use std::io::{Cursor, Read, Write};

use anyhow::{Context, Result, ensure};

/// Write weighted adjacency list to CSR binary bytes.
pub(crate) fn write_weighted_csr_bytes(adjacencies: &Vec<Vec<u32>>, weights: &Vec<Vec<f64>>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_weighted_csr(&mut out, adjacencies, weights)?;
    Ok(out)
}

/// Read weighted adjacency list from CSR binary bytes.
pub(crate) fn read_weighted_csr_bytes(bytes: &[u8]) -> Result<(Vec<Vec<u32>>, Vec<Vec<f64>>)> {
    let mut reader = Cursor::new(bytes);
    read_weighted_csr(&mut reader)
}

fn write_weighted_csr<W: Write>(writer: &mut W, adjacencies: &[Vec<u32>], weights: &[Vec<f64>]) -> Result<()> {
    ensure!(
        weights.len() == adjacencies.len(),
        "[io::csr] weights len ({}) != adj_list len ({})",
        weights.len(),
        adjacencies.len()
    );

    // Validate row shapes and build prefix sums
    let mut indptr: Vec<u64> = Vec::with_capacity(adjacencies.len() + 1);
    indptr.push(0);
    let mut nnz: u64 = 0;
    for (row_i, (nbrs, wts)) in adjacencies.iter().zip(weights).enumerate() {
        ensure!(nbrs.len() == wts.len(), "[io::csr] row {}: neighbors len ({}) != weights len ({})", row_i, nbrs.len(), wts.len());
        nnz += nbrs.len() as u64;
        indptr.push(nnz);
    }

    // Header
    writer.write_all(b"CSRW")
        .context("[io::csr] Failed to write magic bytes")?;
    writer.write_all(&(adjacencies.len() as u64).to_le_bytes())
        .context("[io::csr] Failed to write row count")?;
    writer.write_all(&nnz.to_le_bytes())
        .context("[io::csr] Failed to write nnz")?;

    // indptr
    for &o in &indptr {
        writer.write_all(&o.to_le_bytes())
            .context("[io::csr] Failed to write indptr")?;
    }

    // indices (flattened)
    for row in adjacencies {
        for &j in row {
            writer.write_all(&j.to_le_bytes())
                .context("[io::csr] Failed to write indices")?;
        }
    }

    // data (flattened, f64)
    for row_w in weights {
        for &val in row_w {
            writer.write_all(&val.to_le_bytes())
                .context("[io::csr] Failed to write weights")?;
        }
    }

    Ok(())
}

fn read_weighted_csr<R: Read>(reader: &mut R) -> Result<(Vec<Vec<u32>>, Vec<Vec<f64>>)> {
    // Header
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)
        .context("[io::csr] Failed to read magic bytes")?;
    ensure!(&magic == b"CSRW", "[io::csr] Invalid CSR magic: expected 'CSRW'");

    let mut b8 = [0u8; 8];
    reader.read_exact(&mut b8)
        .context("[io::csr] Failed to read row count")?;
    let n = u64::from_le_bytes(b8) as usize;

    reader.read_exact(&mut b8)
        .context("[io::csr] Failed to read nnz")?;
    let nnz = u64::from_le_bytes(b8) as usize;

    // indptr
    let mut indptr = vec![0u64; n + 1];
    for o in &mut indptr {
        reader.read_exact(&mut b8)
            .context("[io::csr] Failed to read indptr")?;
        *o = u64::from_le_bytes(b8);
    }
    ensure!(indptr[n] as usize == nnz, "[io::csr] nnz mismatch: header {} vs indptr {}", nnz, indptr[n]);

    // indices
    let mut indices = vec![0u32; nnz];
    for x in &mut indices {
        let mut b4 = [0u8; 4];
        reader.read_exact(&mut b4)
            .context("[io::csr] Failed to read indices")?;
        *x = u32::from_le_bytes(b4);
    }

    // data
    let mut data = vec![0f64; nnz];
    for x in &mut data {
        let mut b8 = [0u8; 8];
        reader.read_exact(&mut b8)
            .context("[io::csr] Failed to read weights")?;
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
