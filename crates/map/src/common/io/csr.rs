use std::{fs::File, io::{BufReader, BufWriter, Read, Write}, path::Path};

use anyhow::{bail, Context, Result};

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
