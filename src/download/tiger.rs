use anyhow::{Context, Result};
use crate::io::{download_big_file};
use std::path::PathBuf;

/// Return (two-digit FIPS code string, UPPER_SNAKE name) for a USPS postal code.
/// Includes 50 states + DC + PR. Add more territories if you need them.
fn state_info(code: &str) -> Option<(&'static str, &'static str)> {
    match code {
        "AL" => Some(("01", "ALABAMA")),
        "AK" => Some(("02", "ALASKA")),
        "AZ" => Some(("04", "ARIZONA")),
        "AR" => Some(("05", "ARKANSAS")),
        "CA" => Some(("06", "CALIFORNIA")),
        "CO" => Some(("08", "COLORADO")),
        "CT" => Some(("09", "CONNECTICUT")),
        "DE" => Some(("10", "DELAWARE")),
        "FL" => Some(("12", "FLORIDA")),
        "GA" => Some(("13", "GEORGIA")),
        "HI" => Some(("15", "HAWAII")),
        "ID" => Some(("16", "IDAHO")),
        "IL" => Some(("17", "ILLINOIS")),
        "IN" => Some(("18", "INDIANA")),
        "IA" => Some(("19", "IOWA")),
        "KS" => Some(("20", "KANSAS")),
        "KY" => Some(("21", "KENTUCKY")),
        "LA" => Some(("22", "LOUISIANA")),
        "ME" => Some(("23", "MAINE")),
        "MD" => Some(("24", "MARYLAND")),
        "MA" => Some(("25", "MASSACHUSETTS")),
        "MI" => Some(("26", "MICHIGAN")),
        "MN" => Some(("27", "MINNESOTA")),
        "MS" => Some(("28", "MISSISSIPPI")),
        "MO" => Some(("29", "MISSOURI")),
        "MT" => Some(("30", "MONTANA")),
        "NE" => Some(("31", "NEBRASKA")),
        "NV" => Some(("32", "NEVADA")),
        "NH" => Some(("33", "NEW_HAMPSHIRE")),
        "NJ" => Some(("34", "NEW_JERSEY")),
        "NM" => Some(("35", "NEW_MEXICO")),
        "NY" => Some(("36", "NEW_YORK")),
        "NC" => Some(("37", "NORTH_CAROLINA")),
        "ND" => Some(("38", "NORTH_DAKOTA")),
        "OH" => Some(("39", "OHIO")),
        "OK" => Some(("40", "OKLAHOMA")),
        "OR" => Some(("41", "OREGON")),
        "PA" => Some(("42", "PENNSYLVANIA")),
        "RI" => Some(("44", "RHODE_ISLAND")),
        "SC" => Some(("45", "SOUTH_CAROLINA")),
        "SD" => Some(("46", "SOUTH_DAKOTA")),
        "TN" => Some(("47", "TENNESSEE")),
        "TX" => Some(("48", "TEXAS")),
        "UT" => Some(("49", "UTAH")),
        "VT" => Some(("50", "VERMONT")),
        "VA" => Some(("51", "VIRGINIA")),
        "WA" => Some(("53", "WASHINGTON")),
        "WV" => Some(("54", "WEST_VIRGINIA")),
        "WI" => Some(("55", "WISCONSIN")),
        "WY" => Some(("56", "WYOMING")),
        "DC" => Some(("11", "DISTRICT_OF_COLUMBIA")),
        "PR" => Some(("72", "PUERTO_RICO")),
        _ => None,
    }
}

/// Download geometry data from US Census website
pub fn download_tiger_geometries(out_dir: &PathBuf, state: &String, verbose: u8) -> Result<()> {

    // Build the Census TIGER 2020 PL directory URL for a given postal code.
    // Example: "NE" -> "https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/31_NEBRASKA/31/"
    let code = state.to_ascii_uppercase();

    let (fips, name) = state_info(&code)
        .with_context(|| format!("Unknown state/territory postal code: {code}"))?;

    let base = format!("https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{}_{}/{}/", fips, name, fips);

    // Filenames we need for TIGER 2020 (state/county/tract/bg/vtd/block)
    let files = [
        ("state",  format!("tl_2020_{fips}_state20.zip")),
        ("county", format!("tl_2020_{fips}_county20.zip")),
        ("tract",  format!("tl_2020_{fips}_tract20.zip")),
        ("group",  format!("tl_2020_{fips}_bg20.zip")),
        ("vtd",    format!("tl_2020_{fips}_vtd20.zip")),
        ("block",  format!("tl_2020_{fips}_tabblock20.zip")),
    ];

    for (label, name) in files {
        let file_url = format!("{base}{name}");
        let out_path = out_dir.join(&name);

        if verbose > 0 { eprintln!("[download:{label}] {file_url} -> {}", out_path.display()); }

        download_big_file(file_url, &out_path, true)?;
    }

    Ok(())
}
