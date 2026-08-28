use crate::Result;
use crate::cli::VersionArgs;
use crate::dataset;

pub async fn run(input: &str, filter: Option<&str>, version: &VersionArgs) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;
    // When a filter is set Lance uses its native filtered count (for
    // Lance, pushed into scalar indices when available) rather than scanning.
    let n = ds.count_rows(filter).await?;
    println!("{n}");
    Ok(())
}
