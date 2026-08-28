use crate::Result;
use crate::cli::{SchemaType, VersionArgs};
use crate::commands::common::project_arrow_schema;
use crate::dataset;
use crate::projection;

pub async fn run(
    input: &str,
    ty: SchemaType,
    columns: Option<&[String]>,
    exclude: Option<&[String]>,
    version: &VersionArgs,
) -> Result<()> {
    let ds = dataset::open(input, Some(version)).await?;
    let arrow_schema = ds.arrow_schema();
    let projection = projection::resolve(&arrow_schema, columns, exclude)?;

    match ty {
        SchemaType::Arrow => {
            let projected = project_arrow_schema(arrow_schema.as_ref(), projection.as_deref());
            println!("{:#?}", projected.as_ref());
        }
        SchemaType::Physical => {
            let debug = ds.physical_schema_debug(projection.as_deref())?;
            println!("{debug}");
        }
    }
    Ok(())
}
