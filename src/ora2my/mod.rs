mod export_ddl;
mod export_data;
mod initialize_db;
extern crate yaml_rust;
extern crate serde_yaml;
use crate::args::*;
use export_data::*;
use export_ddl::*;


pub async fn export_oracle() {
    match ARGS.content.as_str() {
        "all" => {
            // export_schema_ddl(&conn, &args.schema, &args.table_name_pattern, ddl_file.as_str()).await;
            export_schema_ddl().await;
            export_schema_data().await;
        },
        "metadata" => {
            export_schema_ddl().await;
        },
        "data" => {
            export_schema_data().await;
        }
        _ => {
            panic!("Wrong value for argument content: {}", ARGS.content)
        }
    }
}
