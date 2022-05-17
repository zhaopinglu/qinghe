extern crate yaml_rust;
extern crate serde_yaml;
use log::{info};
use oracle::{Connection, };
use oracle::sql_type::{ToSql, };
use tokio::io::AsyncWriteExt;
use crate::common::*;
use crate::ora2my::initialize_db::*;
use crate::args::*;

pub async fn precheck_ddl(conn: &Connection, bv_schema: &[(&str, &dyn ToSql)]) {
    // todo: process unsupported type/value.
    let _ = select_sql(&conn, &DDL_SQLS["check_unsupported_type"], bv_schema).await;
    let _ = select_sql(&conn, &DDL_SQLS["check_unsupported_default_val"], bv_schema).await;
}

pub async fn prepare_ddl(conn: &Connection, bv_schema: &[(&str, &dyn ToSql)]) {
    info!("Preparing ddl ...");
    insert_tmp_tab(&conn, &DDL_SQLS["insert_tmp_cons"], bv_schema).await;
    let _ = exec_sql(&conn, &DDL_SQLS["delete_tmp_cons"], &[]).await;
    let _ = exec_sql(&conn, &DDL_SQLS["update_tmp_cons_search_condition"], &[]).await;

    insert_tmp_tab(&conn, &DDL_SQLS["insert_tmp_cols"], bv_schema).await;
    let _ = exec_sql(&conn, &DDL_SQLS["update_tmp_cols_default"], &[]).await;
    let _ = exec_sql(&conn, &DDL_SQLS["update_tmp_cols_vc_to_text"], &[]).await;
    let _ = exec_sql(&conn, &DDL_SQLS["update_tmp_cols_special_define"], &[]).await;
    let _ = exec_sql(&conn, &DDL_SQLS["update_tmp_cols_validate_timestamp"], &[]).await;

    insert_tmp_tab(&conn, &DDL_SQLS["insert_tmp_tab_ddl_1"], &[]).await;
    insert_tmp_tab(&conn, &DDL_SQLS["insert_tmp_tab_ddl_2"], &[]).await;
    insert_tmp_tab(&conn, &DDL_SQLS["insert_tmp_tab_ddl_3"], &[]).await;
}


pub async fn export_schema_ddl() {
    let conn = get_connection().await;
    set_connection_properties(&conn).await;


    let out_filename = get_out_filename("metadata", format!("{}_{}", ARGS.schema, "ddl").as_str());

    info!("Exporting schema ddl: schema: {}, output file: {}", ARGS.schema, out_filename);
    // println!("Exporting schema ddl: schema: {}, output file: {}", ARGS.schema, out_filename);

    let bv_schema : &[(&str, &dyn ToSql)]= &[("schema", &ARGS.schema), ("table_name_pattern", &ARGS.table_name_pattern)];

    initialize_ddl(&conn).await;
    precheck_ddl(&conn, bv_schema).await;
    prepare_ddl(&conn, bv_schema).await;


    // let mut f_ddl = File::create(out_filename).await.unwrap();
    let mut f_ddl = prepare_out_file(out_filename.as_str()).await;

    info!("Exporting schema ddl...");
    let notices = select_sql(&conn, &DDL_SQLS["select_tab_ddl_notice"], &[]).await;
    for notice in notices {
        info!("{}", notice);
        f_ddl.write_all(notice.as_bytes()).await.unwrap();
    }

    let res = select_sql(&conn, &DDL_SQLS["select_tab_ddl"], &[]).await;
    for sql in res {
        // println!("{}", sql);
        f_ddl.write_all(sql.as_bytes()).await.unwrap();
    }

    let res = select_sql(&conn, &DDL_SQLS["select_fk_ddl"], bv_schema).await;
    for sql in res {
        // println!("{}", sql);
        f_ddl.write_all(sql.as_bytes()).await.unwrap();
    }
    info!("Done!")
}
