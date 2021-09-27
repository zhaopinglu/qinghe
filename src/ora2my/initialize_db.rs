extern crate yaml_rust;
extern crate serde_yaml;
use yaml_rust::{YamlLoader, Yaml};
use std::fs::{read_to_string};
use linked_hash_map::LinkedHashMap;
use oracle::{Connection, Error};
use std::collections::HashMap;
use lazy_static::*;
use crate::common::*;
use r2d2_oracle::r2d2::{PooledConnection, Pool};
use r2d2_oracle::OracleConnectionManager;
use log::{info, debug, warn, error};

lazy_static! {
    pub static ref DDL_SQLS: HashMap<String, String> = get_data_type_mappings("ora2my_ddl.yaml");
    pub static ref DATA_SQLS: HashMap<String, String> = get_data_type_mappings("ora2my_data.yaml");
}

pub fn get_data_type_mappings(config_file: &str) -> HashMap<String, String> {
    let contents = read_to_string(config_file).unwrap();
    let docs = YamlLoader::load_from_str(&contents).unwrap();
    let type_map: &LinkedHashMap<Yaml, Yaml> = docs[0]["Oracle2MySQL"].as_hash().unwrap();
    let mut res = HashMap::<String, String>::new();
    type_map.iter().for_each(|(key, value)|{
        res.insert(key.as_str().unwrap().to_string(), value.as_str().unwrap().to_string());
    });
    res
}

pub async fn initialize_ddl(conn: &Connection) {
    info!("Creating helper objects for ddl export ...");
    let _ = exec_sql(&conn, &DDL_SQLS["drop_tmp_cons"], &[]).await;
    let _ = exec_sql(&conn, &DDL_SQLS["drop_tmp_cols"], &[]).await;
    let _ = exec_sql(&conn, &DDL_SQLS["drop_tmp_tab_ddl"], &[]).await;

    create_tab(&conn, &DDL_SQLS["create_tmp_cols"]).await;
    create_tab(&conn, &DDL_SQLS["create_tmp_cons"]).await;
    create_tab(&conn, &DDL_SQLS["create_tmp_tab_ddl"]).await;
}

pub async fn initialize_data(conn: &PooledConnection<OracleConnectionManager>) {
    info!("Creating helper objects for data export ...");
    let _ = exec_sql(&conn, &DATA_SQLS["drop_tmp_tab_data"], &[]).await;
    create_tab(&conn, &DATA_SQLS["create_tmp_tab_data"]).await;

    drop_tmp_seg_ext(&conn);
    create_tmp_seg_ext(&conn);
}

pub fn drop_tmp_seg_ext(conn: &PooledConnection<OracleConnectionManager>) {
    let sql = &DATA_SQLS["drop_tmp_seg_ext"];
    debug!("sql drop_tmp_seg_ext: {}", sql);
    let _ = conn.execute_named(sql, &[]);
}

pub fn create_tmp_seg_ext(conn: &PooledConnection<OracleConnectionManager>) {
    let sql = &DATA_SQLS["create_tmp_seg_ext"];
    debug!("sql create_tmp_seg_ext: {}", sql);
    conn.execute_named(sql, &[]).unwrap();
}

pub fn drop_tmp_prev_scn(conn: &PooledConnection<OracleConnectionManager>) {
    let sql = &DATA_SQLS["drop_tmp_prev_scn"];
    debug!("sql drop_tmp_prev_scn: {}", sql);
    let _ = conn.execute_named(sql, &[]);
}

pub fn create_tmp_prev_scn(conn: &PooledConnection<OracleConnectionManager>) {
    let sql = &DATA_SQLS["create_tmp_prev_scn"];
    debug!("sql create_tmp_prev_scn: {}", sql);
    conn.execute_named(sql, &[]).unwrap();
}

pub fn truncate_tmp_prev_scn(conn: &PooledConnection<OracleConnectionManager>) {
    let sql = &DATA_SQLS["truncate_tmp_prev_scn"];
    debug!("sql truncate_tmp_prev_scn: {}", sql);
    conn.execute_named(sql, &[]).unwrap();
}

pub fn insert_tmp_prev_scn(conn: &PooledConnection<OracleConnectionManager>) {
    let sql = &DATA_SQLS["insert_tmp_prev_scn"];
    debug!("sql insert_tmp_prev_scn: {}", sql);
    conn.execute_named(sql, &[]).unwrap();
    conn.commit().unwrap();
}

pub async fn get_prev_scn (conn: &PooledConnection<OracleConnectionManager>) -> u32 {
    let sql = &DATA_SQLS["select_tmp_prev_scn"];
    debug!("sql_select_tmp_prev_scn: {}", sql);
    conn.query_row_as::<u32>(sql, &[])
        .or_else(|err| {
            match err {
                ref e if matches!(e,Error::NoDataFound) => {
                    warn!("No data found in snapshot scn table. Maybe the table pattern didn't match any table?");
                },
                _ => {
                    error!("*** Something is wrong when querying the previous snapshot scn. Did a consistent mode export successfully finished before? {:?}", err);
                },
            }
            Err(err)
        }).unwrap()
}

pub async fn set_pool_connections_properties(pool: Pool<OracleConnectionManager>) {
    let _ = (0..pool.max_size()).map(|_| async {
        let conn = pool.get().unwrap();
        set_nls_format(&conn).await;
        set_session_tz_utc(&conn).await;
    });
}

pub async fn set_connection_properties(conn: &Connection) {
    set_nls_format(conn).await;
    set_session_tz_utc(conn).await;
}

pub async fn set_nls_format(conn: &Connection) {
    let _ = exec_sql(&conn, &DDL_SQLS["set_nls_date_fmt"], &[]).await;
    let _ = exec_sql(&conn, &DDL_SQLS["set_nls_timestamp_fmt"], &[]).await;
    let _ = exec_sql(&conn, &DDL_SQLS["set_nls_timestamp_tz_fmt"], &[]).await;
}

pub async fn set_session_tz_utc(conn: &Connection) {
    let _ = exec_sql(&conn, &DDL_SQLS["set_session_tz_utc"], &[]).await;
}
