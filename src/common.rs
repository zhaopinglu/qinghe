extern crate yaml_rust;
extern crate serde_yaml;
use log::{error, debug, };
use oracle::{Connection, Statement, };
use oracle::sql_type::{ToSql,  Timestamp};
use chrono::{Utc, TimeZone, FixedOffset};
use chrono::prelude::*;
use crate::args::*;
use r2d2_oracle::{OracleConnectionManager, r2d2};
use r2d2_oracle::r2d2::{Pool, };
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Get output filename.
///
/// # Arguments
/// * `file_type - file type. Suggested values: 'ddl', 'data'
pub fn get_out_filename(export_content: &str, keyword: &str) -> String {
    let prefix = if ARGS.output_prefix == DEF_OUTPUT_FILENAME { "output/".to_string() } else { format!("output/{}_", ARGS.output_prefix) };
    let out_file = match export_content {
        "metadata" => format!("{}{}.sql", prefix, keyword),
        "data" => format!("{}{}_{}.sql", prefix, ARGS.mode, keyword ),
        _ => unimplemented!(),
    };
    out_file
}

pub fn to_utc(ts: Timestamp) -> DateTime<Utc> {
    let offset_secs = ts.tz_offset();
    let fixed_offset = if offset_secs > 0 {FixedOffset::east(offset_secs)} else {FixedOffset::west(offset_secs)};
    let fixed_dt = fixed_offset
        .ymd(ts.year(), ts.month(), ts.day())
        .and_hms_nano(ts.hour(), ts.minute(), ts.second(), ts.nanosecond());
    let utc_dt: DateTime<Utc> = DateTime::from(fixed_dt);
    utc_dt
}

pub async fn exec_sql<'a>(conn: &'a Connection, sql: &'a str, val: &[(&'a str, &dyn ToSql)]) -> oracle::Result<Statement<'a>> {
    debug!("exec_sql: {}", sql);
    let res = conn.execute_named(sql, val);
    res
}


pub async fn select_sql(conn: &Connection, sql: &str, val: &[(&str, &dyn ToSql)]) -> Vec<String> {
    debug!("select_sql: {}", sql);
    let res = conn.query_named(sql, val).unwrap();
    res.map(|row|{
        row.unwrap().get_as::<String>().unwrap()
    }).collect::<Vec<String>>()
}


pub async fn create_tab(conn: &Connection, sql: &str) {
    let res = exec_sql(conn, sql, &[]).await;
    match res {
        Ok(_) => (),
        Err(e) => {
            panic!("Is there another instance of this program running? Creating table failed due to error: {:?}, sql: {}", e, sql);
        }
    }
}

pub async fn insert_tmp_tab(conn: &Connection, sql: &str, val: &[(&str, &dyn ToSql)]) {
    let res = exec_sql(conn, sql, val).await;
    match res {
        Ok(_) => {
            conn.commit().unwrap();
        },
        Err(e) => {
            error!("Failed to create table due to error: {:?}, sql: {}", e, sql);
            panic!("Failed to create table due to error: {:?}, sql: {}", e, sql);
        }
    }
}

pub async fn get_connection() -> Connection {
    let conn_str = format!("//{}:{}/{}", &ARGS.host, &ARGS.port, &ARGS.service_name);
    let mut conn = Connection::connect(&ARGS.user, &ARGS.password, conn_str).unwrap();
    conn.set_autocommit(true);
    conn
}

pub async fn get_connection_pool() -> Pool<OracleConnectionManager> {
    let conn_str = format!("//{}:{}/{}", ARGS.host, ARGS.port, ARGS.service_name);

    // Try standalone connection firstly, to exam the connection string. thus the pgm can abort if anything wrong in connection string or database.
    // Without this pre-test connection request, the r2d2 pool will throw lots of connection errors as below if something wrong in connection string.
    // ORA-12514: TNS:listener does not currently know of service requested in connect descriptor
    let conn = Connection::connect(&ARGS.user, &ARGS.password, &conn_str).unwrap();
    conn.close().unwrap();

    let manager =
        OracleConnectionManager::new(&ARGS.user, &ARGS.password, &conn_str);

    let parallel_num = get_parallel_num();
    let pool = r2d2::Pool::builder()
        .max_size(parallel_num as u32 + 2)
        .build(manager)
        .unwrap();

    let _ = (0..pool.max_size()).map(|_|{
        let mut conn = pool.get().unwrap();
        conn.set_autocommit(true);
    });
    pool
}

pub fn get_parallel_num() -> usize {
    match ARGS.parallel as usize {
        0 => num_cpus::get(),
        n => n,
    }
}

pub fn abort_on_panic() {
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));
}


pub async fn prepare_out_file(out_filename: &str) -> File {
    let mut f_data = File::create(out_filename).await.unwrap();
    f_data.write_all(format!("-- This sql file was created by data migration tool Qinghe v0.9.10 (https://github.com/zhaopinglu/qinghe).\n").as_bytes()).await.unwrap();
    f_data.write_all(format!("-- The timestamp/date values in this file were using UTC timezone.\n").as_bytes()).await.unwrap();
    f_data.write_all(format!("-- So make sure the session timezone is UTC before execute the follwing sql.\n").as_bytes()).await.unwrap();
    f_data.write_all(format!("set time_zone='+00:00';\n").as_bytes()).await.unwrap();
    f_data.write_all(format!("set FOREIGN_KEY_CHECKS=0;\n").as_bytes()).await.unwrap();
    f_data.write_all(format!("\n").as_bytes()).await.unwrap();
    f_data
}
