
extern crate num_cpus;
extern crate yaml_rust;
extern crate serde_yaml;
use futures::*;
use log::{info, debug, trace, error};
use oracle::{Connection, SqlValue, RowValue, Row, Error};
use oracle::sql_type::{ToSql, OracleType, Timestamp};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Semaphore, RwLock, OwnedRwLockWriteGuard};
use crate::common::*;
use crate::ora2my::initialize_db::*;
use crate::args::*;
use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::mpsc::{Sender, Receiver};
use r2d2_oracle::OracleConnectionManager;
use r2d2_oracle::r2d2::{PooledConnection, Pool};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::runtime::*;
use lazy_static::*;
use itertools::Itertools;
use std::collections::HashMap;

type TabStatusMap = Arc<DashMap<String, Arc<RwLock<u8>>>>;
lazy_static! {
    pub static ref TOKIO_RT: Runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(get_parallel_num())
        .enable_all()
        .build()
        .unwrap();

    // rwlock for parallel access control: during incremental mode export,
    // we should generate delete sql firstly, then insert sql.
    pub static ref TAB_STATUS: TabStatusMap = TabStatusMap::new(Default::default());
}

#[derive(Debug, Clone)]
pub struct TmpTabData {
    _owner: String,
    table_name: String,
    chunk_id: u32,
    _dop: u32,
    _has_lob: u32,
    start_rowid: String,
    end_rowid: String,
    snap_scn: u32,
    not_pk_col_cnt: u32,
    col_clause: String,
    update_clause: String,
    pk_clause: String,
    _status: String,
    _pid: String,
    _start_time: Timestamp,
    _end_time: Timestamp,
    _out_file: String,
}
impl RowValue for TmpTabData {
    fn get(row: &Row) -> std::result::Result<TmpTabData, oracle::Error> {
        let tab = TmpTabData{
            _owner: row.get::<&str, String>("OWNER").unwrap(),
            table_name: row.get::<&str, String>("TABLE_NAME").unwrap(),
            chunk_id: row.get::<&str, u32>("CHUNK_ID").unwrap(),
            _dop: row.get::<&str, u32>("DOP").unwrap(),
            _has_lob: row.get::<&str, u32>("HAS_LOB").unwrap_or(0),
            start_rowid: row.get::<&str, String>("START_ROWID").unwrap(),
            end_rowid: row.get::<&str, String>("END_ROWID").unwrap(),
            snap_scn: row.get::<&str, u32>("SNAP_SCN").unwrap(),
            not_pk_col_cnt: row.get::<&str, u32>("NOT_PK_COL_CNT").unwrap_or(0),
            col_clause: row.get::<&str, String>("COL_CLAUSE").unwrap_or("".to_string()),
            update_clause: row.get::<&str, String>("UPDATE_CLAUSE").unwrap(),
            pk_clause: row.get::<&str, String>("PK_CLAUSE").unwrap_or("".to_string()),
            _status: row.get::<&str, String>("STATUS").unwrap_or("".to_string()),
            _pid: row.get::<&str, String>("PID").unwrap_or("".to_string()),
            _start_time: row.get::<&str, Timestamp>("START_TIME").unwrap_or(Timestamp::new(1970, 1, 1, 0, 0, 0, 0)),
            _end_time: row.get::<&str, Timestamp>("END_TIME").unwrap_or(Timestamp::new(1970, 1, 1, 0, 0, 0, 0)),
            _out_file: row.get::<&str, String>("OUT_FILE").unwrap_or("".to_string()),
        };
        Ok(tab)
    }
}

pub async fn select_tab_data(conn: &Connection) -> Vec<TmpTabData>{
    let mut stmt = conn.prepare(&DATA_SQLS["select_tmp_tab_data"], &[]).unwrap();
    stmt.query_as::<TmpTabData>(&[])
        .unwrap()
        .map(|tab| tab.unwrap())
        .collect::<Vec<_>>()
}


pub async fn export_schema_data() {
    info!("Exporting schema data: schema: {}, table_name_pattern: {}, output directory: output/", &ARGS.schema, &ARGS.table_name_pattern);
    let bv_schema : &[(&str, &dyn ToSql)]= &[("schema", &ARGS.schema), ("table_name_pattern", &ARGS.table_name_pattern)];
    let start_time = Instant::now();

    let pool = get_connection_pool().await;
    set_pool_connections_properties(pool.clone()).await;
    let conn = pool.get().unwrap();
    initialize_data(&conn).await;
    prepare_data(&conn, bv_schema).await;

    match ARGS.mode.as_str() {
        "normal" => {
            drop_tmp_prev_scn(&conn);
        },
        "consistent" => {
            drop_tmp_prev_scn(&conn);
            create_tmp_prev_scn(&conn);
        },
        _ => (),
    };

    let tables = select_tab_data(&conn).await;
    let total_chunks = tables.len();
    // let mut total_tab_cnt = 0u32;
    // let mut prev_tab_name = "".to_string();
    let parallel_num = get_parallel_num();
    info!("Parallel number: {}", parallel_num);

    let throttle_sem = Arc::new(Semaphore::new(parallel_num));

    // Create a not-used channel, just for getting tx and rx vars.
    let (mut tx, mut _rx) = mpsc::channel::<String>(1);

    let mut handles: Vec<JoinHandle<(String, u32,u32, u128, u128)>> = vec![];
    let mut seq = 1u32;
    for tab in tables {
        if !TAB_STATUS.contains_key(&tab.table_name){

            // create new channel(tx, rc) and a task with rc.
            let chn = mpsc::channel::<String>(parallel_num * 4);
            tx = chn.0;
            _rx = chn.1;
            TAB_STATUS.insert(tab.table_name.clone(), Arc::new(RwLock::new(0)));
            let out_filename = get_out_filename("data", format!("{}_{}_{}", ARGS.schema, tab.table_name,"data").as_str());
            let pool = pool.clone();
            let table_name = tab.table_name.clone();
            // create recv task
            let hdl = TOKIO_RT.spawn(async move {
                sink_tab_data(pool, out_filename, table_name.clone(), _rx,
                              format!("{:>4}/{}", seq, total_chunks).clone()).await;

                // no meaning, just for compatibility with the returned handle of fetch_tab_data.
                ("".to_string(), 0, 0, 0, 0)
            });
            handles.push(hdl);
        }

        let pool = pool.clone();
        let tab2 = tab.clone();

        let access_lock = TAB_STATUS.get(&tab.table_name).unwrap().value().clone();
        // hold the write lock on the tab to make sure the delete-sql will be generated in advance of insert-sql.
        let write_guard = if ARGS.mode.as_str() == "incremental" && tab2.chunk_id == 1 {
            Some(access_lock.write_owned().await)
        } else {
            None
        };

        let parallel_permit = throttle_sem.clone().acquire_owned().await.unwrap();
        let tx2 = tx.clone();
        let hdl = TOKIO_RT.spawn(async move {
            let seq_str = format!("{:>4}/{}", seq, total_chunks);
            // let (table_name, exp_row_cnt, del_row_cnt, exp_row_ela, edel_row_ela) = fetch_tab_data(pool, tab2, tx2, seq_str, write_guard).await;
            let res = fetch_tab_data(pool, tab2, tx2, seq_str, write_guard).await;
            drop(parallel_permit);
            res
        });
        handles.push(hdl);

        // if tab.table_name != prev_tab_name {
        //     total_tab_cnt = total_tab_cnt + 1;
        //     prev_tab_name = tab.table_name.clone();
        // }
        seq = seq + 1;
    }
    drop(tx);
    let res_handles = future::join_all(handles).await;

    save_prev_scn(&conn).await;

    let res_handles = res_handles
        .into_iter()
        .map(|h| h.unwrap())
        .collect::<Vec<_>>();
    info_summary(res_handles, start_time.elapsed().as_secs());

}

fn info_summary(res_handles: Vec<(String, u32, u32, u128, u128)>, secs: u64) {
    info!("Summary:");

    // sum over group by table_name
    let hmap = res_handles
        .iter()
        .fold(HashMap::new(), |mut acc, h|{
            let e = acc.entry(h.0.clone()).or_insert((0, 0, 0, 0));
            *e = (e.0 + h.1, e.1 + h.2, e.2 + h.3, e.3 + h.4);
            acc
        });

    for tab in hmap.keys().sorted(){
        if tab.is_empty() {
            continue;
        }
        let e = hmap.get(tab).unwrap();
        info!("Table: {:<30}, Rows exported: {:>12}, ela(secs): {:>10}. Rows for delete: {:>12}, ela(secs): {:>10}", tab, e.0, e.2/1000000, e.1, e.3);
    }

    let (tot_exp_cnt, tot_del_cnt)= res_handles.iter().fold((0u32, 0u32), |(exp_cnt, del_cnt), hdl| {
        let (_, chunk_exp_cnt, chunk_del_cnt, _, _) = hdl;
        (exp_cnt + chunk_exp_cnt, del_cnt + chunk_del_cnt)
    });
    info!("Exporting schema data finished: schema: {}, table_name_pattern: \"{}\". Exported tables: {}. Total Exported Rows: {}. Total Rows For Delete: {}. Ela(secs): {}",
        &ARGS.schema,
        &ARGS.table_name_pattern,
        0, //total_tab_cnt,
        tot_exp_cnt,
        tot_del_cnt,
        secs
    );
}

pub async fn save_prev_scn(conn: &PooledConnection<OracleConnectionManager>) {
    match ARGS.mode.as_str() {
        "consistent"  => {
            insert_tmp_prev_scn(&conn);
            let prev_scn = get_prev_scn(&conn).await;
            info!("This consistent mode export is based on SCN {}. It will be used for the subsequent incremental mode export", prev_scn);
        },
        "incremental" => {
            truncate_tmp_prev_scn(&conn);
            insert_tmp_prev_scn(&conn);
            let prev_scn = get_prev_scn(&conn).await;
            info!("This incremental mode export is based on SCN {}. It will be used for the subsequent incremental mode export", prev_scn);
        },
        _ => (),
    }

}

/// Receive table data from channel then sink to file.
/// _conn var is not needed yet.
pub async fn sink_tab_data(_pool: Pool<OracleConnectionManager>,
                           out_filename: String,
                           table_name: String,
                           mut rx: Receiver<String>,
                           seq: String) {
    let mut f_data = prepare_out_file(out_filename.as_str()).await;
    // loop will be ended until all tx released.
    while let Some(data) = rx.recv().await {
        trace!("Sinker received: {}", data);
        f_data.write_all(data.as_bytes()).await.unwrap();
    }
    // todo: Check the status of each chunks.
    // Do we need to assume and handle the case that some tasks crashed?

    debug!("{} Table {} sinking task finished, out_file: {}", &seq, table_name, &out_filename);
}

// move this function to common mod. v0.9.8
// pub async fn prepare_out_file(out_filename: &str) -> File {
//     let mut f_data = File::create(out_filename).await.unwrap();
//     f_data.write_all(format!("-- This sql file was created by data migration tool Qinghe v0.9.9 (https://github.com/zhaopinglu/qinghe).\n").as_bytes()).await.unwrap();
//     f_data.write_all(format!("-- The timestamp/date values in this file were using UTC timezone.\n").as_bytes()).await.unwrap();
//     f_data.write_all(format!("-- So make sure the session timezone is UTC before execute the follwing sql.\n").as_bytes()).await.unwrap();
//     f_data.write_all(format!("set time_zone='+00:00';\n").as_bytes()).await.unwrap();
//     f_data.write_all(format!("set FOREIGN_KEY_CHECKS=0;\n").as_bytes()).await.unwrap();
//     f_data.write_all(format!("\n").as_bytes()).await.unwrap();
//     f_data
// }


pub async fn build_fetch_sql(conn: &PooledConnection<OracleConnectionManager>, tab: &TmpTabData, sql_type: &str) -> String {
    match ARGS.mode.as_str() {
        "normal" => {
            format!("select * from \"{}\".\"{}\" where rowid between '{}' and '{}'",
                    &ARGS.schema, &tab.table_name, &tab.start_rowid, &tab.end_rowid)
        },
        "consistent" => {
            format!("select * from \"{}\".\"{}\" as of scn {} where rowid between '{}' and '{}'",
                    &ARGS.schema, &tab.table_name, &tab.snap_scn, &tab.start_rowid, &tab.end_rowid )
        },
        "incremental" => {
            if sql_type == "insert" {
                format!("select * from \"{}\".\"{}\" as of scn {} where ora_rowscn > {} and rowid between '{}' and '{}'",
                        &ARGS.schema, &tab.table_name, &tab.snap_scn, get_prev_scn(&conn).await, &tab.start_rowid, &tab.end_rowid)
            } else if sql_type == "delete" {
                let val_clause = if tab.pk_clause == "" { "*".to_string() } else { tab.pk_clause.replace("`","\"") };
                format!("select {} from \"{}\".\"{}\" as of scn {} WHERE rowid NOT IN (SELECT rowid FROM \"{}\".\"{}\" AS OF scn {})",
                        val_clause, &ARGS.schema, &tab.table_name, get_prev_scn(&conn).await,&ARGS.schema, &tab.table_name, &tab.snap_scn,)
            } else {
                panic!("Wrong sql_type value:{}", sql_type)
            }
        },
        _ => panic!("Wrong mode value:{}", ARGS.mode)
    }
}

/// sql_type --- sql_prefix1 -----     col/pk_clause  sql_prefix2 -- val_clause --   --------------- sql_suffix --------------------------
/// insert   insert [ignore] into xxx  ( ... )        values      ( ... ), ( ... )   on duplicate key update c1=values(c1), c2=values(c2);
/// delete   delete from xxx where     ( ... )        in (        ( ... ), ( ... )   );
pub fn build_sql_prefix_and_suffix(tab: &TmpTabData, sql_type: &str) -> (String, String) {
    let mut _prefix = String::new();
    let mut _suffix = String::new();
    match sql_type {
        "insert" => {
            if tab.not_pk_col_cnt > 0 {
                _prefix = format!("insert into `{}`({}) values ", tab.table_name, tab.col_clause)
            } else {
                _prefix = format!("insert ignore into `{}`({}) values ", tab.table_name, tab.col_clause)
            }
            _suffix = format!("{};\n", tab.update_clause);
        },
        "delete" => {
            let val_clause = if tab.pk_clause == "" { &tab.col_clause } else { &tab.pk_clause };
            _prefix = format!("delete from `{}` where ({}) in (", tab.table_name, val_clause);
            _suffix = format!(");\n");
        },
        _ => panic!("Wrong argument values: sql_type {}", sql_type)
    }
    (_prefix, _suffix)
}

pub async fn build_full_sql_and_send(conn: &PooledConnection<OracleConnectionManager>,
                                     tab: &TmpTabData,
                                     sender: &Sender<String>,
                                     seq: &str,
                                     sql_type: &str) -> (u32, u128) {
    let start_time = Instant::now();
    let buf_size = 1048576 * 4;
    let (sql_prefix, sql_suffix) = build_sql_prefix_and_suffix(&tab, sql_type);
    let fetch_sql = build_fetch_sql(&conn, tab, sql_type).await;
    debug!("{} Fetch sql: {}", &seq, fetch_sql);

    let res = conn.query_named(&fetch_sql, &[]);

    // With incremental mode, while exporting a newly created table, which was created after previous consistent/incremental export,
    // will hit error ORA-01466: unable to read data - table definition has changed.
    match res {
        Err(Error::OciError(e)) if e.code() == 1466 => {
            error!("Skip generating {} sql for table {} due to database error: {}. It seems this table was created after the previous consistent/incremental mode export.",
                sql_type, tab.table_name, e.message());
            return (0,0);
        },
        _ => {}
    }

    let rows = res.unwrap();

    let mut refined_values: Vec<String> = vec![String::new(); rows.column_info().len()];
    let mut row_cnt = 0u32;
    let mut comma_flag = false;

    let mut buf = String::with_capacity(buf_size);
    for row_result in rows {
        row_cnt = row_cnt + 1;

        let row = row_result.unwrap();
        for (col_idx, val) in row.sql_values().iter().enumerate() {
            refined_values[col_idx] = refine_sql_value(val);
            // trace!("Column Info: Tab: {:-30} Col: {:-30} [{:-30}] [{:-8}]: {}", tab.table_name, col_info[col_idx].name(),
            // val.oracle_type().unwrap().to_string(), if col_info[col_idx].nullable() {"NULL"} else {"NOT NULL"}, refined_values[col_idx]);
        }
        let value_clause = build_sql_value_clause(&refined_values);

        if row_cnt == 1 {
            buf.push_str(sql_prefix.as_str());
            buf.push_str(value_clause.as_str());
            comma_flag = true;
            continue;
        }
        if ARGS.batch_number == 0u32 {
            if comma_flag == true {
                buf.push_str(",");
            }
            buf.push_str(value_clause.as_str());
            comma_flag=true;
            continue;
        }
        // implicit: total_rows_num > 1
        if ( row_cnt -1 ) % ARGS.batch_number != 0 {
            if comma_flag == true {
                buf.push_str(",");
            }
            buf.push_str(value_clause.as_str());
            comma_flag=true;
        } else {
            buf.push_str(sql_suffix.as_str());
            sender.send(buf.clone()).await.unwrap();
            buf.clear();
            buf.push_str(sql_prefix.as_str());
            buf.push_str(value_clause.as_str());
            comma_flag=true;
        }
    }
    if row_cnt > 0 {
        buf.push_str(sql_suffix.as_str());
        sender.send(buf).await.unwrap();
    }
    (row_cnt, start_time.elapsed().as_micros())
}

/// Fetch table data from Oracle then send to sinker task via channel.
pub async fn fetch_tab_data(pool: Pool<OracleConnectionManager>,
                            tab: TmpTabData,
                            sender: Sender<String>,
                            seq: String,
                            rwlock_guard: Option<OwnedRwLockWriteGuard<u8>>) -> (String, u32, u32, u128, u128) {
    debug!("{} Table {}.{} (Chunk {}) exporting ...", &seq, &ARGS.schema, tab.table_name, tab.chunk_id);
    debug!("fetch_tab_data - Requesting connection from pool. {:?}", pool.state());
    let conn = pool.get().unwrap();

    let output_prefix = format!("{} Table {}.{:<30} - (Chunk {:<4})", seq, &ARGS.schema, &tab.table_name, tab.chunk_id);

    let (chunk_del_cnt, chunk_del_ela) =
        // Note: only run delete-sql export job when chunk_id=1.
        // Block the insert-sql export futures until the delete-sql export future finished. Will use with tokio rwlock.
        if ARGS.mode.as_str() == "incremental" && tab.chunk_id == 1 {
            debug!("{} Delete SQL generation begin. ", output_prefix);
            let (chunk_del_cnt, chunk_del_ela) = build_full_sql_and_send(&conn, &tab, &sender, seq.as_str(), "delete").await;
            // Release the write lock after generated the delete-sql.
            if let Some(guard) = rwlock_guard {
                drop(guard);
            }
            info!("{} Delete SQL generated. Rows: {:<9}. Ela(sec): {:<5}. Rows/Sec: {}",
                output_prefix, chunk_del_cnt, chunk_del_ela/1000000,
                if chunk_del_ela > 0 { chunk_del_cnt as u128 * 1000000 / chunk_del_ela } else { 0 }
            );
            (chunk_del_cnt, chunk_del_ela)
        } else {
            (0u32, 0u128)
        };

    // Request the read lock before processing the insert-sql.
    // Make sure the insert-sql process won't begin until the delete-sql process finished.
    let read_guard = TAB_STATUS.get(&tab.table_name).unwrap().value().clone().read_owned().await;
    debug!("{} Insert SQL generation begin. ", output_prefix);
    let (chunk_exp_cnt, chunk_exp_ela) = build_full_sql_and_send(&conn, &tab, &sender, seq.as_str(), "insert").await;
    drop(read_guard);
    info!("{} exported. Rows: {:<9}. Ela(sec): {:<5}. Rows/Sec: {}",
        output_prefix, chunk_exp_cnt, chunk_exp_ela/1000000,
        if chunk_exp_ela > 0 { chunk_exp_cnt as u128 * 1000000 / chunk_exp_ela } else { 0 }
    );
    // v0.9.9
    (tab.table_name, chunk_exp_cnt, chunk_del_cnt, chunk_exp_ela, chunk_del_ela)
}

pub fn build_sql_value_clause(refined_values: &[String]) -> String {
    format!("({})", refined_values.join(", "))
}

pub async fn prepare_data(conn: &PooledConnection<OracleConnectionManager>, bv_schema: &[(&str, &dyn ToSql)]) {
    info!("Preparing ...");
    insert_tmp_tab(&conn, &DATA_SQLS["insert_tmp_seg_ext"], bv_schema).await;
    insert_tmp_tab(&conn, &DATA_SQLS["insert_tmp_tab_data"], bv_schema).await;
}


pub fn refine_sql_value(val: &SqlValue) -> String {
    if val.is_null().unwrap() {
        return String::from("NULL");
    }

    let ora_type = val.oracle_type().unwrap();
    let refined_val = match ora_type {
        OracleType::Varchar2(..) |
        OracleType::NVarchar2(..) |
        OracleType::Char(..) |
        OracleType::NChar(..) |
        OracleType::CLOB |
        OracleType::NCLOB => {
            format!("'{}'", val.to_string().replace("'","''").replace("\\", "\\\\"))
        },
        OracleType::Date |
        OracleType::Rowid |
        OracleType::IntervalDS(..) |
        OracleType::IntervalYM(..) => {
            format!("'{}'", val.to_string())
        },
        OracleType::Number(..) |
        OracleType::Int64 |
        OracleType::UInt64 |
        OracleType::Float(..) |
        OracleType::BinaryDouble |
        OracleType::BinaryFloat => {
            val.to_string()
        }
        OracleType::TimestampLTZ(..) => {
            let ts= val.get::<Timestamp>().unwrap();
            // For data type: 'timestamp with local time zone', the datetime part of value will be converted to current time zone.
            // But the timezone property will still be set to the original time zone value.
            // It seems a bug of rust-oracle.
            // So as a workaround, set the time zone value of TimestampLTZ to 0 (aka. UTC).
            let utc_dt = to_utc(ts.and_tz_offset(0));
            let utc_str = utc_dt.format("\'%F %T.%f\'").to_string();

            utc_str
        },
        OracleType::Timestamp(..) |
        OracleType::TimestampTZ(..) => {
            // samples: 2021-09-02 18:03:42.475227 +8:00  or 2021-09-02 18:03:42.475227 Asia/Shanghai
            // 2 rules to make sure the correctness of tz data migration:
            //  1. set session tz to utc in source db(oracle), and convert the tz data with utc tz offset when exporting.
            //  2. set session tz also to utc in target db(mysql), and use timestamp data type in target db(mysql).
            //      (add set timezone before import data in target db.)
            let ts= val.get::<Timestamp>().unwrap();
            let utc_dt = to_utc(ts);
            // utc_dt.format("%Y-%m-%d %H:%M:%S.%f")
            let utc_str = utc_dt.format("\'%F %T.%6f\'").to_string();
            let utc_str = utc_str.strip_suffix(".000000").unwrap_or(&utc_str).strip_suffix(".000").unwrap_or(&utc_str);
            // info!("val: {:?}, ora_type: {:?}, timestamp value: {:?}", val, ora_type, utc_str);
            utc_str.to_string()
        },
        OracleType::BLOB |
        OracleType::Raw(..) |
        OracleType::LongRaw => {
            // mysql insert sql for BLOB data: x'504B03...', seems no length limitation.
            // oracle insert sql for BLOB data: '504B03...'. But better use to_blob('xxx...') instead.
            // and the length of each piece in to_blob should not exceed 1000. Because the max byte length of to_blob is 4000
            // and some unicode char occupies 4 bytes.
            format!("x'{}'", val.to_string())
        },
        _ => val.to_string().replace("'","''"),
    };
    refined_val
}

