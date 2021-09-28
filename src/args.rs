use structopt::StructOpt;
use lazy_static::*;

pub const DEF_OUTPUT_FILENAME: &str = "<SCHEMA>_<..>";

#[derive(StructOpt, Debug)]
#[structopt(name = "Qinghe", about = "Zhaoping Lu <zhaopinglu77@gmail.com>\nA tool to migrate schema and data from Oracle 11g+ to MySQL 5.7+.\nFeel free to use this software and there is no warranty for it.")]
pub struct Arguments {
    /// Activate debug mode
    #[structopt(long)]
    pub debug: bool,

    /// User
    #[structopt(short, long, default_value = "test")]
    pub user: String,

    /// Password
    #[structopt(short, long, default_value = "test")]
    pub password: String,

    /// Host
    #[structopt(short = "H", long, default_value = "192.168.12.5")]
    pub host: String,

    /// Port
    #[structopt(short = "P", long, default_value = "1521" )]
    pub port: u32,

    /// Service_name
    #[structopt(short, long, default_value = "orcl" )]
    pub service_name: String,

    /// Schema
    #[structopt(short = "S", long, default_value = "TEST" )]
    pub schema: String,

    /// Output file name suffix, will be followed by a suffix string, like "_ddl.sql".
    #[structopt(short = "o", long, default_value = &DEF_OUTPUT_FILENAME )]
    pub output_prefix: String,

    /// The number of parallel tasks for table data exporting.
    /// Valid values: 0: Auto (=cpu count). 1: No parallel. 2: Run 2 tasks for data exporting, etc,. .
    #[structopt(short = "x", long, default_value = "0" )]
    pub parallel: u32,

    /// Content. Valid values are: metadata, data, all
    #[structopt(short = "c", long, default_value = "metadata" )]
    pub content: String,

    /// Specify the number of value clauses for the generated
    /// multiple-row-syntax INSERT statements.
    /// 0: means no limit.
    #[structopt(short = "b", long, default_value = "200" )]
    pub batch_number: u32,

    /// Specify the table name pattern for exporting.
    #[structopt(short = "t", long, default_value = "." )]
    pub table_name_pattern: String,

    /// Log level. Valid values: error, warn, info, debug, trace.
    #[structopt(short = "l", long, default_value = "info" )]
    pub log_level: String,

    /// Consistent mode. Only meanful when content="data".
    /// # Valid values:
    /// * `normal`: Export all tables' data as they are;
    /// * `consistent`: Export all table data in a consistent snapshot;
    /// * `incremental`: Export the data changed since the previous consistent or incremental export.
    /// # Note: 'consistent' or 'increment' modes export could hit ORA-01555 error if don't have sufficient undo tablespace.
    #[structopt(short = "m", long, default_value = "normal" )]
    pub mode: String,
}


lazy_static!{
    pub static ref ARGS: Arguments = Arguments::from_args();
}