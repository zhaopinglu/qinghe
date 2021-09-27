mod args;
mod counter;
mod ora2my;
mod common;

extern crate yaml_rust;
extern crate serde_yaml;
use log::{Level, info};
use structopt::StructOpt;
use args::*;
use ora2my::*;
use crate::common::abort_on_panic;

#[tokio::main]
async fn main() {
    let log_lev: Level = match ARGS.log_level.as_str() {
        "error" => Level::Error,
        "warn" => Level::Warn,
        "info" => Level::Info,
        "debug" => Level::Debug,
        "trace" => Level::Trace,
        _  => unimplemented!(),
    };

    fast_log::init_log("qinghe.log", 1000, log_lev, None, true).unwrap();

    info!("Using arguments:\n{:#?}\n------------------------------------------------------------------------", Arguments::from_args());

    abort_on_panic();

    export_oracle().await;

}