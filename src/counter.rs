
use dashmap::DashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use std::time::Instant;
use async_trait::async_trait;
use log::{warn};

pub type _CounterMap = Arc<DashMap<u32, u64>>;

#[async_trait]
pub trait CounterTrait {
    fn add(&self, id: u32, n: u64);
    fn sum(&self) -> u64;
    async fn print_counter(&self);
}

#[async_trait]
impl CounterTrait for DashMap<u32, u64> {
    fn add(&self, id: u32, n: u64) {
        // let mut val = self.entry(id).or_insert(0);
        // val.add(n)
        *self.entry(id).or_insert(0) += n;
    }

    fn sum(&self) -> u64 {
        self.iter().fold(0, |acc, x| acc + x.value())
    }

    async fn print_counter(&self) {
        let begin = Instant::now();
        let mut prev_sum = 0u64;

        loop {
            sleep(Duration::from_secs(1)).await;
            let sum = self.sum();
            let ela = begin.elapsed().as_secs();
            warn!("Ela_Secs: {:>6.2} Inserted_Rows: {:>9} Rps: {}, 1secRps: {}",
                ela,
                sum,
                sum/ela,
                sum - prev_sum
            );
            prev_sum = sum;
        }

    }
}
