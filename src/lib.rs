use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::future::{select, Either};
use log::{debug, error, trace, warn};
use tokio::{sync::mpsc::{unbounded_channel, UnboundedSender}, time::{Instant, Sleep, sleep_until}};

#[derive(Debug, Clone, Copy)]
enum Cmd {
    Terminate,
    Continue,
}

pub struct Leaky {
    counter: Arc<AtomicUsize>,
    capacity: usize,
    sender: UnboundedSender<Cmd>,
}

const LONG_DURATION: Duration = Duration::from_secs(3*365*24*3600);

#[inline]
fn sleep(period: Duration) -> Option<(Instant, Sleep)> {
    let t =  Instant::now();
    Some((t, sleep_until(t+period)))

}

impl Leaky {
    pub fn new(capacity: usize, rate: f32) -> Self {
        const KILO: f32 = 1_000.0;
        assert!(rate <= KILO, "Is not much usable beyond ms");
        assert!(capacity > 0);
        let interval_ms: u64 = (KILO / rate) as u64;
        let counter = Arc::new(AtomicUsize::new(0));
        let (sender, mut recipient) = unbounded_channel();
        let counter2 = counter.clone();
        let _t = tokio::spawn(async move {
            let period = Duration::from_millis(interval_ms);
            debug!("period {:?}", period);
            let mut next_time = sleep(period);
            loop {
                let (prev_time, next_tick) = next_time.take().unwrap();
                let tick = Box::pin(next_tick);
                let recv = Box::pin(recipient.recv());
                match select(tick, recv).await {
                    Either::Left((_, _r)) => {
                        let endured =Instant::now().duration_since(prev_time).as_millis();
                        let mut ratio = (endured as u64 / interval_ms) as usize;
                        if ratio > 1 {
                            warn!("has been waiting too long {}x", ratio)
                        } else if ratio == 0 {
                            warn!("has been waiting too shoot {}", endured);
                            // we always want to proceed
                            ratio =1
                        }
                        let v = counter2.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                            if v > 0 {
                                Some(v.saturating_sub(ratio))
                            } else {
                                None
                            }
                        });
                        trace!("Tick for old v {:?}", v);
                        match v {
                            Ok(1) | Err(0) => next_time = { 
                                sleep(LONG_DURATION)
                            },
                            _ => next_time = {
                                sleep(period)
                            }
                        }
                    }
                    Either::Right((cmd, _)) => match cmd {
                        Some(Cmd::Continue) => next_time = {
                            sleep(period)
                        },
                        Some(Cmd::Terminate) | None => break,
                    },
                };
            }
        });
        Leaky {
            counter,
            capacity,
            sender,
        }
    }

    pub fn start_one(&self) -> Result<usize, usize> {
        let mut v = self.counter.load(Ordering::SeqCst);
        if v >= self.capacity {
            Result::Err(v)
        } else {
            while let Err(n) =
                self.counter
                    .compare_exchange(v, v + 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                if n >= self.capacity {
                    return Result::Err(n);
                } else {
                    v = n
                }
            }
            if v == 0 {
                // we just get some slots filled start leaking
                self.sender.send(Cmd::Continue).ok();
            }
            Result::Ok(v + 1)
        }
    }

    pub fn immediate_capacity(&self) -> usize {
        self.capacity - self.counter.load(Ordering::Relaxed)
    }
}

impl Drop for Leaky {
    fn drop(&mut self) {
        if let Err(_) = self.sender.send(Cmd::Terminate) {
            error!("Cannot send terminate to leaky background task")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    use log::{debug, trace};
    use tokio::time::{interval, sleep};

    use crate::Leaky;

    type Error = Box<dyn std::error::Error + Send + 'static>;
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_interval() -> Result<(), Error> {
        env_logger::try_init().ok();
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let t = tokio::spawn(async move {
            let mut i = interval(Duration::from_millis(10));
            let start = Instant::now();
            for _i in 0..100 {
                let prev = c.fetch_add(1, Ordering::Relaxed);
                let enlapsed = Instant::now().duration_since(start).as_millis();
                trace!("tick {} - time {}", prev, enlapsed);
                i.tick().await;
            }
        });
        t.await.unwrap();
        assert_eq!(100, counter.load(Ordering::Relaxed));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_leaky_basic() {
        env_logger::try_init().ok();
        let leaky = Leaky::new(50, 50.0);
        for i in 1..=50 {
            let res = leaky.start_one();
            assert!(res.is_ok());
            let n = res.unwrap();
            assert_eq!(n, i)
        }
        //now leaky should be full
        for _i in 1..=10 {
            let res = leaky.start_one();
            if let Err(n) = res {
                assert_eq!(n, 50)
            } else {
                panic!("Leaky should be full")
            }
        }
        // wait a bit for leak:
        sleep(Duration::from_millis(40)).await;
        let res = leaky.start_one();
        if let Ok(n) = res {
            assert!(n>48 && n <= 50, "should release one or two slots");
        } else {
            panic!("Slot was not released by leaky")
        }

        // wait bit more
        sleep(Duration::from_millis(300)).await;

        let res = leaky.start_one();
        if let Ok(n) = res {
            debug!("Taken slots after 300ms {}", n);
            assert!(n < 50 - 10, "taken slots should decrease by at least 10");
        } else {
            panic!("Slot was not released by leaky")
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_leaky_pausing() {
        env_logger::try_init().ok();
        let leaky = Leaky::new(10, 500.0);
        for _i in 1..=10 {
            assert!(leaky.start_one().is_ok());
        }
        //should be full now
        assert!(leaky.start_one().is_err());
        sleep(Duration::from_millis(50)).await;
        assert_eq!(leaky.immediate_capacity(), 10);
        sleep(Duration::from_millis(200)).await;

        // again

        for _i in 1..=10 {
            assert!(leaky.start_one().is_ok());
        }
        //should be full now
        assert!(leaky.start_one().is_err());
        sleep(Duration::from_millis(50)).await;
        assert_eq!(leaky.immediate_capacity(), 10);

    }
}
