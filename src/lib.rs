use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::{future::poll_fn, ready};
use log::{error, trace, warn};
use tokio::{sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, time::{Instant, Sleep, sleep}};

#[derive(Debug, Clone, Copy)]
enum Cmd {
    Terminate,
    Pause,
    Continue,
}

struct Ticker {
    timer: Option<Pin<Box<Sleep>>>,
    cmd: Pin<Box<UnboundedReceiver<Cmd>>>,
    period: Duration,
}

impl Ticker {
    fn new(period: Duration, cmd_channel: UnboundedReceiver<Cmd>) -> Self {
        Ticker {
            timer: Some(Box::pin(sleep(period))),
            cmd: Box::pin(cmd_channel),
            period,
        }
    }
    fn poll_tick(&mut self, ctx: &mut Context) -> Poll<Option<Instant>> {
        match Pin::new(&mut self.cmd).poll_recv(ctx) {
            Poll::Ready(Some(cmd)) => match cmd {
                Cmd::Terminate => return Poll::Ready(None),
                Cmd::Pause => {
                    self.timer.take();
                }
                Cmd::Continue => self.timer = Some(Box::pin(sleep(self.period))),
            },
            Poll::Ready(None) => {
                warn!("Channel should not be closed, rather use terminate message");
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }
        match self.timer.as_mut() {
            Some(timer) => {
                ready!(timer.as_mut().poll(ctx));
                let t = timer.deadline();
                let next_tick = t + *&self.period;
                timer.as_mut().reset(next_tick);
                Poll::Ready(Some(t))
            }
            None => Poll::Pending,
        }
    }

    async fn tick(&mut self) -> Option<Instant> {
        poll_fn(|cx| self.poll_tick(cx)).await
    }
}

pub struct Leaky {
    counter: Arc<AtomicUsize>,
    capacity: usize,
    sender: UnboundedSender<Cmd>,
}

impl Leaky {
    pub fn new(capacity: usize, rate: f32) -> Self {
        const KILO: f32 = 1_000.0;
        assert!(rate <= KILO, "Is not much usable beyond ms");
        assert!(capacity > 0);
        let interval_ms: u64 = (KILO / rate) as u64;
        let counter = Arc::new(AtomicUsize::new(0));
        let (sender, recipient) = unbounded_channel();
        let counter2 = counter.clone();
        let sender2 = sender.clone();
        let _t = tokio::spawn(async move {
            let period = Duration::from_millis(interval_ms);
            let mut ticker = Ticker::new(period, recipient);
            while let Some(_t) = ticker.tick().await {

                let v = counter2.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                    if v > 0 {
                        Some(v.saturating_sub(1))
                    } else {
                        None
                    }
                });
                trace!("Tick for old v {:?}", v);
                match v {
                    Ok(1) | Err(0) => {
                        trace!("Pausing leaking because empty");
                        sender2.send(Cmd::Pause).expect("ticker is not running");
                    }
                    _ => {}
                }
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
    use tokio::{
        sync::mpsc::unbounded_channel,
        time::{interval, sleep},
    };

    use crate::{Cmd, Leaky, Ticker};

    type Error = Box<dyn std::error::Error + Send + 'static>;
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        sleep(Duration::from_millis(30)).await;
        let res = leaky.start_one();
        if let Ok(n) = res {
            assert_eq!(n, 50, "should release one slot");
        } else {
            panic!("Slot was not released by leaky")
        }

        // wait bit more
        sleep(Duration::from_millis(300)).await;

        let res = leaky.start_one();
        if let Ok(n) = res {
            debug!("Taken slots after 300ms {}", n);
            assert!(n <= 50 - 14, "taken slots should decrease by at least 14");
        } else {
            panic!("Slot was not released by leaky")
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_leaky_pausing() {
        env_logger::try_init().ok();
        let leaky = Leaky::new(10, 50.0);

        macro_rules! tst {
            () => {

                for _i in 1..=10 {
                    assert!(leaky.start_one().is_ok());
                }
                //should be full now
                assert!(leaky.start_one().is_err());
                sleep(Duration::from_millis(300)).await;
                assert_eq!(leaky.immediate_capacity(), 10);

            }
        }
        
        tst!();

        sleep(Duration::from_millis(400)).await;
        // again

        tst!();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_ticker() {
        env_logger::try_init().ok();
        let (sender, receiver) = unbounded_channel();
        const PERIOD: u64 = 20;
        let mut ticker = Ticker::new(Duration::from_millis(PERIOD), receiver);
        let start = Instant::now();
        let mut count = 1;
        while let Some(t) = ticker.tick().await {
            let d = t.duration_since(start.into());
            let diff = d.as_secs_f64() * 1000.0 - (count * PERIOD) as f64;
            trace!("tick {} time {:?} diff {}", count, d, diff);
            if count <= 10 {
                assert!(
                    diff.abs() < 0.1,
                    "difference from theoretical start is less then 0.1 ms"
                );
            }

            if count == 10 {
                sender.send(Cmd::Pause).ok();
                let s2 = sender.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    s2.send(Cmd::Continue)
                });
            }

            if count == 20 {
                sender.send(Cmd::Terminate).ok();
            } else {
                count += 1;
            }
        }
        assert_eq!(count, 20);
    }
}
