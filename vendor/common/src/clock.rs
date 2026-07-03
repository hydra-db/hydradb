use std::ops::Add;
use std::{
    sync::RwLock,
    time::{Duration, SystemTime},
};

pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[derive(Debug)]
pub struct MockClock {
    now: RwLock<SystemTime>,
}

impl Clock for MockClock {
    fn now(&self) -> SystemTime {
        *self.now.read().unwrap()
    }
}

impl Default for MockClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MockClock {
    pub fn with_time(time: SystemTime) -> Self {
        Self {
            now: RwLock::new(time),
        }
    }

    pub fn new() -> Self {
        Self::with_time(SystemTime::now())
    }

    pub fn advance(&self, duration: Duration) {
        let mut now = self.now.write().unwrap();
        *now = now.add(duration);
    }

    pub fn set_time(&self, time: SystemTime) {
        *self.now.write().unwrap() = time;
    }
}
