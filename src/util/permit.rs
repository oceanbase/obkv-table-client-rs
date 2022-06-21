// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use spin::Mutex;

use crate::error::{CommonErrCode, Error::Common as CommonErr, Result};

pub struct Permits {
    permits: Arc<Mutex<usize>>,
    max: usize,
}

pub struct PermitGuard(Arc<Mutex<usize>>, usize);

impl Permits {
    pub fn new(max: usize) -> Permits {
        Permits {
            permits: Arc::new(Mutex::new(0)),
            max,
        }
    }

    pub fn acquire(&self) -> Result<PermitGuard> {
        let mut p = self.permits.lock();

        if *p >= self.max {
            return Err(CommonErr(
                CommonErrCode::PermitDenied,
                format!("Fail to acquire a permit, max: {}", self.max),
            ));
        }

        *p += 1;

        Ok(PermitGuard(self.permits.clone(), *p))
    }
}

impl PermitGuard {
    #[inline]
    pub fn permit(&self) -> usize {
        self.1
    }
}

impl Drop for PermitGuard {
    #[inline]
    fn drop(&mut self) {
        *self.0.lock() -= 1;
    }
}

#[cfg(test)]

mod test {
    use std::thread;

    use super::*;

    #[test]
    fn test_permits() {
        let permits = Permits::new(3);

        let guard1 = permits.acquire().expect("Fail to acquire a permit.");
        assert_eq!(1, guard1.permit());
        let guard2 = permits.acquire().expect("Fail to acquire a permit.");
        assert_eq!(2, guard2.permit());
        let guard3 = permits.acquire().expect("Fail to acquire a permit.");
        assert_eq!(3, guard3.permit());

        assert!(!permits.acquire().is_ok());

        drop(guard1);
        let guard4 = permits.acquire();
        assert!(guard4.is_ok());
        let guard4 = guard4.unwrap();
        assert_eq!(3, guard4.permit());

        let guard5 = permits.acquire();
        assert!(!guard5.is_ok());
        assert_eq!(3, *permits.permits.lock());

        drop(guard2);
        drop(guard3);
        drop(guard4);
        assert_eq!(0, *permits.permits.lock());
    }

    #[test]
    fn test_acquire_concurrently() {
        let permits = Arc::new(Permits::new(3));

        let mut handles = Vec::with_capacity(10);
        for _ in 0..10 {
            let permits = permits.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    let g = permits.acquire();
                    if let Ok(g) = g {
                        assert!(g.permit() > 0);
                    }
                }
            }));
        }

        for h in handles {
            assert!(h.join().is_ok());
        }

        assert_eq!(0, *permits.permits.lock());
    }
}
