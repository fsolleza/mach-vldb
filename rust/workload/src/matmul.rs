use rand::prelude::*;
use std::{
    time::{Instant, Duration},
    thread,
};
use thread_priority::*;

struct Matrix<const D: usize>(Box<[[f64; D]; D]>);

impl<const D: usize> Matrix<D> {
    fn random() -> Self {
        let mut a: Self = Matrix(Box::new([[0.; D]; D]));
        let mut rng = thread_rng();
        for i in 0..D {
            for j in 0..D {
                a.0[i][j] = rng.gen();
            }
        }
        a
    }

    fn multiply(&self, other: &Matrix<D>) -> Self {
        let mut a: Self = Matrix(Box::new([[0.; D]; D]));
        for i in 0..D {
            for j in 0..D {
                for k in 0..D {
                    a.0[i][j] += self.0[i][k] * other.0[k][j];
                }
            }
        }
        a
    }

    fn sum(&self) -> f64 {
        let mut sum = 0.;
        for i in 0..D {
            for j in 0..D {
                sum += self.0[i][j];
            }
        }
        sum
    }
}

const N: usize = 512; // 512mb matrix
const THREADS: usize = 1;

fn main() {
    let mut handles = Vec::new();
    for i in 0..THREADS {
        let h = thread::spawn(move || {
            assert!(core_affinity::set_for_current(core_affinity::CoreId { id: 1 }));
            assert!(set_current_thread_priority(ThreadPriority::Max).is_ok());
            let a: Matrix<N> = Matrix::random();
            let b: Matrix<N> = Matrix::random();
            let mut print_a = true;
            let mut total = 0.;
            loop {
                let now = Instant::now();
                total = a.multiply(&b).sum();
                let dur = now.elapsed().as_secs_f64();
                println!("Total: {} {}", total, dur);
                thread::sleep(Duration::from_secs(10));
                println!("About to multiply");
                thread::sleep(Duration::from_secs(1));
            }
        });
        handles.push(h);
    }
    for h in handles {
        h.join();
    }
}

