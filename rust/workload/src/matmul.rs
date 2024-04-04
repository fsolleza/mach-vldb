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

fn set_core_affinity(cpu: usize) {
    use core_affinity::{set_for_current, CoreId};
    assert!(set_for_current(CoreId { id: cpu }));
}

fn random_core_affinity<const T: usize>() -> usize {
    let mut rng = thread_rng();
    let cpu = rng.gen::<usize>() % T;
    set_core_affinity(cpu);
    cpu
}

const N: usize = 768; // 512mb matrix
const THREADS: usize = 1;
const THREAD_CHOICE: [usize; 2] = [1, 3];

fn main() {
    let mut handles = Vec::new();
    for i in 0..THREADS {
        let h = thread::spawn(move || {
            let mut rng = thread_rng();
            set_core_affinity(THREAD_CHOICE[rng.gen_range(0usize..2)]);
            assert!(set_current_thread_priority(ThreadPriority::Max).is_ok());
            let a: Matrix<N> = Matrix::random();
            let b: Matrix<N> = Matrix::random();
            let mut print_a = true;
            let mut total = 0.;
            loop {
                let idx = rng.gen_range(0usize..THREAD_CHOICE.len());
                let cpu = THREAD_CHOICE[idx] as usize;
                set_core_affinity(THREAD_CHOICE[idx] as usize);
                let now = Instant::now();
                total = a.multiply(&b).sum();
                let dur = now.elapsed().as_secs_f64();
                println!("Total: {}, cpu: {}, duration: {}", total, cpu, dur);
                let secs: u64 = rng.gen_range(20u64..30);
                thread::sleep(Duration::from_secs(secs));
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

