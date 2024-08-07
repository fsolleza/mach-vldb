use rand::prelude::*;
use std::{
	thread,
	time::{Duration, Instant},
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
const CPU: usize = 6;

fn main() {
	println!("HERE");
	let mut idx = 0;
	let mut rng = thread_rng();
	set_core_affinity(CPU);
	let mut print_a = true;
	let mut total = 0.;
	loop {
		thread::sleep(Duration::from_secs(5));
		let e = Instant::now();
		let mut c = 0;
		while e.elapsed() < Duration::from_millis(100) {
			c += 1;
		}
		println!("Total: {}", c);
	}
}
