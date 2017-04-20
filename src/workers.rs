use std::io;
use std::process::Command;
use std::os::unix::process::CommandExt;
use libc::pid_t;

use nix::unistd::{fork, ForkResult};

pub struct Worker {
    id: u64,
    pid: pid_t
}

pub fn start_workers(count: usize) -> io::Result<Vec<Worker>> {

    (0..count as u64).map(|id| {
        start_worker(id)
    }).collect()
}

pub fn start_worker(id: u64) -> io::Result<Worker> {
    let path = weldr_path();

    match fork()? {
        ForkResult::Parent { child, .. } => {
            info!("Spawned worker id {} as child pid {}", id, child);

            return Ok(Worker {
                id: id,
                pid: child
            })
        }
        ForkResult::Child => {
            trace!("I am a new child");

            Command::new(&path)
                .arg("--worker")
                .arg(id.to_string())
                .exec();

            unreachable!();
            //process::exit(0);
        }
    }
}

fn weldr_path() -> String {
    "/Users/herman/projects/weldr/target/debug/weldr".to_owned()
}
