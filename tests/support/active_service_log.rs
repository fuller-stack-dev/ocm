use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// A separate service stream that deliberately ignores Gateway quiescence.
pub struct ActiveServiceLog {
    path: PathBuf,
    stop: Arc<AtomicBool>,
    writer: Option<JoinHandle<()>>,
}

impl ActiveServiceLog {
    pub fn start(path: &Path) -> Self {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(path)
            .unwrap();
        file.write_all(b"diagnostic stream starts\n").unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let writer_stop = Arc::clone(&stop);
        let (ready_tx, ready_rx) = mpsc::sync_channel(0);
        let writer = thread::spawn(move || {
            file.write_all(b"independent service is running\n").unwrap();
            ready_tx.send(()).unwrap();
            while !writer_stop.load(Ordering::Relaxed) {
                file.write_all(b"independent node diagnostics continue during checkpoint\n")
                    .unwrap();
                thread::sleep(Duration::from_micros(100));
            }
        });
        ready_rx.recv().unwrap();
        Self {
            path: path.to_path_buf(),
            stop,
            writer: Some(writer),
        }
    }

    pub fn finish(mut self) -> Vec<u8> {
        self.stop.store(true, Ordering::Relaxed);
        self.writer.take().unwrap().join().unwrap();
        fs::read(&self.path).unwrap()
    }
}

impl Drop for ActiveServiceLog {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}
