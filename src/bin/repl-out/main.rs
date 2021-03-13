use clap::clap_app;
use crc::crc64::checksum_iso;
use milliseriesdb::db;
use milliseriesdb::repl::{Msg, Proto};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpSocket;

struct ReplicatedFile {
    series: String,
    file_name: String,
    file: File,
    cur_offset: u64,
}

const BLOCK_SIZE: usize = 5 * 1024;

fn compute_digest(file: &mut File, length: u64) -> io::Result<(u32, Vec<(u32, u64)>)> {
    let mut buffer = [0u8; BLOCK_SIZE];
    let mut blocks = Vec::new();
    let mut remaining = length;

    while remaining > 0 {
        let size = remaining.min(BLOCK_SIZE as u64);

        file.read_exact(&mut buffer[0..size as usize])?;

        let checksum = checksum_iso(&buffer[..size as usize]);

        blocks.push((size as u32, checksum));

        remaining -= size;
    }

    Ok((BLOCK_SIZE as u32, blocks))
}

impl ReplicatedFile {
    async fn digest<S>(&mut self, proto: &mut Proto<S>, offset: u64) -> io::Result<()>
    where
        S: AsyncWrite + AsyncRead + Unpin,
    {
        log::debug!("bootstrapping {:?}/{:?}", self.series, self.file_name);

        let (block_size, blocks) = compute_digest(&mut self.file, offset)?;

        let digest_msg = Msg::Digest {
            series: self.series.clone(),
            file_name: self.file_name.clone(),
            block_size: block_size,
            blocks: blocks,
        };

        proto.write(&digest_msg).await?;

        match proto.read().await? {
            Msg::Mismatch { offset } => {
                log::debug!("    mismatch from offset {:?}", offset);
                self.cur_offset = offset;
            },
            Msg::Sync => {
                log::debug!("    replica in sync");
                self.cur_offset = offset;
            },
            _ => return Err(StrError("    unexpected response").into())
        }

        Ok(())
    }

    async fn replicate<S>(&mut self, proto: &mut Proto<S>, offset: u64) -> io::Result<()>
    where
        S: AsyncWrite + AsyncRead + Unpin,
    {
        if self.cur_offset >= offset || offset == 0 {
            return Ok(());
        }

        if self.cur_offset == 0 {
            self.digest(proto, offset).await?;
        }

        Ok(())
    }
}

struct StrError<'a>(&'a str);

impl<'a> Into<io::Error> for StrError<'a> {
    fn into(self) -> io::Error {
        io::Error::new(io::ErrorKind::Other, self.0)
    }
}

struct ReplicatedSeries {
    path: PathBuf,
    data: ReplicatedFile,
    index: ReplicatedFile,
    log: ReplicatedFile,
}

impl ReplicatedSeries {
    fn create<P: Into<PathBuf>>(path: P, series: String) -> io::Result<ReplicatedSeries> {
        let path = path.into();

        let logs = db::log::read_log_filenames(&path)?;

        if logs.is_empty() {
            return Err(StrError("can not find any log data").into());
        }

        let (log_path, log_seq) = &logs[0];

        Ok(ReplicatedSeries {
            path: path.clone(),
            data: ReplicatedFile {
                series: series.clone(),
                file_name: "series.dat".to_owned(),
                file: File::open(path.join("series.dat"))?,
                cur_offset: 0,
            },
            index: ReplicatedFile {
                series: series.clone(),
                file_name: "series.idx".to_owned(),
                file: File::open(path.join("series.idx"))?,
                cur_offset: 0,
            },
            log: ReplicatedFile {
                series: series.clone(),
                file_name: db::log::log_filename(*log_seq),
                file: File::open(log_path)?,
                cur_offset: 0,
            },
        })
    }

    async fn replicate<S>(&mut self, proto: &mut Proto<S>) -> io::Result<()>
    where
        S: AsyncWrite + AsyncRead + Unpin,
    {
        let log_entry = db::log::read_last_log_entry(&self.path)?.ok_or(io::Error::new(io::ErrorKind::Other, "no log entry found"))?;

        let logs = db::log::read_log_filenames(&self.path)?;

        if logs.is_empty() {
            return Err(StrError("can not find any log data").into());
        }

        let (log_path, log_seq) = &logs[0];

        let log_filename = db::log::log_filename(*log_seq);

        if log_filename != self.log.file_name {
            self.log = ReplicatedFile {
                series: self.log.series.clone(),
                file_name: log_filename,
                file: File::open(log_path)?,
                cur_offset: 0,
            }
        }

        self.data.replicate(proto, log_entry.data_offset).await?;
        self.index.replicate(proto, log_entry.data_offset).await?;
        self.log.replicate(proto, log_entry.data_offset).await
    }
}

struct ReplicatedDB {
    db_path: PathBuf,
    replicated_series: HashMap<String, ReplicatedSeries>,
}

impl ReplicatedDB {
    async fn replicate<S>(&mut self, proto: &mut Proto<S>) -> io::Result<()>
    where
        S: AsyncWrite + AsyncRead + Unpin,
    {
        for (series, path) in db::get_series_paths(&self.db_path)? {
            if !self.replicated_series.contains_key(&series) {
                match ReplicatedSeries::create(path, series.clone()) {
                    Ok(repl_series) => {
                        self.replicated_series.insert(series.clone(), repl_series);
                    }
                    Err(err) => log::warn!("can not replicate series: {:?}", err),
                }
            }
        }

        for repl_series in self.replicated_series.values_mut() {
            repl_series.replicate(proto).await?;
        }

        Ok(())
    }
}

struct ReplicaStream {
    replica_addr: String,
    db: ReplicatedDB,
}

impl ReplicaStream {
    async fn replicate(&mut self) -> io::Result<()> {
        log::debug!("replication run for {:?}", self.replica_addr);
        let socket = TcpSocket::new_v4()?;
        let mut proto = Proto {
            stream: socket.connect(self.replica_addr.parse().unwrap()).await?,
        };
        self.db.replicate(&mut proto).await
    }
    async fn run(&mut self) -> io::Result<()> {
        log::debug!("starting replica stream for db {:?} to {:?}", self.db.db_path, self.replica_addr);
        loop {
            let run_result = self.replicate().await;
            log::debug!("   run result: {:?}", run_result);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        Ok(())
    }
}

async fn start_replica_stream(db_path: &str, replica_addr: &str) -> io::Result<()> {
    let mut stream = ReplicaStream {
        replica_addr: replica_addr.to_owned(),
        db: ReplicatedDB {
            db_path: db_path.into(),
            replicated_series: HashMap::new(),
        },
    };

    stream.run().await
}

async fn run_check(db_path: &str) -> io::Result<()> {
    loop {
        for (series, path) in db::get_series_paths(&db_path.into())? {
            if let Some(entry) = db::log::read_last_log_entry(path)? {
                log::debug!("last entry of {:?}: {:?}", series, entry);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    stderrlog::new().module(module_path!()).verbosity(4).init().unwrap();
    let matches = clap_app!(milliseriesdb =>
        (@arg path: -p <PATH> --path "path to database")
        (@arg addr: -a <ADDR> --addr default_value("127.0.0.1:1234") "listen address")
        (@arg replica: -r <REPLICA> --replica ... #{1, 10} "replicas")
    )
    .get_matches();

    let db_path = matches.value_of("path").unwrap();

    let listen_addr = matches.value_of("addr").unwrap();

    let replicas: Vec<String> = matches.values_of("replica").unwrap().map(|s| s.to_owned()).collect();

    start_replica_stream(db_path, &replicas[0]).await
}
