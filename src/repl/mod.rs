use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub enum Msg {
    Digest {
        series: String,
        file_name: String,
        block_size: u32,
        blocks: Vec<(u32, u64)>,
    },
    Block {
        series: String,
        file_name: String,
        block: Vec<u8>,
    },
    Mismatch {
        offset: u64,
    },
    Sync,
}

pub struct Proto<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    pub stream: S,
}

impl<S> Proto<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    pub async fn write(&mut self, msg: &Msg) -> io::Result<()> {
        match msg {
            Msg::Digest {
                series,
                file_name,
                block_size,
                blocks,
            } => {
                self.stream.write_u8(0).await?;
                self.stream.write_u16(series.as_bytes().len() as u16).await?;
                self.stream.write_all(series.as_bytes()).await?;
                self.stream.write_u16(file_name.as_bytes().len() as u16).await?;
                self.stream.write_all(file_name.as_bytes()).await?;
                self.stream.write_u32(*block_size).await?;
                self.stream.write_u32(blocks.len() as u32).await?;
                for (size, crc) in blocks {
                    self.stream.write_u32(*size).await?;
                    self.stream.write_u64(*crc).await?;
                }
                Ok(())
            }
            Msg::Block { series, file_name, block } => {
                self.stream.write_u8(1).await?;
                self.stream.write_u16(series.as_bytes().len() as u16).await?;
                self.stream.write_all(series.as_bytes()).await?;
                self.stream.write_u16(file_name.as_bytes().len() as u16).await?;
                self.stream.write_all(file_name.as_bytes()).await?;
                self.stream.write_all(&block).await?;
                Ok(())
            }
            Msg::Mismatch { offset } => {
                self.stream.write_u8(2).await?;
                self.stream.write_u64(*offset).await?;
                Ok(())
            }
            Msg::Sync => {
                self.stream.write_u8(3).await?;
                Ok(())
            }
        }
    }
    async fn read_string(&mut self) -> io::Result<String> {
        let len = self.stream.read_u16().await?;
        let mut bytes = vec![0u8; len as usize];

        self.stream.read_exact(&mut bytes).await?;

        String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("can not read the string: {:?}", e)))
    }
    pub async fn read(&mut self) -> io::Result<Msg> {
        match self.stream.read_u8().await? {
            0 => Ok(Msg::Digest {
                series: self.read_string().await?,
                file_name: self.read_string().await?,
                block_size: self.stream.read_u32().await?,
                blocks: {
                    let len = self.stream.read_u32().await? as usize;
                    let mut blocks = Vec::new();
                    for _ in 0..len {
                        blocks.push((self.stream.read_u32().await?, self.stream.read_u64().await?));
                    }
                    blocks
                },
            }),
            1 => Ok(Msg::Block {
                series: self.read_string().await?,
                file_name: self.read_string().await?,
                block: {
                    let len = self.stream.read_u32().await?;
                    let mut block = vec![0u8; len as usize];

                    self.stream.read_exact(&mut block).await?;

                    block
                },
            }),
            2 => Ok(Msg::Mismatch {
                offset: self.stream.read_u64().await?,
            }),
            3 => Ok(Msg::Sync),
            _ => Err(io::Error::new(io::ErrorKind::Other, "got unexpected message")),
        }
    }
}