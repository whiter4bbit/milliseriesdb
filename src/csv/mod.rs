use crate::storage::Entry;
use bytes::buf::Buf;

pub fn read_csv_line(line: &str) -> Option<Entry> {
    let mut split = line.split(';');

    match (split.next(), split.next()) {
        (Some(ts), Some(value)) => {
            let ts = ts.trim().parse::<u64>().ok()?;
            let value = value.trim().parse::<f64>().ok()?;
            Some(Entry { ts, value })
        }
        _ => None,
    }
}

pub fn to_csv(entries: &[Entry]) -> String {
    entries
        .iter()
        .map(|entry| format!("{}; {:.2}\n", entry.ts, entry.value))
        .collect::<Vec<String>>()
        .join("")
}

pub struct ChunkedReader {
    buf: Vec<u8>,
}

impl ChunkedReader {
    pub fn new() -> ChunkedReader {
        ChunkedReader { buf: Vec::new() }
    }

    pub fn read<B: Buf>(&mut self, chunk: B) -> Chunk<B> {
        Chunk {
            chunk: chunk,
            buf: &mut self.buf,
        }
    }
}

pub struct Chunk<'a, B: Buf> {
    chunk: B,
    buf: &'a mut Vec<u8>,
}

impl<'a, B> Iterator for Chunk<'a, B>
where
    B: Buf,
{
    type Item = Result<Entry, ()>;
    fn next(&mut self) -> Option<Self::Item> {
        while self.chunk.has_remaining() {
            let c = self.chunk.get_u8();
            self.buf.push(c);

            if c == b'\n' {
                let line = std::str::from_utf8(&self.buf).ok();

                let entry = Some(
                    line.and_then(|line| read_csv_line(&line))
                        .map(Ok)
                        .unwrap_or_else(|| Err(())),
                );

                self.buf.clear();

                return entry;
            }
        }
        None
    }
}
