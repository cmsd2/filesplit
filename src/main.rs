use clap::{Clap};
use std::path::{Path, PathBuf};
use std::fs;
use ubyte::{ByteUnit, ToByteUnit};
use num::Integer;
use std::io::{BufReader, BufWriter, Read, Write, IoSlice, IoSliceMut};
use circbuf::CircBuf;

static DEFAULT_CHUNK_SIZE: u64 = 104_857_600;
type Error = Box<dyn std::error::Error + 'static>;

fn parse_ubyte(src: &str) -> std::result::Result<ByteUnit, Error> {
    return Ok(src.parse().map_err(|e| format!("error parsing byte quantity: {}", e))?)
}

#[derive(Clap, Debug)]
struct Opts {
    #[clap(short, long)]
    pub file: PathBuf,

    #[clap(short, long)]
    pub chunks: Option<u64>,

    #[clap(short, long, parse(try_from_str = parse_ubyte))]
    pub size: Option<ByteUnit>,

    #[clap(short, long)]
    pub dest: Option<PathBuf>,
}

fn main() {
    let opts: Opts = Opts::parse();
    println!("opts: {:?}", opts);

    let dest = opts.dest.unwrap_or(PathBuf::from("."));
    if !dest.is_dir() {
        fs::create_dir_all(&dest).expect("create dest dir");
    }

    let metadata = fs::metadata(&opts.file).expect("stat file");
    let file_len = metadata.len();
    let mut chunk_size = opts.size.unwrap_or(DEFAULT_CHUNK_SIZE.bytes());

    if let Some(chunks) = opts.chunks {
        chunk_size = file_len.div_ceil(&chunks).bytes();
    }

    let chunks = opts.chunks.unwrap_or(file_len.div_ceil(&chunk_size.as_u64()));

    split(&opts.file, &dest, chunk_size.as_u64(), chunks).expect("split");
}

fn split(file: &Path, dest: &Path, size: u64, chunks: u64) -> std::result::Result<(), Error> {
    let file_name = file.file_name().ok_or_else(|| format!("no file name"))?;
    let width = (chunks as f64).log10().trunc() as usize + 1;

    let file_handle = fs::File::open(file).map_err(|e| format!("error opening input file: {:?}", e))?;
    let mut buf_reader = BufReader::new(file_handle);
    let mut buffer = CircBuf::with_capacity(1.megabytes().as_u64() as usize)?;
    
    for i in 0..chunks {
        let mut chunk_file_name = file_name.to_owned();
        chunk_file_name.push(format!(".{:01$}", i + 1, width));
        let chunk_file_path = dest.join(chunk_file_name);
        println!("copying chunk {}", i);
        buf_reader = create_chunk(buf_reader, &chunk_file_path, size, &mut buffer)?;
    }

    Ok(())
}

fn create_chunk<R: Read>(reader: R, chunk_file_path: &Path, size: u64, buffer: &mut CircBuf) -> std::result::Result<R, Error> {
    let chunk_file = fs::File::create(chunk_file_path).map_err(|e| format!("error opening chunk file: {:?}", e))?;

    let mut writer = BufWriter::new(chunk_file);

    let mut chunk_reader = reader.take(size);

    copy_bytes(&mut chunk_reader, &mut writer, buffer)?;

    Ok(chunk_reader.into_inner())
}

fn copy_bytes<R: Read,W: Write>(reader: &mut R, writer: &mut BufWriter<W>, buffer: &mut CircBuf) -> std::result::Result<(), Error> {
    loop {
        if !buffer.is_full() {
            let count = reader.read_v(buffer.get_avail())?;
            buffer.advance_write(count);

            if count == 0 {
                break;
            }
        }
        
        if !buffer.is_empty() {
            let count = writer.write_v(buffer.get_bytes())?;
            buffer.advance_read(count);
        }
    }

    while !buffer.is_empty() {
        let count = writer.write_v(buffer.get_bytes())?;
        buffer.advance_read(count);
    }

    Ok(())
}

trait ReadVectored<R: Read> {
    fn read_v(&mut self, bytes: [&mut [u8]; 2]) -> std::io::Result<usize>;
}

impl <R: Read> ReadVectored<R> for R {
    fn read_v(&mut self, mut bytes: [&mut [u8]; 2]) -> std::io::Result<usize> {
        let (a, b) = bytes.split_first_mut().unwrap();
        let first = IoSliceMut::new(a);
        let second = IoSliceMut::new(b[0]);
        self.read_vectored(&mut [first, second])
    }
}

trait WriteVectored<W: Write> {
    fn write_v(&mut self, bytes: [&[u8]; 2]) -> std::io::Result<usize>;
}

impl <W: Write> WriteVectored<W> for W {
    fn write_v(&mut self, bytes: [&[u8]; 2]) -> std::io::Result<usize> {
        let first = IoSlice::new(bytes[0]);
        let second = IoSlice::new(bytes[1]);
        self.write_vectored(&[first, second])
    }
}