//! Local MP3, FLAC and WAV playback through Spotifast's fastframe audio output.

use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use fastframe_audio::{Maintained, Output, OutputOptions, Render};
use rodio::source::UniformSourceIterator;
use rodio::{Decoder, Source};

static COMMANDS: OnceLock<mpsc::Sender<Command>> = OnceLock::new();
static ACTIVE: AtomicBool = AtomicBool::new(false);

enum Command {
    Play(PathBuf),
    Toggle,
    Stop,
}

struct RenderState {
    receiver: Receiver<Vec<f32>>,
    current: VecDeque<f32>,
}
 
struct LocalRender {
    state: Arc<Mutex<RenderState>>,
}

impl Render for LocalRender {
    fn configure(&mut self, _sample_rate: u32, _channels: u16) {}

    fn render(&mut self, out: &mut [f32]) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut written = 0;
        while written < out.len() {
            if state.current.is_empty() {
                let next = state.receiver.try_recv();
                match next {
                    Ok(samples) => state.current.extend(samples),
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => {
                        out[written..].fill(0.0);
                        return;
                    }
                }
            }
            while written < out.len() {
                let Some(sample) = state.current.pop_front() else {
                    break;
                };
                out[written] = sample;
                written += 1;
            }
        }
    }
}

fn commands() -> &'static mpsc::Sender<Command> {
    COMMANDS.get_or_init(|| {
        let (sender, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("spotifast-local-audio".into())
            .spawn(move || run_player(receiver))
            .expect("unable to start local audio thread");
        sender
    })
}

/// Starts playing a local file. Supported formats are MP3, FLAC and WAV.
pub fn play(path: impl Into<PathBuf>) -> Result<()> {
    let file = LocalFile::from_path(path)?;
    // Fail before changing playback if the file cannot be opened or decoded.
    if is_wav(&file.path) {
        let _ = read_wav(&file.path)?;
    } else {
        let _ = file.open_decoder()?;
    }
    commands()
        .send(Command::Play(file.path))
        .context("local audio player has stopped")?;
    ACTIVE.store(true, Ordering::SeqCst);
    Ok(())
}

/// Toggles pause/resume when a local file is selected.
pub fn toggle() {
    if ACTIVE.load(Ordering::SeqCst) {
        let _ = commands().send(Command::Toggle);
    }
}

/// Stops local playback and returns the app to its normal Spotify controls.
pub fn stop() {
    if ACTIVE.swap(false, Ordering::SeqCst) {
        let _ = commands().send(Command::Stop);
    }
}

/// Whether local-file mode currently owns the play/pause button.
pub fn is_active() -> bool {
    ACTIVE.load(Ordering::SeqCst)
}

fn run_player(commands: Receiver<Command>) {
    let (_, sample_rx) = mpsc::sync_channel::<Vec<f32>>(8);
    let state = Arc::new(Mutex::new(RenderState {
        receiver: sample_rx,
        current: VecDeque::new(),
    }));
    let mut output: Option<Output<LocalRender>> = None;
    let mut paused = false;

    while let Ok(command) = commands.recv() {
        match command {
            Command::Play(path) => {
                let opened = if output.is_none() {
                    match Output::open(
                        OutputOptions::default(),
                        LocalRender {
                            state: Arc::clone(&state),
                        },
                    ) {
                        Ok(opened) => {
                            output = Some(opened);
                            true
                        }
                        Err(error) => {
                            log::error!("could not open audio output for local file: {error}");
                            ACTIVE.store(false, Ordering::SeqCst);
                            false
                        }
                    }
                } else {
                    true
                };
                if !opened {
                    continue;
                }

                let (sender, receiver) = mpsc::sync_channel::<Vec<f32>>(8);
                {
                    let mut render_state = state.lock().unwrap_or_else(|p| p.into_inner());
                    render_state.receiver = receiver;
                    render_state.current.clear();
                }
                let output_ref = output.as_mut().expect("output was just opened");
                output_ref.resume();
                paused = false;
                let sample_rate = output_ref.sample_rate();
                let channels = output_ref.channels().max(1);
                thread::Builder::new()
                    .name("spotifast-local-decoder".into())
                    .spawn(move || decode_file(path, sample_rate, channels, sender))
                    .expect("unable to start local audio decoder");
            }
            Command::Toggle => {
                if let Some(output) = &mut output {
                    if paused {
                        output.resume();
                    } else {
                        output.pause();
                    }
                    paused = !paused;
                }
            }
            Command::Stop => {
                if let Some(output) = &mut output {
                    output.pause();
                }
                paused = true;
                let (sender, receiver) = mpsc::sync_channel::<Vec<f32>>(1);
                let _ = sender.send(Vec::new());
                let mut render_state = state.lock().unwrap_or_else(|p| p.into_inner());
                render_state.receiver = receiver;
                render_state.current.clear();
            }
        }

        if let Some(output) = &mut output
            && let Maintained::Failed(error) = output.maintain()
        {
            log::error!("local audio output failed: {error}");
            ACTIVE.store(false, Ordering::SeqCst);
        }
    }

}

fn decode_file(path: PathBuf, sample_rate: u32, channels: u16, sender: SyncSender<Vec<f32>>) {
    let result = (|| -> Result<()> {
        if is_wav(&path) {
            let (source_channels, source_rate, samples) = read_wav(&path)?;
            let source = rodio::buffer::SamplesBuffer::new(source_channels, source_rate, samples);
            send_samples(UniformSourceIterator::new(source, channels, sample_rate), &sender)
        } else {
            let file = File::open(&path)
                .with_context(|| format!("Unable to open audio file '{}'", path.display()))?;
            let decoder = Decoder::try_from(BufReader::new(file))
                .with_context(|| format!("Unable to decode audio file '{}'", path.display()))?;
            send_samples(UniformSourceIterator::new(decoder, channels, sample_rate), &sender)
        }
    })();
    if let Err(error) = result {
        log::error!("local audio playback failed: {error:#}");
        ACTIVE.store(false, Ordering::SeqCst);
    }
}

fn send_samples<S: Iterator<Item = f32>>(source: S, sender: &SyncSender<Vec<f32>>) -> Result<()> {
    let mut chunk = Vec::with_capacity(4096);
    for sample in source {
        chunk.push(sample);
        if chunk.len() >= 4096 {
            if sender.send(std::mem::take(&mut chunk)).is_err() {
                return Ok(());
            }
            chunk = Vec::with_capacity(4096);
        }
    }
    if !chunk.is_empty() {
        let _ = sender.send(chunk);
    }
    Ok(())
}

fn is_wav(path: &Path) -> bool {
    path.extension().and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("wav"))
}

/// Reads common RIFF/WAVE PCM and IEEE-float files without adding a new
/// dependency to the locked build.
fn read_wav(path: &Path) -> Result<(u16, u32, Vec<f32>)> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = File::open(path)
        .with_context(|| format!("Unable to open WAV file '{}'", path.display()))?;
    let mut header = [0_u8; 12];
    file.read_exact(&mut header).context("Invalid WAV header")?;
    if &header[0..4] != b"RIFF" || &header[8..12] != b"WAVE" {
        bail!("'{}' is not a RIFF/WAVE file", path.display());
    }

    let mut format: Option<(u16, u16, u32, u16)> = None;
    let mut data: Option<(u64, u32)> = None;
    loop {
        let mut chunk_header = [0_u8; 8];
        match file.read_exact(&mut chunk_header) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error).context("Unable to read WAV chunks"),
        }
        let size = u32::from_le_bytes(chunk_header[4..8].try_into().unwrap());
        let offset = file.stream_position()?;
        match &chunk_header[0..4] {
            b"fmt " => {
                if size < 16 {
                    bail!("Invalid WAV format chunk");
                }
                let mut fmt = [0_u8; 16];
                file.read_exact(&mut fmt)?;
                format = Some((
                    u16::from_le_bytes([fmt[0], fmt[1]]),
                    u16::from_le_bytes([fmt[2], fmt[3]]),
                    u32::from_le_bytes(fmt[4..8].try_into().unwrap()),
                    u16::from_le_bytes([fmt[14], fmt[15]]),
                ));
            }
            b"data" => data = Some((offset, size)),
            _ => {}
        }
        file.seek(SeekFrom::Start(offset + u64::from(size) + u64::from(size % 2)))?;
        if format.is_some() && data.is_some() {
            break;
        }
    }

    let (encoding, channels, sample_rate, bits) =
        format.ok_or_else(|| anyhow::anyhow!("WAV format chunk is missing"))?;
    let (data_offset, data_size) =
        data.ok_or_else(|| anyhow::anyhow!("WAV data chunk is missing"))?;
    if channels == 0 || sample_rate == 0 {
        bail!("Invalid WAV channel count or sample rate");
    }
    let bytes_per_sample = usize::from(bits).div_ceil(8);
    if !matches!((encoding, bits), (1, 8 | 16 | 24 | 32) | (3, 32 | 64)) {
        bail!("Unsupported WAV encoding: format {encoding}, {bits} bits");
    }
    let frame_bytes = bytes_per_sample * usize::from(channels);
    if frame_bytes == 0 || data_size as usize % frame_bytes != 0 {
        bail!("WAV data is not aligned to complete audio frames");
    }
    let mut bytes = vec![0_u8; data_size as usize];
    file.seek(SeekFrom::Start(data_offset))?;
    file.read_exact(&mut bytes).context("Truncated WAV audio data")?;
    let mut samples = Vec::with_capacity(bytes.len() / bytes_per_sample);
    for sample in bytes.chunks_exact(bytes_per_sample) {
        let value = match (encoding, bits) {
            (1, 8) => (f32::from(sample[0]) - 128.0) / 128.0,
            (1, 16) => i16::from_le_bytes([sample[0], sample[1]]) as f32 / 32768.0,
            (1, 24) => {
                let raw = i32::from(sample[0])
                    | (i32::from(sample[1]) << 8)
                    | (i32::from(sample[2]) << 16);
                let signed = (raw << 8) >> 8;
                signed as f32 / 8_388_608.0
            }
            (1, 32) => i32::from_le_bytes(sample.try_into().unwrap()) as f32 / 2_147_483_648.0,
            (3, 32) => f32::from_le_bytes(sample.try_into().unwrap()),
            (3, 64) => f64::from_le_bytes(sample.try_into().unwrap()) as f32,
            _ => unreachable!("WAV encoding was checked above"),
        };
        samples.push(value);
    }
    Ok((channels, sample_rate, samples))
}

/// Basic information available before a local file is handed to the player.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFile {
    /// Absolute path to the selected file.
    pub path: PathBuf,
    /// A readable title derived from the filename.
    pub title: String,
}

impl LocalFile {
    /// Describes a supported file without decoding its audio.
    pub fn from_path(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if !is_supported(&path) {
            bail!(
                "Unsupported audio file '{}'. Choose an MP3, FLAC, or WAV file.",
                path.display()
            );
        }
        let title = path.file_stem().and_then(|name| name.to_str())
            .filter(|name| !name.trim().is_empty()).unwrap_or("Local audio").to_owned();
        Ok(Self { path, title })
    }

    /// Opens and decodes the selected file.
    pub fn open_decoder(&self) -> Result<Decoder<BufReader<File>>> {
        let file = File::open(&self.path)
            .with_context(|| format!("Unable to open audio file '{}'", self.path.display()))?;
        Decoder::try_from(BufReader::new(file))
            .with_context(|| format!("Unable to decode audio file '{}'", self.path.display()))
    }

    /// Duration reported by the decoder, if the container provides it.
    pub fn duration(&self) -> Result<Option<Duration>> {
        if is_wav(&self.path) {
            let (channels, sample_rate, samples) = read_wav(&self.path)?;
            return Ok(Some(Duration::from_secs_f64(
                samples.len() as f64 / f64::from(channels) / f64::from(sample_rate),
            )));
        }
        Ok(self.open_decoder()?.total_duration())
    }
}

/// Whether a filename uses one of the formats compiled into Spotifast.
pub fn is_supported(path: &Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()).is_some_and(|extension| {
        ["mp3", "flac", "wav"].iter().any(|supported| extension.eq_ignore_ascii_case(supported))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_supported_extensions_case_insensitively() {
        for name in ["song.mp3", "song.MP3", "song.flac", "song.FLAC", "song.wav", "song.Wav"] {
            assert!(is_supported(Path::new(name)), "{name}");
        }
    }

    #[test]
    fn rejects_uncompiled_formats() {
        for name in ["song.ogg", "song.m4a", "song.txt", "song"] {
            assert!(!is_supported(Path::new(name)), "{name}");
        }
    }

    #[test]
    fn derives_title_from_filename() {
        let file = LocalFile::from_path("music/My Song.MP3").unwrap();
        assert_eq!(file.title, "My Song");
    }

    #[test]
    fn refuses_unsupported_files_before_opening_them() {
        let error = LocalFile::from_path("does-not-exist.txt").unwrap_err();
        assert!(error.to_string().contains("Unsupported audio file"));
    }
}
