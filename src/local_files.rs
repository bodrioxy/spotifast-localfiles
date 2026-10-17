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
    channels: usize,
}

struct LocalRender {
    state: Arc<Mutex<RenderState>>,
}

impl Render for LocalRender {
    fn configure(&mut self, _sample_rate: u32, channels: u16) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.channels = usize::from(channels.max(1));
    }

    fn render(&mut self, out: &mut [f32]) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut written = 0;
        while written < out.len() {
            if state.current.is_empty() {
                match state.receiver.try_recv() {
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
    let _ = file.open_decoder()?;
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
    let (samples, sample_rx) = mpsc::sync_channel::<Vec<f32>>(8);
    let state = Arc::new(Mutex::new(RenderState {
        receiver: sample_rx,
        current: VecDeque::new(),
        channels: 2,
    }));
    let renderer = LocalRender { state: Arc::clone(&state) };
    let mut output: Option<Output<LocalRender>> = None;
    let mut paused = false;

    while let Ok(command) = commands.recv() {
        match command {
            Command::Play(path) => {
                let opened = if output.is_none() {
                    match Output::open(OutputOptions::default(), LocalRender { state: Arc::clone(&state) }) {
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
                let channels = usize::from(output_ref.channels().max(1));
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

        if let Some(output) = &mut output {
            match output.maintain() {
                Maintained::Failed(error) => {
                    log::error!("local audio output failed: {error}");
                    ACTIVE.store(false, Ordering::SeqCst);
                }
                _ => {}
            }
        }
    }

    let _ = samples;
}

fn decode_file(path: PathBuf, sample_rate: u32, channels: usize, sender: SyncSender<Vec<f32>>) {
    let result = (|| -> Result<()> {
        let file = File::open(&path)
            .with_context(|| format!("Unable to open audio file '{}'", path.display()))?;
        let decoder = Decoder::try_from(BufReader::new(file))
            .with_context(|| format!("Unable to decode audio file '{}'", path.display()))?;
        let source = UniformSourceIterator::new(decoder, channels, sample_rate);
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
    })();
    if let Err(error) = result {
        log::error!("local audio playback failed: {error:#}");
        ACTIVE.store(false, Ordering::SeqCst);
    }
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
