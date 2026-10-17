//! Local audio-file decoding primitives.
//!
//! This module deliberately keeps file decoding separate from Spotify's
//! librespot decoder. The application can use the same rodio mixer for local
//! sources without treating a filesystem path as a Spotify URI.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rodio::{Decoder, Source};

/// Basic information available before a local file is handed to the player.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFile {
    /// Absolute path to the selected file.
    pub path: PathBuf,
    /// A readable title derived from the filename.
    pub title: String,
}

impl LocalFile {
    /// Describes a supported file without opening or decoding its audio.
    pub fn from_path(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if !is_supported(&path) {
            bail!(
                "Unsupported audio file '{}'. Choose an MP3, FLAC, or WAV file.",
                path.display()
            );
        }

        let title = path
            .file_stem()
            .and_then(|name| name.to_str())
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("Local audio")
            .to_owned();

        Ok(Self { path, title })
    }

    /// Opens and decodes the selected file.
    ///
    /// The returned source yields interleaved floating-point samples and can
    /// be appended to the application's existing rodio mixer.
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

/// Whether the filename uses one of the formats compiled into Spotifast.
pub fn is_supported(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            ["mp3", "flac", "wav"]
                .iter()
                .any(|supported| extension.eq_ignore_ascii_case(supported))
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
