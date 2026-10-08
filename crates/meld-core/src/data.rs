//! Declared job inputs and outputs, and the failures that moving them can cause.

use std::{collections::BTreeSet, error::Error, fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// Largest single input or output file a job may declare.
pub const MAX_FILE_BYTES: u64 = 1024 * 1024 * 1024;
/// Largest combined size of all input files of one job.
pub const MAX_JOB_INPUT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
/// Most input files, and separately most output files, of one job.
pub const MAX_FILES_PER_JOB: usize = 1000;
/// Longest accepted relative path, in bytes.
pub const MAX_PATH_BYTES: usize = 1024;

/// Hex-encoded SHA-256 digest that identifies the content of a file.
///
/// Always 64 lowercase hexadecimal characters, so equal content has exactly
/// one spelling and the digest can safely name a file on disk.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Sha256Digest(String);

impl Sha256Digest {
    const HEX_LEN: usize = 64;

    /// Builds a digest from raw hash output.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut hex = String::with_capacity(Self::HEX_LEN);
        for byte in bytes {
            hex.push(char::from(HEX[usize::from(byte >> 4)]));
            hex.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        Self(hex)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for Sha256Digest {
    type Err = InvalidDigest;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let valid = value.len() == Self::HEX_LEN
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if valid {
            Ok(Self(value.to_owned()))
        } else {
            Err(InvalidDigest)
        }
    }
}

impl TryFrom<String> for Sha256Digest {
    type Error = InvalidDigest;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Sha256Digest> for String {
    fn from(digest: Sha256Digest) -> Self {
        digest.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidDigest;

impl fmt::Display for InvalidDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("digest must be 64 lowercase hexadecimal characters")
    }
}

impl Error for InvalidDigest {}

/// A file the controller provides to the node before the process starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputFile {
    /// Destination inside the execution workspace, `/`-separated and relative.
    pub path: String,
    pub sha256: Sha256Digest,
    pub size_bytes: u64,
    /// Whether the file is made executable in the workspace.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub executable: bool,
}

/// A file the job promises to leave in its workspace for collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputSpec {
    /// Location inside the execution workspace, `/`-separated and relative.
    pub path: String,
}

/// A collected output as reported by the node after the process finished.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputFile {
    pub path: String,
    pub sha256: Sha256Digest,
    pub size_bytes: u64,
}

/// Everything a job declares about the files it reads and writes.
///
/// Only declared files move: a node receives exactly `inputs`, and only
/// `outputs` are collected from its workspace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<InputFile>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<OutputSpec>,
}

impl DataSpec {
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty() && self.outputs.is_empty()
    }

    /// Combined size of every input file.
    pub fn total_input_bytes(&self) -> u64 {
        self.inputs
            .iter()
            .fold(0, |total, input| total.saturating_add(input.size_bytes))
    }

    /// Rejects declarations that could escape the workspace or be ambiguous.
    ///
    /// Paths are compared ignoring ASCII case, because a node may run on a
    /// case-insensitive filesystem where `A` and `a` are the same file.
    pub fn validate(&self) -> Result<(), DataSpecError> {
        if self.inputs.len() > MAX_FILES_PER_JOB || self.outputs.len() > MAX_FILES_PER_JOB {
            return Err(DataSpecError::TooManyFiles {
                limit: MAX_FILES_PER_JOB,
            });
        }

        let mut claimed = Vec::with_capacity(self.inputs.len() + self.outputs.len());
        for (index, input) in self.inputs.iter().enumerate() {
            validate_relative_path(&input.path)
                .map_err(|reason| DataSpecError::InvalidInputPath { index, reason })?;
            if input.size_bytes > MAX_FILE_BYTES {
                return Err(DataSpecError::FileTooLarge {
                    index,
                    limit: MAX_FILE_BYTES,
                });
            }
            claimed.push(input.path.to_ascii_lowercase());
        }
        if self.total_input_bytes() > MAX_JOB_INPUT_BYTES {
            return Err(DataSpecError::InputsTooLarge {
                limit: MAX_JOB_INPUT_BYTES,
            });
        }
        for (index, output) in self.outputs.iter().enumerate() {
            validate_relative_path(&output.path)
                .map_err(|reason| DataSpecError::InvalidOutputPath { index, reason })?;
            claimed.push(output.path.to_ascii_lowercase());
        }

        let unique: BTreeSet<&str> = claimed.iter().map(String::as_str).collect();
        if unique.len() != claimed.len() {
            return Err(DataSpecError::DuplicatePath);
        }
        // A file cannot also be a directory that holds another declared file.
        let nested = unique.iter().any(|path| {
            unique.iter().any(|other| {
                other
                    .strip_prefix(path)
                    .is_some_and(|rest| rest.starts_with('/'))
            })
        });
        if nested {
            return Err(DataSpecError::NestedPath);
        }
        Ok(())
    }
}

/// Why a declared path cannot be used inside a workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidPath {
    Empty,
    TooLong,
    Absolute,
    /// Backslashes and drive prefixes mean different things per platform.
    PlatformSpecific,
    /// Empty, `.` or `..` components.
    UnsafeComponent,
    ControlCharacter,
}

impl fmt::Display for InvalidPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "is empty",
            Self::TooLong => "is too long",
            Self::Absolute => "must be relative",
            Self::PlatformSpecific => "must use `/` and no drive prefix",
            Self::UnsafeComponent => "must not contain empty, `.` or `..` components",
            Self::ControlCharacter => "must not contain control characters",
        })
    }
}

/// Checks that a path stays inside the workspace on every supported platform.
pub fn validate_relative_path(path: &str) -> Result<(), InvalidPath> {
    if path.is_empty() {
        return Err(InvalidPath::Empty);
    }
    if path.len() > MAX_PATH_BYTES {
        return Err(InvalidPath::TooLong);
    }
    if path.starts_with('/') {
        return Err(InvalidPath::Absolute);
    }
    if path.contains(['\\', ':']) {
        return Err(InvalidPath::PlatformSpecific);
    }
    if path.chars().any(char::is_control) {
        return Err(InvalidPath::ControlCharacter);
    }
    if path
        .split('/')
        .any(|component| matches!(component, "" | "." | ".."))
    {
        return Err(InvalidPath::UnsafeComponent);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSpecError {
    InvalidInputPath { index: usize, reason: InvalidPath },
    InvalidOutputPath { index: usize, reason: InvalidPath },
    DuplicatePath,
    NestedPath,
    TooManyFiles { limit: usize },
    FileTooLarge { index: usize, limit: u64 },
    InputsTooLarge { limit: u64 },
}

impl fmt::Display for DataSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInputPath { index, reason } => {
                write!(formatter, "input #{index} path {reason}")
            }
            Self::InvalidOutputPath { index, reason } => {
                write!(formatter, "output #{index} path {reason}")
            }
            Self::DuplicatePath => {
                formatter.write_str("input and output paths must be unique, ignoring case")
            }
            Self::NestedPath => formatter
                .write_str("a declared file must not also be a directory of another declared file"),
            Self::TooManyFiles { limit } => {
                write!(
                    formatter,
                    "a job may declare at most {limit} inputs and outputs"
                )
            }
            Self::FileTooLarge { index, limit } => {
                write!(
                    formatter,
                    "input #{index} exceeds the {limit} byte file limit"
                )
            }
            Self::InputsTooLarge { limit } => {
                write!(formatter, "inputs exceed the combined {limit} byte limit")
            }
        }
    }
}

impl Error for DataSpecError {}

impl DataFailure {
    /// Whether another attempt, possibly on another node, could succeed.
    ///
    /// A full disk, an unreachable controller or a failed upload say something
    /// about one node or one moment. A bad path, a size limit, a checksum
    /// mismatch or a missing output say something about the job and come back
    /// every time.
    pub const fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::LocalStorage { .. }
                | Self::InputUnavailable { .. }
                | Self::InsufficientDisk { .. }
                | Self::OutputUploadFailed { .. }
        )
    }
}

/// Why a node or the controller could not move a job's data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataFailure {
    /// A declared path is unsafe; the controller normally rejects these first.
    InvalidPath {
        path: String,
    },
    /// The node could not store the file in its cache or workspace.
    LocalStorage {
        path: String,
    },
    /// The content could not be fetched from the controller.
    InputUnavailable {
        path: String,
    },
    /// Received content did not match the declared digest or size.
    ChecksumMismatch {
        path: String,
    },
    TooLarge {
        path: String,
        limit_bytes: u64,
    },
    /// The node lacks the free space the inputs need.
    InsufficientDisk {
        needed_bytes: u64,
        available_bytes: u64,
    },
    /// The process succeeded but did not leave a declared output file.
    OutputMissing {
        path: String,
    },
    /// A declared output could not be read or sent to the controller.
    OutputUploadFailed {
        path: String,
    },
}

impl fmt::Display for DataFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath { path } => write!(formatter, "path `{path}` is not allowed"),
            Self::LocalStorage { path } => {
                write!(formatter, "node could not store `{path}` locally")
            }
            Self::InputUnavailable { path } => {
                write!(formatter, "input `{path}` could not be fetched")
            }
            Self::ChecksumMismatch { path } => {
                write!(
                    formatter,
                    "input `{path}` does not match its declared checksum"
                )
            }
            Self::TooLarge { path, limit_bytes } => {
                write!(formatter, "`{path}` exceeds the {limit_bytes} byte limit")
            }
            Self::InsufficientDisk {
                needed_bytes,
                available_bytes,
            } => write!(
                formatter,
                "inputs need {needed_bytes} bytes but only {available_bytes} are free"
            ),
            Self::OutputMissing { path } => {
                write!(formatter, "declared output `{path}` was not produced")
            }
            Self::OutputUploadFailed { path } => {
                write!(formatter, "output `{path}` could not be uploaded")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(fill: char) -> Sha256Digest {
        fill.to_string().repeat(64).parse().expect("valid digest")
    }

    fn input(path: &str) -> InputFile {
        InputFile {
            path: path.to_owned(),
            sha256: digest('a'),
            size_bytes: 10,
            executable: false,
        }
    }

    fn output(path: &str) -> OutputSpec {
        OutputSpec {
            path: path.to_owned(),
        }
    }

    #[test]
    fn digest_from_bytes_is_lowercase_hex() {
        let digest = Sha256Digest::from_bytes([0xab; 32]);

        assert_eq!(digest.as_str(), "ab".repeat(32));
        assert_eq!(digest.as_str().parse::<Sha256Digest>(), Ok(digest));
    }

    #[test]
    fn malformed_digests_are_rejected() {
        for value in [
            "",
            "abc",
            &"A".repeat(64),
            &"g".repeat(64),
            &"a".repeat(63),
            &"a".repeat(65),
        ] {
            assert_eq!(value.parse::<Sha256Digest>(), Err(InvalidDigest), "{value}");
        }
    }

    #[test]
    fn digest_is_validated_when_deserialized() {
        let valid = serde_json::from_str::<Sha256Digest>(&format!("\"{}\"", "b".repeat(64)));
        let invalid = serde_json::from_str::<Sha256Digest>("\"../../etc/passwd\"");

        assert_eq!(valid.expect("valid digest"), digest('b'));
        assert!(invalid.is_err());
    }

    #[test]
    fn executable_flag_defaults_to_false_and_is_omitted_unless_set() {
        let plain: InputFile = serde_json::from_str(&format!(
            r#"{{"path":"a","sha256":"{}","size_bytes":1}}"#,
            "a".repeat(64)
        ))
        .expect("older manifests should deserialize");
        let executable = InputFile {
            executable: true,
            ..plain.clone()
        };

        assert!(!plain.executable);
        assert!(
            !serde_json::to_string(&plain)
                .expect("serializes")
                .contains("executable")
        );
        assert!(
            serde_json::to_string(&executable)
                .expect("serializes")
                .contains(r#""executable":true"#)
        );
    }

    #[test]
    fn empty_data_spec_is_valid_and_omits_itself() {
        let spec = DataSpec::default();

        assert!(spec.is_empty());
        assert_eq!(spec.validate(), Ok(()));
        assert_eq!(serde_json::to_string(&spec).expect("serializes"), "{}");
    }

    #[test]
    fn well_formed_declarations_are_accepted() {
        let spec = DataSpec {
            inputs: vec![input("data.csv"), input("scripts/run.py")],
            outputs: vec![output("out/result.json")],
        };

        assert_eq!(spec.validate(), Ok(()));
        assert_eq!(spec.total_input_bytes(), 20);
    }

    #[test]
    fn paths_that_could_escape_the_workspace_are_rejected() {
        let cases = [
            ("", InvalidPath::Empty),
            ("/etc/passwd", InvalidPath::Absolute),
            ("../outside", InvalidPath::UnsafeComponent),
            ("a/../../b", InvalidPath::UnsafeComponent),
            ("a//b", InvalidPath::UnsafeComponent),
            ("./a", InvalidPath::UnsafeComponent),
            ("a/", InvalidPath::UnsafeComponent),
            ("a\\b", InvalidPath::PlatformSpecific),
            ("C:/temp", InvalidPath::PlatformSpecific),
            ("a\0b", InvalidPath::ControlCharacter),
        ];

        for (path, reason) in cases {
            let spec = DataSpec {
                inputs: vec![input(path)],
                outputs: vec![],
            };
            assert_eq!(
                spec.validate(),
                Err(DataSpecError::InvalidInputPath { index: 0, reason }),
                "{path:?}"
            );
        }
    }

    #[test]
    fn output_path_errors_name_the_output() {
        let spec = DataSpec {
            inputs: vec![],
            outputs: vec![output("ok"), output("../bad")],
        };

        assert_eq!(
            spec.validate(),
            Err(DataSpecError::InvalidOutputPath {
                index: 1,
                reason: InvalidPath::UnsafeComponent
            })
        );
    }

    #[test]
    fn overlong_path_is_rejected() {
        let spec = DataSpec {
            inputs: vec![input(&"a".repeat(MAX_PATH_BYTES + 1))],
            outputs: vec![],
        };

        assert_eq!(
            spec.validate(),
            Err(DataSpecError::InvalidInputPath {
                index: 0,
                reason: InvalidPath::TooLong
            })
        );
    }

    #[test]
    fn duplicate_paths_are_rejected_ignoring_case_and_across_kinds() {
        for spec in [
            DataSpec {
                inputs: vec![input("a.txt"), input("A.TXT")],
                outputs: vec![],
            },
            DataSpec {
                inputs: vec![],
                outputs: vec![output("r"), output("r")],
            },
            DataSpec {
                inputs: vec![input("same")],
                outputs: vec![output("same")],
            },
        ] {
            assert_eq!(spec.validate(), Err(DataSpecError::DuplicatePath));
        }
    }

    #[test]
    fn a_file_cannot_also_be_a_directory() {
        let spec = DataSpec {
            inputs: vec![input("data")],
            outputs: vec![output("data/result")],
        };

        assert_eq!(spec.validate(), Err(DataSpecError::NestedPath));
    }

    #[test]
    fn similar_prefixes_are_not_nesting() {
        let spec = DataSpec {
            inputs: vec![input("data"), input("data2"), input("data.txt")],
            outputs: vec![],
        };

        assert_eq!(spec.validate(), Ok(()));
    }

    #[test]
    fn size_and_count_limits_are_enforced() {
        let oversized = DataSpec {
            inputs: vec![InputFile {
                size_bytes: MAX_FILE_BYTES + 1,
                ..input("big")
            }],
            outputs: vec![],
        };
        assert_eq!(
            oversized.validate(),
            Err(DataSpecError::FileTooLarge {
                index: 0,
                limit: MAX_FILE_BYTES
            })
        );

        let too_large_together = DataSpec {
            inputs: (0..5)
                .map(|index| InputFile {
                    size_bytes: MAX_FILE_BYTES,
                    ..input(&format!("part{index}"))
                })
                .collect(),
            outputs: vec![],
        };
        assert_eq!(
            too_large_together.validate(),
            Err(DataSpecError::InputsTooLarge {
                limit: MAX_JOB_INPUT_BYTES
            })
        );

        let too_many = DataSpec {
            inputs: vec![],
            outputs: (0..=MAX_FILES_PER_JOB)
                .map(|index| output(&format!("o{index}")))
                .collect(),
        };
        assert_eq!(
            too_many.validate(),
            Err(DataSpecError::TooManyFiles {
                limit: MAX_FILES_PER_JOB
            })
        );
    }

    #[test]
    fn data_failure_round_trips_as_tagged_json() {
        let failure = DataFailure::InsufficientDisk {
            needed_bytes: 10,
            available_bytes: 3,
        };

        let json = serde_json::to_string(&failure).expect("serializes");

        assert!(json.contains(r#""type":"insufficient_disk""#));
        assert_eq!(
            serde_json::from_str::<DataFailure>(&json).expect("deserializes"),
            failure
        );
    }

    #[test]
    fn only_failures_that_may_pass_are_transient() {
        let path = || "p".to_owned();
        assert!(DataFailure::LocalStorage { path: path() }.is_transient());
        assert!(DataFailure::InputUnavailable { path: path() }.is_transient());
        assert!(
            DataFailure::InsufficientDisk {
                needed_bytes: 2,
                available_bytes: 1
            }
            .is_transient()
        );
        assert!(!DataFailure::InvalidPath { path: path() }.is_transient());
        assert!(!DataFailure::ChecksumMismatch { path: path() }.is_transient());
        assert!(
            !DataFailure::TooLarge {
                path: path(),
                limit_bytes: 1
            }
            .is_transient()
        );
        assert!(!DataFailure::OutputMissing { path: path() }.is_transient());
    }
}
