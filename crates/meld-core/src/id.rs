//! Strongly typed identifiers used across the Meld domain.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! define_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a new random identifier.
            pub fn generate() -> Self {
                Self(Uuid::new_v4())
            }

            /// Wraps an existing UUID without changing its value.
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// Returns the underlying UUID.
            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }
    };
}

define_id!(
    NodeId,
    "Uniquely identifies a physical node in the cluster."
);
define_id!(JobId, "Uniquely identifies a logical job.");
define_id!(
    ExecutionId,
    "Uniquely identifies one execution attempt of a job."
);
define_id!(
    MessageId,
    "Uniquely identifies one protocol message for correlation and deduplication."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_unique() {
        assert_ne!(NodeId::generate(), NodeId::generate());
    }

    #[test]
    fn id_can_be_parsed_from_its_display_value() {
        let id = JobId::generate();

        assert_eq!(id.to_string().parse::<JobId>(), Ok(id));
    }

    #[test]
    fn id_has_a_transparent_json_representation() {
        let id = ExecutionId::generate();

        let json = serde_json::to_string(&id).expect("ID should serialize");
        let deserialized =
            serde_json::from_str::<ExecutionId>(&json).expect("ID should deserialize");

        assert_eq!(deserialized, id);
        assert_eq!(json, format!("\"{id}\""));
    }
}
