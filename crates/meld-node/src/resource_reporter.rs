//! Collects the local resource values reported in heartbeats.

use meld_core::ResourceSnapshot;
use sysinfo::System;

pub struct ResourceReporter {
    system: System,
}

impl ResourceReporter {
    pub fn new() -> Self {
        Self {
            system: System::new_all(),
        }
    }

    pub fn total_memory(&self) -> u64 {
        self.system.total_memory()
    }

    pub fn snapshot(&mut self) -> ResourceSnapshot {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();

        ResourceSnapshot {
            cpu_usage_percent: self.system.global_cpu_usage().round().clamp(0.0, 100.0) as u8,
            available_memory_bytes: self.system.available_memory(),
            running_executions: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_values_respect_domain_bounds() {
        let mut reporter = ResourceReporter::new();

        let snapshot = reporter.snapshot();

        assert!(snapshot.cpu_usage_percent <= 100);
        assert!(snapshot.available_memory_bytes <= reporter.total_memory());
        assert_eq!(snapshot.running_executions, 0);
    }
}
