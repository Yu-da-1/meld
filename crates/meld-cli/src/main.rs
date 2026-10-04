mod controller_client;

use std::{
    error::Error,
    io::{self, Write},
};

use clap::{Args, Parser, Subcommand};
use meld_core::{
    ExecutionState, JobId, JobSpec, JobState, LimitedResource, NodeId, NodeState,
    NodeStateResponse, NodeVerdict, PlacementConstraints, QueueReason, ResourceRequirements,
};

use crate::controller_client::ControllerClient;

const CONTROLLER_URL_ENV: &str = "MELD_CONTROLLER_URL";
const DEFAULT_CONTROLLER_URL: &str = "http://127.0.0.1:3000";
const DEFAULT_MEMORY: &str = "256MiB";

#[derive(Debug, Parser)]
#[command(name = "meld", version, about = "Submit and inspect Meld jobs")]
struct Cli {
    /// Base URL of the Meld controller.
    #[arg(
        long,
        global = true,
        env = CONTROLLER_URL_ENV,
        default_value = DEFAULT_CONTROLLER_URL
    )]
    controller: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Args)]
struct RunArgs {
    /// Minimum logical CPUs required by the job.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    cpu: u32,

    /// Minimum memory required (for example: 512MiB or 8GB).
    #[arg(long, default_value = DEFAULT_MEMORY, value_parser = parse_memory)]
    memory: u64,

    /// Maximum process execution time in seconds.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    timeout: Option<u64>,

    /// Maximum time from submission until job completion in seconds.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    job_timeout: Option<u64>,

    /// Only run on nodes with this operating system (for example: linux).
    #[arg(long = "os", value_name = "OS")]
    operating_system: Option<String>,

    /// Only run on nodes with this CPU architecture (for example: aarch64).
    #[arg(long = "arch", value_name = "ARCH")]
    architecture: Option<String>,

    /// Only run on nodes advertising this capability. Repeat for several.
    #[arg(long = "require", value_name = "CAPABILITY")]
    capabilities: Vec<String>,

    /// Program and arguments. Place these after `--`.
    #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
    command: Vec<String>,
}

impl RunArgs {
    fn into_job_spec(self) -> JobSpec {
        let mut command = self.command.into_iter();
        let program = command
            .next()
            .expect("clap requires at least one command argument");
        JobSpec {
            program,
            args: command.collect(),
            requirements: ResourceRequirements {
                logical_cpus: self.cpu,
                memory_bytes: self.memory,
            },
            job_timeout_secs: self.job_timeout,
            execution_timeout_secs: self.timeout,
            constraints: PlacementConstraints {
                operating_system: self.operating_system,
                architecture: self.architecture,
                capabilities: self.capabilities,
            },
        }
    }
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Submit a command for remote execution.
    Run(RunArgs),

    /// Show the current state of one job.
    Status { job_id: JobId },

    /// Print captured stdout and stderr for one job.
    Logs { job_id: JobId },

    /// Request cancellation of one job.
    Cancel { job_id: JobId },

    /// List nodes with their state, capacity, and latest usage.
    Nodes,

    /// Stop placing new jobs on a node; running jobs finish normally.
    Drain { node_id: NodeId },

    /// Make a drained node eligible for new jobs again.
    Resume { node_id: NodeId },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let client = ControllerClient::new(&cli.controller)?;

    match cli.command {
        Commands::Run(args) => submit_job(&client, &args.into_job_spec()).await?,
        Commands::Status { job_id } => show_status(&client, job_id).await?,
        Commands::Logs { job_id } => show_logs(&client, job_id).await?,
        Commands::Cancel { job_id } => cancel_job(&client, job_id).await?,
        Commands::Nodes => show_nodes(&client).await?,
        Commands::Drain { node_id } => print_node_state(client.drain(node_id).await?),
        Commands::Resume { node_id } => print_node_state(client.resume(node_id).await?),
    }

    Ok(())
}

async fn submit_job(client: &ControllerClient, spec: &JobSpec) -> Result<(), Box<dyn Error>> {
    let response = client.submit(spec).await?;

    println!("job_id: {}", response.job_id);
    println!("state: {}", job_state_name(response.state));
    Ok(())
}

async fn show_status(client: &ControllerClient, job_id: JobId) -> Result<(), Box<dyn Error>> {
    let response = client.status(job_id).await?;

    println!("job_id: {}", response.job_id);
    println!("state: {}", job_state_name(response.state));
    println!("program: {}", response.spec.program);
    let constraints = &response.spec.constraints;
    if let Some(operating_system) = &constraints.operating_system {
        println!("require_os: {operating_system}");
    }
    if let Some(architecture) = &constraints.architecture {
        println!("require_arch: {architecture}");
    }
    if !constraints.capabilities.is_empty() {
        println!(
            "require_capabilities: {}",
            constraints.capabilities.join(", ")
        );
    }
    if let Some(reason) = response.queue_reason {
        println!("queue_reason: {}", queue_reason_name(reason));
    }
    if !response.placement.is_empty() {
        println!("placement:");
        for assessment in &response.placement {
            println!(
                "  {} ({}): {}",
                assessment.node_id,
                assessment.hostname,
                describe_verdict(assessment.verdict)
            );
        }
    }
    if let Some(execution) = response.execution {
        println!("execution_id: {}", execution.execution_id);
        println!("node_id: {}", execution.node_id);
        println!("execution_state: {}", execution_state_name(execution.state));
        if let Some(result) = execution.result {
            match result.exit_code {
                Some(exit_code) => println!("exit_code: {exit_code}"),
                None => println!("exit_code: unavailable"),
            }
        }
    }
    Ok(())
}

async fn show_logs(client: &ControllerClient, job_id: JobId) -> Result<(), Box<dyn Error>> {
    let response = client.logs(job_id).await?;
    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();

    stdout.write_all(response.output.stdout.content.as_bytes())?;
    stderr.write_all(response.output.stderr.content.as_bytes())?;
    if response.output.stdout.truncated {
        writeln!(stderr, "warning: captured stdout was truncated")?;
    }
    if response.output.stderr.truncated {
        writeln!(stderr, "warning: captured stderr was truncated")?;
    }
    if response.output.stdout.lossy || response.output.stderr.lossy {
        writeln!(
            stderr,
            "warning: invalid UTF-8 in captured output was replaced"
        )?;
    }
    Ok(())
}

async fn cancel_job(client: &ControllerClient, job_id: JobId) -> Result<(), Box<dyn Error>> {
    let response = client.cancel(job_id).await?;

    println!("job_id: {}", response.job_id);
    println!("state: {}", job_state_name(response.state));
    Ok(())
}

async fn show_nodes(client: &ControllerClient) -> Result<(), Box<dyn Error>> {
    let response = client.nodes().await?;
    if response.nodes.is_empty() {
        println!("no nodes registered");
    }

    for (index, node) in response.nodes.iter().enumerate() {
        if index > 0 {
            println!();
        }
        let capacity = node.descriptor.capacity;
        println!("node_id: {}", node.descriptor.id);
        println!("hostname: {}", node.descriptor.hostname);
        println!("state: {}", node_state_name(node.state));
        println!(
            "platform: {}/{}",
            node.descriptor.operating_system, node.descriptor.architecture
        );
        println!(
            "capacity: {} cpus, {} bytes memory, up to {} concurrent executions",
            capacity.logical_cpus, capacity.memory_bytes, capacity.max_concurrent_executions
        );
        if !node.descriptor.capabilities.is_empty() {
            println!("capabilities: {}", node.descriptor.capabilities.join(", "));
        }
        if let Some(snapshot) = node.snapshot {
            println!(
                "usage: {}% cpu, {} bytes memory available, {} executions running",
                snapshot.cpu_usage_percent,
                snapshot.available_memory_bytes,
                snapshot.running_executions
            );
        }
        if let Some(age_ms) = node.last_heartbeat_age_ms {
            println!("last_heartbeat_ms_ago: {age_ms}");
        }
    }
    Ok(())
}

fn print_node_state(response: NodeStateResponse) {
    println!("node_id: {}", response.node_id);
    println!("state: {}", node_state_name(response.state));
}

const fn node_state_name(state: NodeState) -> &'static str {
    match state {
        NodeState::Joining => "joining",
        NodeState::Ready => "ready",
        NodeState::Draining => "draining",
        NodeState::Unreachable => "unreachable",
    }
}

fn parse_memory(value: &str) -> Result<u64, String> {
    let value = value.trim();
    let unit_start = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (amount, unit) = value.split_at(unit_start);
    if amount.is_empty() {
        return Err("memory must start with a positive integer".to_owned());
    }

    let amount = amount
        .parse::<u64>()
        .map_err(|error| format!("invalid memory amount: {error}"))?;
    let multiplier = match unit.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "K" | "KB" => 1_000,
        "M" | "MB" => 1_000_000,
        "G" | "GB" => 1_000_000_000,
        "KIB" => 1_024,
        "MIB" => 1_048_576,
        "GIB" => 1_073_741_824,
        _ => {
            return Err("unsupported memory unit; use B, KB, MB, GB, KiB, MiB, or GiB".to_owned());
        }
    };
    let bytes = amount
        .checked_mul(multiplier)
        .ok_or_else(|| "memory size is too large".to_owned())?;
    if bytes == 0 {
        return Err("memory must be greater than zero".to_owned());
    }
    Ok(bytes)
}

const fn job_state_name(state: JobState) -> &'static str {
    match state {
        JobState::Submitted => "submitted",
        JobState::Queued => "queued",
        JobState::Assigned => "assigned",
        JobState::Running => "running",
        JobState::Cancelling => "cancelling",
        JobState::TimingOut => "timing_out",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Cancelled => "cancelled",
        JobState::TimedOut => "timed_out",
        JobState::Lost => "lost",
    }
}

fn describe_verdict(verdict: NodeVerdict) -> String {
    match verdict {
        NodeVerdict::Selected { load_permille } => format!(
            "selected: lowest load ({} after placement)",
            format_permille(load_permille)
        ),
        NodeVerdict::Eligible { load_permille } => format!(
            "eligible, not chosen: another node was preferred ({} after placement here)",
            format_permille(load_permille)
        ),
        NodeVerdict::NotReady { state } => {
            format!("not accepting jobs ({})", node_state_name(state))
        }
        NodeVerdict::ConstraintsNotSatisfied => {
            "does not satisfy the placement constraints".to_owned()
        }
        NodeVerdict::InsufficientCapacity => {
            "total capacity is smaller than the request".to_owned()
        }
        NodeVerdict::NoFreeCapacity { resource } => format!(
            "no free {} right now",
            match resource {
                LimitedResource::ConcurrentExecutions => "execution slots",
                LimitedResource::Cpu => "CPU",
                LimitedResource::Memory => "memory",
            }
        ),
    }
}

fn format_permille(permille: u32) -> String {
    format!("{}.{}%", permille / 10, permille % 10)
}

const fn queue_reason_name(reason: QueueReason) -> &'static str {
    match reason {
        QueueReason::NoReadyNodes => "no_ready_nodes",
        QueueReason::ConstraintsNotSatisfied => "constraints_not_satisfied",
        QueueReason::InsufficientResources => "insufficient_resources",
        QueueReason::NoAvailableNodes => "no_available_nodes",
        QueueReason::WaitingForEarlierJob => "waiting_for_earlier_job",
        QueueReason::AwaitingAssignment => "awaiting_assignment",
    }
}

const fn execution_state_name(state: ExecutionState) -> &'static str {
    match state {
        ExecutionState::Assigned => "assigned",
        ExecutionState::Accepted => "accepted",
        ExecutionState::Running => "running",
        ExecutionState::Cancelling => "cancelling",
        ExecutionState::Succeeded => "succeeded",
        ExecutionState::Failed => "failed",
        ExecutionState::Cancelled => "cancelled",
        ExecutionState::TimedOut => "timed_out",
        ExecutionState::Rejected => "rejected",
        ExecutionState::Lost => "lost",
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[test]
    fn run_preserves_program_argument_boundaries() {
        let cli = Cli::try_parse_from([
            "meld",
            "run",
            "--cpu",
            "4",
            "--memory",
            "2GiB",
            "--timeout",
            "60",
            "--job-timeout",
            "120",
            "--",
            "cargo",
            "build",
            "--release",
        ])
        .expect("valid run command should parse");

        let Commands::Run(args) = cli.command else {
            panic!("run subcommand should be selected");
        };
        let spec = args.into_job_spec();
        assert_eq!(spec.requirements.logical_cpus, 4);
        assert_eq!(spec.requirements.memory_bytes, 2 * 1_073_741_824);
        assert_eq!(spec.execution_timeout_secs, Some(60));
        assert_eq!(spec.job_timeout_secs, Some(120));
        assert_eq!(spec.program, "cargo");
        assert_eq!(spec.args, ["build", "--release"]);
        assert!(spec.constraints.is_empty());
    }

    #[test]
    fn run_collects_placement_constraints() {
        let cli = Cli::try_parse_from([
            "meld",
            "run",
            "--os",
            "linux",
            "--arch",
            "aarch64",
            "--require",
            "gpu",
            "--require",
            "docker",
            "--",
            "nvidia-smi",
        ])
        .expect("valid run command should parse");

        let Commands::Run(args) = cli.command else {
            panic!("run subcommand should be selected");
        };
        let constraints = args.into_job_spec().constraints;
        assert_eq!(constraints.operating_system.as_deref(), Some("linux"));
        assert_eq!(constraints.architecture.as_deref(), Some("aarch64"));
        assert_eq!(constraints.capabilities, ["gpu", "docker"]);
    }

    #[test]
    fn verdicts_are_described_in_plain_words() {
        assert_eq!(
            describe_verdict(NodeVerdict::Selected { load_permille: 255 }),
            "selected: lowest load (25.5% after placement)"
        );
        assert_eq!(
            describe_verdict(NodeVerdict::NoFreeCapacity {
                resource: LimitedResource::Memory
            }),
            "no free memory right now"
        );
        assert_eq!(
            describe_verdict(NodeVerdict::NotReady {
                state: NodeState::Draining
            }),
            "not accepting jobs (draining)"
        );
    }

    #[test]
    fn memory_parser_supports_decimal_and_binary_units() {
        assert_eq!(parse_memory("8GB"), Ok(8_000_000_000));
        assert_eq!(parse_memory("256MiB"), Ok(256 * 1_048_576));
        assert!(parse_memory("0").is_err());
        assert!(parse_memory("12TiB").is_err());
    }

    #[test]
    fn run_requires_a_command() {
        assert!(Cli::try_parse_from(["meld", "run"]).is_err());
    }

    #[test]
    fn controller_url_can_be_provided_as_a_global_option() {
        let job_id = JobId::generate();
        let cli = Cli::try_parse_from([
            "meld",
            "status",
            &job_id.to_string(),
            "--controller",
            "http://controller:4000",
        ])
        .expect("global controller option should parse");

        assert_eq!(cli.controller, "http://controller:4000");
    }

    #[test]
    fn default_controller_url_matches_node_default() {
        // SAFETY: this test only reads the parsed default and does not mutate the process environment.
        if env::var_os(CONTROLLER_URL_ENV).is_none() {
            let cli = Cli::try_parse_from(["meld", "status", &JobId::generate().to_string()])
                .expect("default controller URL should parse");
            assert_eq!(cli.controller, DEFAULT_CONTROLLER_URL);
        }
    }
}
