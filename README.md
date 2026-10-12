# Meld

**Run commands on the other computers in your home or lab, and get the results back.**

[日本語](README.ja.md)

Meld joins a handful of computers on a trusted network into one place to run jobs.
You describe a command and what it needs; Meld picks a machine with room for it,
runs it there, and gives you the output, the exit code and any files it made.

> **Status: experimental.** This is a research project, built in small, tested steps.
> It is meant for a few machines on a network you trust, running code you trust.
> It is **not** a way to run untrusted code. See [What it is not](#what-it-is-not).
>
> Not to be confused with the *Meld* diff and merge tool.

```text
$ meld run --memory 512MiB -- python3 train.py
job_id: 734a4410-a96d-4223-8366-a7ae1f1d3469
state: queued

$ meld status 734a4410-a96d-4223-8366-a7ae1f1d3469
state: succeeded
placement:
  f031d52d-... (workstation): selected: lowest load (6.6% after placement)
exit_code: 0
```

## What it does

- **Runs a command on another machine.** `meld run -- <command>`, with `status`,
  `logs` and `cancel`. Timeouts for a run and for the whole job.
- **Chooses where.** It places each job on a node with enough free CPU and memory,
  preferring the least loaded, and can be told to require an OS, an architecture or a
  capability. `meld status` explains why a job went where it did, or why it waits.
- **Moves files.** `--input` sends files or directories to the job, `--output` names
  what it makes, and `meld fetch` brings them back. Content is identified by SHA-256,
  checked on arrival, and sent only once.
- **Keeps one job from hurting the others.** A job that uses more memory than it
  reserved (plus 10%) is stopped on its own, with the reason shown.
- **Survives failures.** The controller keeps its state across restarts. If a node
  disappears, its jobs become `lost`, and a job you marked safe to repeat
  (`--max-attempts N --idempotent`) runs again elsewhere.
- **Controls who joins.** Clients and nodes authenticate with separate tokens. A new
  node waits until you `meld approve` it, and cannot pose as another node. Connections
  are encrypted and the controller is verified by its certificate fingerprint.

## What it is not

Be clear about these before relying on it:

- **Not a sandbox.** A job runs as the user that runs the node, with that user's
  access. Jobs are not isolated from each other, and can read the node's own secret.
  Only run code you would run on that machine yourself.
- **Not a place for secrets.** Commands, arguments and files are stored unencrypted
  on the controller and can be read by anyone holding the client token. There is no
  way to pass a secret to a job.
- **Not highly available.** There is one controller. If it is down, nothing new
  starts; running jobs continue and are picked up again when it returns.
- **Not for the internet.** It assumes a network you control. There is no NAT
  traversal, no discovery and no multi-user permissions.
- **Not a workflow engine.** Jobs are independent; there are no dependencies between
  them.
- **Limited resource control.** Memory is enforced; CPU, disk and network are not.
- **Moves data through one place.** Every file goes through the controller. There is
  no awareness of where data already lives.

**What has been tested:** automated end-to-end tests that start a real controller,
nodes and client on one machine, and a manual run with the controller on macOS and a
node on Linux (WSL) over a real LAN. **Not yet verified:** native Windows, more than
one node placed across machines, and long-running use.

## Quick start

You need [Rust](https://rustup.rs). The pinned toolchain is picked up automatically.

```sh
cargo build --release
# binaries: target/release/{meld, meld-controller, meld-node}
```

**1. Choose two secrets** (at least 16 characters each, and different):

```sh
export MELD_CLIENT_TOKEN=$(openssl rand -hex 16)   # for `meld`
export MELD_NODE_TOKEN=$(openssl rand -hex 16)     # for nodes joining
```

**2. Start the controller** on one machine:

```sh
MELD_CONTROLLER_ADDR=0.0.0.0:3000 meld-controller
# INFO encrypted connections only; give clients and nodes
#      MELD_CONTROLLER_FINGERPRINT=f6e74019...3b8c
```

With tokens set, the controller makes itself a certificate on first start and prints
its fingerprint (also written to `tls-fingerprint` in its state directory). Clients
and nodes trust only that certificate.

**3. Start a node** on each machine that should run jobs (it can be the same machine):

```sh
export MELD_CONTROLLER_URL=https://controller.lan:3000
export MELD_CONTROLLER_FINGERPRINT=f6e74019...3b8c
export MELD_NODE_TOKEN=...            # same value as the controller's
meld-node
```

**4. Use it** from anywhere that has the client token and the fingerprint:

```sh
export MELD_CONTROLLER_URL=https://controller.lan:3000
export MELD_CONTROLLER_FINGERPRINT=f6e74019...3b8c
export MELD_CLIENT_TOKEN=...

meld nodes                     # the new node is "pending"
meld approve <node-id>         # let it join; it becomes "ready"
meld run -- sh -c 'echo hello from $(hostname)'
meld status <job-id>
meld logs <job-id>
```

**Send files and get results back:**

```sh
meld run --input data.txt --output result.txt -- sh -c 'tr a-z A-Z < data.txt > result.txt'
meld fetch <job-id> --out-dir ./results
```

Without tokens, the controller listens on `127.0.0.1` only, without encryption or
approval: convenient for trying it out on one machine, nothing else.

## Commands

| Command | What it does |
|---|---|
| `meld run [options] -- <command>` | Submit a job |
| `meld status <job>` | State, placement, attempts, why it waits or failed |
| `meld logs <job>` | Captured stdout and stderr |
| `meld fetch <job> --out-dir DIR` | Download the files named with `--output` |
| `meld cancel <job>` | Stop a job |
| `meld retry <job>` | Run a failed, timed-out, cancelled or lost job again |
| `meld nodes` | Nodes, their state, capacity and usage |
| `meld approve <node>` / `meld revoke <node>` | Let a node join / remove it |
| `meld drain <node>` / `meld resume <node>` | Stop / resume placing new jobs on a node |

Useful `meld run` options: `--cpu`, `--memory` (default 256MiB, also the limit),
`--timeout`, `--job-timeout`, `--os`, `--arch`, `--require`, `--input`, `--output`,
`--max-attempts` with `--idempotent`. See `meld run --help`.

## Configuration

Everything is set with environment variables.

| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `MELD_CLIENT_TOKEN` | controller, `meld` | none | Token `meld` presents |
| `MELD_NODE_TOKEN` | controller, node | none | Token a node presents to join |
| `MELD_CONTROLLER_URL` | `meld`, node | `http://127.0.0.1:3000` | Where the controller is |
| `MELD_CONTROLLER_FINGERPRINT` | `meld`, node | none | SHA-256 of the controller's certificate to trust |
| `MELD_CONTROLLER_ADDR` | controller | `127.0.0.1:3000` | Address to listen on |
| `MELD_CONTROLLER_STATE_DIR` | controller | OS data dir | Database, files, certificate |
| `MELD_STATE_DIR` | node | OS data dir | Identity, secret, cache, workspaces |
| `MELD_HEARTBEAT_INTERVAL_SECS` | node | 5 | How often a node reports in |
| `MELD_HEARTBEAT_TIMEOUT_SECS` | controller | 15 | Silence before a node is unreachable |
| `MELD_LOST_GRACE_SECS` | controller | 30 | Further wait before its jobs are given up on |
| `MELD_MAX_CONCURRENT_EXECUTIONS` | node | CPU count | Jobs a node runs at once |
| `MELD_CAPABILITIES` | node | none | Labels jobs can require (comma separated) |
| `MELD_CACHE_MAX_BYTES` | node | 8 GiB | Input cache size, in bytes |
| `MELD_MAX_BLOB_BYTES`, `MELD_BLOB_QUOTA_BYTES` | controller | 1 GiB, 16 GiB | Largest file; total stored, in bytes |
| `MELD_OUTPUT_RETENTION_SECS` | controller | 86400 | Time outputs are kept for sure |

Setting only one of the two tokens is an error, and so is giving both the same value.
A controller that listens beyond the local machine refuses to start without tokens.

## How it works

```text
you ──meld──▶  controller  ◀── nodes poll for work ──  node ─▶ runs the process
                (one)                                  node ─▶ runs the process
```

- A **node** connects out to the controller, reports its capacity and usage, and asks
  for work. The controller never connects to a node.
- The **controller** keeps the record of jobs and nodes, decides placement, and stores
  files.
- A **job** has attempts. A retry is a new attempt of the same job, so history is kept.

Design notes and the roadmap, including what was decided against and why, are in
[`docs/`](docs/) (currently in Japanese).

## Development

```sh
cargo test --workspace        # unit and end-to-end tests; the latter start real processes
cargo clippy --workspace --all-targets
```

The end-to-end tests start a real controller, nodes and `meld`, then kill, freeze and
restart them. They run on Unix only and take a minute or two.

| Crate | Role |
|---|---|
| `meld-core` | Shared types, state transitions and the protocol |
| `meld-controller` | The controller |
| `meld-node` | The node |
| `meld-cli` | The `meld` command |
| `meld-tls` | Encrypted transport and certificate pinning |
