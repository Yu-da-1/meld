# Meld

**自宅や研究室にある他のコンピューターでコマンドを実行し、結果を受け取る。**

[English](README.md)

Meld は、信頼できるネットワーク上の数台のコンピューターを、ジョブを実行するひとつの場所としてまとめます。
実行したいコマンドと必要な資源を伝えると、Meld が空きのあるマシンを選んで実行し、出力、終了コード、作られたファイルを返します。

> **状態: 実験的です。** 小さく、テストしながら作っている研究プロジェクトです。
> 信頼できるネットワーク上の数台のマシンで、信頼できるコードを動かすことを想定しています。
> 信頼できないコードを動かす用途には**使えません**。[できないこと](#できないこと)を参照してください。
>
> 差分・マージツールの *Meld* とは関係ありません。

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

## できること

- **別のマシンでコマンドを実行する。** `meld run -- <command>`。`status`、`logs`、`cancel` と、実行時間とjob全体のtimeoutがあります。
- **実行先を選ぶ。** CPUとメモリに空きがあるnodeのうち、負荷の低いものへ配置します。OS、アーキテクチャ、capabilityで絞り込めます。`meld status` で、なぜそこに置かれたか、なぜ待っているかが分かります。
- **ファイルを運ぶ。** `--input` で渡し、`--output` で作られるファイルを宣言し、`meld fetch` で受け取ります。内容はSHA-256で識別され、到着時に検証され、同じ内容は一度しか送りません。
- **1つのjobが他を巻き込まないようにする。** 予約したメモリ(の110%)を超えたjobは、そのjobだけが理由つきで止められます。
- **障害に耐える。** controllerは再起動しても状態を保ちます。nodeが消えるとjobは `lost` になり、再実行しても安全と宣言したjob(`--max-attempts N --idempotent`)は別のnodeで再実行されます。
- **参加できる者を制御する。** clientとnodeは別々のトークンで認証します。新しいnodeは `meld approve` するまで働けず、他のnodeになりすませません。通信は暗号化され、controllerは証明書のフィンガープリントで確かめます。

## できないこと

使う前に、次の点を明確にしておいてください。

- **sandboxではありません。** jobは、nodeを動かしているユーザーの権限で動きます。jobどうしは隔離されず、nodeの秘密も読めます。そのマシンで自分で実行してよいコードだけを動かしてください。
- **secretの置き場ではありません。** コマンド、引数、ファイルはcontrollerに暗号化されずに保存され、clientのトークンを持つ誰もが読めます。jobにsecretを渡す手段はありません。
- **高可用ではありません。** controllerは1台です。停止中は新しいjobが始まりません。実行中のjobは続き、controllerが戻れば再び拾われます。
- **インターネット向けではありません。** 自分が管理するネットワークを前提にしています。NAT越え、探索、複数利用者の権限管理はありません。
- **ワークフローエンジンではありません。** jobは独立しており、依存関係を持てません。
- **資源の制御は限定的です。** メモリは強制しますが、CPU、ディスク、ネットワークは制限しません。
- **データは1か所を経由します。** すべてのファイルがcontrollerを通ります。データがすでにある場所を考慮しません。

**確認したこと:** 1台のマシンで、実際のcontroller、node、clientを起動する自動のend-to-end testと、macOSのcontrollerとLinux(WSL)のnodeを実際のLANでつないだ手動の確認です。**まだ確認していないこと:** ネイティブWindows、複数のnodeにまたがる配置、長時間の運用です。

## クイックスタート

[Rust](https://rustup.rs) が必要です。固定されたtoolchainは自動で選ばれます。

```sh
cargo build --release
# 実行ファイル: target/release/{meld, meld-controller, meld-node}
```

**1. 2つの秘密を決める**(それぞれ16文字以上、別の値):

```sh
export MELD_CLIENT_TOKEN=$(openssl rand -hex 16)   # `meld` 用
export MELD_NODE_TOKEN=$(openssl rand -hex 16)     # 参加するnode用
```

**2. controllerを起動する**(1台で):

```sh
MELD_CONTROLLER_ADDR=0.0.0.0:3000 meld-controller
# INFO encrypted connections only; give clients and nodes
#      MELD_CONTROLLER_FINGERPRINT=f6e74019...3b8c
```

トークンを設定すると、controllerは初回起動で自分用の証明書を作り、フィンガープリントを表示します(state dirの `tls-fingerprint` にも書きます)。clientとnodeは、その証明書だけを信頼します。

**3. nodeを起動する**(jobを実行させたい各マシンで。controllerと同じマシンでも構いません):

```sh
export MELD_CONTROLLER_URL=https://controller.lan:3000
export MELD_CONTROLLER_FINGERPRINT=f6e74019...3b8c
export MELD_NODE_TOKEN=...            # controllerと同じ値
meld-node
```

**4. 使う**(clientのトークンとフィンガープリントがある場所から):

```sh
export MELD_CONTROLLER_URL=https://controller.lan:3000
export MELD_CONTROLLER_FINGERPRINT=f6e74019...3b8c
export MELD_CLIENT_TOKEN=...

meld nodes                     # 新しいnodeは "pending"
meld approve <node-id>         # 参加を許可する。"ready" になる
meld run -- sh -c 'echo hello from $(hostname)'
meld status <job-id>
meld logs <job-id>
```

**ファイルを渡して、結果を受け取る:**

```sh
meld run --input data.txt --output result.txt -- sh -c 'tr a-z A-Z < data.txt > result.txt'
meld fetch <job-id> --out-dir ./results
```

トークンを設定しない場合、controllerは `127.0.0.1` だけで待ち受け、暗号化も承認もありません。1台で試すためのもので、それ以外には使わないでください。

## コマンド

| コマンド | 内容 |
|---|---|
| `meld run [options] -- <command>` | jobを投入する |
| `meld status <job>` | 状態、配置、試行、待っている理由、失敗の理由 |
| `meld logs <job>` | 取得した標準出力と標準エラー |
| `meld fetch <job> --out-dir DIR` | `--output` で宣言したファイルを受け取る |
| `meld cancel <job>` | jobを止める |
| `meld retry <job>` | 失敗、timeout、cancel、`lost` のjobを再実行する |
| `meld nodes` | nodeの状態、容量、使用状況 |
| `meld approve <node>` / `meld revoke <node>` | nodeの参加を許可する / 取り消す |
| `meld drain <node>` / `meld resume <node>` | nodeへの新規配置を止める / 再開する |

`meld run` の主なオプション: `--cpu`、`--memory`(既定256MiB。これが上限にもなります)、`--timeout`、`--job-timeout`、`--os`、`--arch`、`--require`、`--input`、`--output`、`--max-attempts` と `--idempotent`。詳しくは `meld run --help`。

## 設定

設定はすべて環境変数です。

| 変数 | 使う側 | 既定値 | 内容 |
|---|---|---|---|
| `MELD_CLIENT_TOKEN` | controller、`meld` | なし | `meld` が提示するトークン |
| `MELD_NODE_TOKEN` | controller、node | なし | nodeが参加するときのトークン |
| `MELD_CONTROLLER_URL` | `meld`、node | `http://127.0.0.1:3000` | controllerの場所 |
| `MELD_CONTROLLER_FINGERPRINT` | `meld`、node | なし | 信頼するcontrollerの証明書のSHA-256 |
| `MELD_CONTROLLER_ADDR` | controller | `127.0.0.1:3000` | 待ち受けるアドレス |
| `MELD_CONTROLLER_STATE_DIR` | controller | OSのデータ領域 | データベース、ファイル、証明書 |
| `MELD_STATE_DIR` | node | OSのデータ領域 | 識別子、秘密、cache、作業場所 |
| `MELD_HEARTBEAT_INTERVAL_SECS` | node | 5 | 報告の間隔 |
| `MELD_HEARTBEAT_TIMEOUT_SECS` | controller | 15 | 沈黙がこれを超えると到達不能 |
| `MELD_LOST_GRACE_SECS` | controller | 30 | そのjobを諦めるまでの、さらなる猶予 |
| `MELD_MAX_CONCURRENT_EXECUTIONS` | node | CPU数 | nodeが同時に実行するjob数 |
| `MELD_CAPABILITIES` | node | なし | jobが要求できるラベル(カンマ区切り) |
| `MELD_CACHE_MAX_BYTES` | node | 8 GiB | 入力cacheの上限(バイト) |
| `MELD_MAX_BLOB_BYTES`、`MELD_BLOB_QUOTA_BYTES` | controller | 1 GiB、16 GiB | 1ファイルの上限、保管する合計(バイト) |
| `MELD_OUTPUT_RETENTION_SECS` | controller | 86400 | 出力を確実に残す期間 |

2つのトークンの片方だけの設定と、同じ値の設定は、エラーです。ローカルのマシン以外で待ち受けるcontrollerは、トークンなしでは起動しません。

## 仕組み

```text
あなた ──meld──▶  controller  ◀── nodeが仕事を取りに来る ──  node ─▶ processを実行
                   (1台)                                     node ─▶ processを実行
```

- **node** はcontrollerへ自分から接続し、容量と使用状況を報告して、仕事を求めます。controllerからnodeへ接続することはありません。
- **controller** はjobとnodeの記録を持ち、配置を決め、ファイルを保管します。
- **job** は試行(attempt)を持ちます。再実行は同じjobの新しい試行になるので、履歴は残ります。

設計の記録と、見送ったことの理由を含む roadmap は、[`docs/`](docs/) にあります(現在は日本語です)。

## 開発

```sh
cargo test --workspace        # 単体testと、実processを起動するend-to-end test
cargo clippy --workspace --all-targets
```

end-to-end testは、実際のcontroller、node、`meld` を起動し、killや凍結や再起動を行います。Unix限定で、1〜2分かかります。

| crate | 役割 |
|---|---|
| `meld-core` | 共有する型、状態遷移、protocol |
| `meld-controller` | controller |
| `meld-node` | node |
| `meld-cli` | `meld` コマンド |
| `meld-tls` | 暗号化された通信と、証明書の固定 |
