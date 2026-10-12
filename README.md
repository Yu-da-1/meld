# Meld

> Turn your computers into one logical compute environment.

Meld は、自宅や小規模な環境にある複数のコンピューターをまとめ、利用者からはひとつの論理的な計算環境として扱えるようにする分散コンピューティング基盤です。

最初の目標は「複数台の資源を完全に透過的な1台のOSとして見せること」ではありません。まずは、参加ノードを把握し、利用可能な資源に応じて実行先を選び、ジョブを安全に実行して結果を確認できる、小さく理解可能なシステムを作ります。その土台を固めた後に、データ配置、キャッシュ、障害復旧などを段階的に追加します。

## 現在の状態

Phase 0からPhase 7までが完了しています。認証(7a)、nodeの承認(7b-1)、node専用の秘密(7b-2a)、通信の暗号化(7c)、jobのメモリ制限(7d)、残るデータと権限の整理(7e)まで実装済みで、Phase 7は完了しています。

- `meld-controller`、`meld-node`、`meld-core`、`meld-cli`、`meld-tls` の5 crateでvirtual workspaceを構成しています。
- 共有domain model、job / execution状態遷移、in-memory Node RegistryとJob Manager、least-loaded Schedulerを実装済みです。
- nodeはHTTP/JSONでcontrollerへ登録し、永続化したidentity、capacity、resource snapshot、heartbeatを報告します。
- controllerはheartbeat timeoutによる`Unreachable`判定と、再接続・同一identityでの再登録を扱います。
- `meld run / status / logs / cancel`でnative processのremote execution、log回収、cancel、timeoutを扱えます。
- Phase 4のスケジューリングと資源会計として、次の機能が動作します。
  - nodeごとの予約済みCPU・メモリを合計し、capacityを超える配置を防ぎます。1つのnodeで複数jobを同時に実行できます(`MELD_MAX_CONCURRENT_EXECUTIONS`、既定はCPU数)。
  - `meld drain / resume`で新規配置を止めたり再開したりできます。実行中のjobは完走します。
  - `meld run --os / --arch / --require`で実行先のOS、architecture、capability(`MELD_CAPABILITIES`)を指定できます。
  - 複数のnodeが候補のとき、予約率が最も低いnodeを選びます。`meld status`にnodeごとの判定理由が表示されます。
  - 原則FIFOです。制約を満たすnodeがない、または合計容量を超えるjobは、後続のjobを止めません。
- Phase 5のデータ移動として、ファイルを渡して成果物を受け取れます。
  - `meld run --input SRC[=DEST] --output PATH -- <command>`で、手元のファイルやディレクトリをjobの作業ディレクトリへ送り、jobが作ったファイルを宣言できます。`meld fetch <job-id>`で回収します。
  - ファイルは内容のsha256で識別し、controllerが持たない内容だけを転送します。nodeはローカルcacheで再利用します。受信側は常にhashとサイズを検証します。
  - 転送の失敗は、jobの失敗と理由(`meld status`の`data_failure`)として表示されます。
- Phase 6の永続化と障害復旧として、controllerの状態はSQLiteに保存され、再起動後に復元されます。node消失は`Lost`として扱い、`meld retry`と、冪等と宣言したjobの自動retryがあります。
- Phase 7の認証として、controllerはclient用とnode用の別々のトークンで呼び出し元を確認し、認証が有効なときは通信を暗号化します。

## 使い方

```text
meld nodes
meld run --cpu 4 --memory 8G -- cargo build --release
meld run --input data.csv --input scripts --output out/result.json -- python3 scripts/analyze.py data.csv
meld status <job-id>
meld logs <job-id>
meld fetch <job-id> --out-dir ./results
meld cancel <job-id>
meld approve <node-id>
meld revoke <node-id>
```

`--memory`で指定した量は、nodeに予約され、jobが使ってよい量にもなります。超えて使うと(予約の110%まで余裕があります)、そのjobだけが止められて失敗し、`meld status`に理由が出ます。指定しないと既定の256MiBです。

`--input`にディレクトリを渡すと、中のファイルをすべて、そのディレクトリ名の下へ送ります。`SRC=DEST`で配置先を指定できます。`jobs`のような一覧表示は、まだありません。

### 設定

| 対象 | 環境変数 | 既定値 | 内容 |
|---|---|---|---|
| controller | `MELD_CONTROLLER_STATE_DIR` | OSのデータディレクトリ配下 | blobの保管先 |
| controller | `MELD_MAX_BLOB_BYTES` | 1GiB | 1ファイルの上限 |
| controller | `MELD_BLOB_QUOTA_BYTES` | 16GiB | 保管する内容の合計 |
| controller | `MELD_OUTPUT_RETENTION_SECS` | 86400 | 完了したjobの出力を削除しない期間 |
| node | `MELD_CACHE_MAX_BYTES` | 8GiB | 入力のcacheの上限 |
| `meld`、node | `MELD_CONTROLLER_FINGERPRINT` | なし | 信頼するcontrollerの証明書のsha256(認証が有効なcontrollerへ接続するとき) |
| controller | `MELD_CLIENT_TOKEN` | なし | `meld`が提示するトークン(controllerと`meld`の両方に設定) |
| controller | `MELD_NODE_TOKEN` | なし | nodeが参加するときのトークン(controllerと各nodeに設定) |

### 認証

トークンは16文字以上の、空白を含まない表示可能なASCII文字列です。`meld`は`MELD_CLIENT_TOKEN`、nodeは`MELD_NODE_TOKEN`を環境変数から読みます(コマンドラインに出さないため、引数では渡せません)。

- 両方のトークンを設定すると、controllerは全APIで`Authorization: Bearer <token>`を要求します。片方だけの設定と、同じ値の設定は起動時に拒否します。
- clientのトークンではjobの投入・照会・cancel・nodeの承認など、`meld`の操作ができます。`MELD_NODE_TOKEN`は、nodeが参加(登録)するためのトークンで、これでできるのは登録だけです。登録したnodeは、自分だけの秘密(state dirの`node-secret`)で、そのnodeとして動きます。他のnodeになりすますことはできません(403)。ファイルの転送だけは、clientと、承認済みのnodeに許可します。
- `node-secret`は消さないでください。失うと、controllerに別の秘密として断られます。その場合は`meld revoke <node-id>`、`meld approve <node-id>`のあとでnodeを再起動します。
- 未設定のままloopback以外のアドレス(`MELD_CONTROLLER_ADDR=0.0.0.0:3000`など)で待ち受けようとすると、controllerは起動を拒否します。loopbackでは警告を出して認証なしで動きます。
- トークンの変更は、controllerとすべてのnode、`meld`の設定を揃えて再起動します。トークンが合わないnodeは終了します。
- 認証が有効なとき、新しいnodeは承認待ち(`pending`)で参加します。`meld nodes`でIDを確かめ、`meld approve <node-id>`で承認します。承認するまで、nodeはjobを受け取りません。`meld revoke <node-id>`で参加を取り消すと、そのnodeは終了し、実行中だったjobは`lost`になります。認証が無効なとき(loopbackの開発用途)は、すべて自動で承認します。

### 暗号化

認証が有効なcontrollerは、TLSだけで待ち受けます。

1. controllerを起動すると、初回に自己署名証明書を作り、フィンガープリントを表示します(`<state dir>/tls-fingerprint`にも書きます)。
2. `meld`とnodeに、そのフィンガープリントと`https://`のURLを設定します。

```text
# controller
MELD_CLIENT_TOKEN=... MELD_NODE_TOKEN=... MELD_CONTROLLER_ADDR=0.0.0.0:3000 meld-controller
#   encrypted connections only; give clients and nodes MELD_CONTROLLER_FINGERPRINT=<64桁>

# meld と node
export MELD_CONTROLLER_URL=https://controller.lan:3000
export MELD_CONTROLLER_FINGERPRINT=<64桁>
```

- フィンガープリントが一致する証明書だけを信頼します。名前は見ないので、IPアドレスでも接続できます。
- `tls-cert.pem`と`tls-key.pem`を自分で置くと、それを使います。
- 証明書を作り直す(`tls-*`を消す)と、フィンガープリントが変わります。すべての`meld`とnodeの設定を替えてください。
- トークンがあるのにloopback以外へ`http://`で接続すると、警告します。トークンは平文で流れます。

### 運用上の注意

- jobは、nodeを動かしているユーザーの権限で動き、jobどうしを隔離しません。信頼できるjobだけを動かしてください。
- jobの仕様(コマンドと引数)、入力ファイル、出力ファイルは、暗号化されずに保管され、clientのトークンを持つ人が読めます。パスワードやAPIキーを渡さないでください。
- controllerとnodeは、専用の一般ユーザーで動かしてください。state dirは、新しく作るときに所有者だけが読める権限になります。
- jobが残すものと、いつ消えるかは、[Roadmap](docs/roadmap.md)のPhase 7(7e)にまとめています。

将来的には、利用者が物理的な実行先を毎回指定せず、データの所在も考慮して実行先が決まる状態を目指します。

Meld は各ノードの容量、現在の負荷、必要な機能を考慮して実行先を決定します。データの所在を考慮した配置は、今後のPhaseで扱います。

## 構成

```text
User / CLI
    |
    v
meld-controller        Control Plane
    |                   node、resource、job、schedule を管理
    |
    +-------------------+-------------------+
    |                   |                   |
    v                   v                   v
meld-node           meld-node           meld-node
Executor            Executor            Executor
    |                   |                   |
    v                   v                   v
Process             Process             Process

meld-core
    controller と node が共有する型、protocol、状態表現
```

想定する Rust workspace は次のとおりです。

```text
meld/
├── Cargo.toml
├── README.md
├── docs/
│   ├── architecture.md
│   └── roadmap.md
└── crates/
    ├── meld-controller/
    ├── meld-node/
    ├── meld-cli/
    ├── meld-tls/      通信の暗号化と、証明書の固定
    └── meld-core/
```

`meld-node` は各物理マシン上で常駐するプロセスです。AI agent との混同を避け、ジョブ実行だけでなく資源監視やローカルデータ管理まで担う「クラスタのノード」という役割を表すため、この名前を採用します。

## 設計方針

- 最小の end-to-end 経路を先に通し、分散システム固有の複雑さを一度に持ち込まない。
- 論理的な状態と物理的な配置を分ける。
- 実行先の決定は controller、実行そのものは node に分離する。
- 最初は単一 controller、信頼されたLAN、native process実行を前提にする。
- 障害を隠すのではなく、状態遷移として観測可能にする。
- protocol と domain model を `meld-core` に集め、通信方式とは疎結合にする。

詳細は以下を参照してください。

- [Architecture](docs/architecture.md) — 目的、全体構成、責務、主要な処理フロー
- [Roadmap](docs/roadmap.md) — 段階的な実装順序と各段階の完了条件

## 当面の非目標

初期段階では、次の機能を完成条件に含めません。

- 分散共有メモリやプロセス移動
- OS全体を透過的に統合する Single System Image
- controller の冗長化と強い可用性保証
- WAN越しのP2P discoveryやNAT traversal
- 複数テナント、課金、厳密な権限制御
- 汎用分散ストレージ
- 高度なGPUスケジューリング

これらを否定するのではなく、基礎となる実行・状態管理・障害検知が安定してから検討します。
