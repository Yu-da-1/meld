# Meld Implementation Roadmap

## 1. 進め方

Meld は、コンポーネントを個別に作り込んでから最後に接続するのではなく、小さなend-to-end経路を通し、それを段階ごとに堅牢にします。

```text
workspace
    -> shared model
    -> controller/node connection
    -> heartbeat/resource reporting
    -> one remote job
    -> observable job lifecycle
    -> scheduling
    -> data movement
    -> recovery/security
```

各phaseには完了条件を置きます。後続phaseのためだけの抽象化は先に作りすぎず、ただしcrate間の責務境界は維持します。

## 2. Phase 0 — Repository Foundation

### 目的

設計上決めた3 crateを実体化し、開発の基礎を整えます。

### 実装対象

- `crates/meld-controller` binary crate
- `crates/meld-node` binary crate
- `crates/meld-core` library crate
- root packageを残すかvirtual workspaceへ移行するかの決定
- formatting、lint、unit testの基本設定
- 最小限のerror handlingとstructured logging

### 完了条件

- `cargo check --workspace` が成功する。
- `cargo test --workspace` が成功する。
- controllerとnodeを別々に起動できる。
- crateの依存方向が `controller/node -> core` であり、`core` が実行crateへ依存しない。

### 現在地

virtual workspaceと3 crateを作成し、build、test、lint、個別起動の完了条件を満たしています。structured loggingはPhase 2で導入済みです。

## 3. Phase 1 — Domain Model and Local Vertical Slice

### 目的

networkや高度なschedulerより先に、Meldが管理する状態と1 jobのlife cycleを確定します。

### 実装対象

`meld-core` に最小限の型を定義します。

- `NodeId`、`JobId`、`ExecutionId`
- `NodeDescriptor` と `ResourceCapacity`
- `ResourceSnapshot`
- `JobSpec` と `ResourceRequirements`
- `JobState` と `ExecutionResult`
- controller/node間messageの最小集合

controller内部でin-memoryのNode Manager、Job Manager、単純schedulerを作り、まず同一process内のfake nodeで一連の状態遷移をtestします。

### 完了条件

- jobの正常終了、失敗、キャンセルの状態遷移がtestされている。
- 不正な状態遷移を拒否できる。
- 要求資源を満たすnodeと満たさないnodeを判定できる。
- domain modelが特定のtransportへ依存していない。

### 現在地

完了しています。共有domain model、状態遷移、in-memoryのNode RegistryとJob Manager、deterministicなfirst-fit Scheduler、controller-node間の最小protocol contractを実装し、正常終了、失敗、キャンセル、Lost、schedule不能を同一process内のtestで検証しています。

## 4. Phase 2 — Node Registration and Liveness

### 目的

実際の `meld-node` と `meld-controller` を接続し、controllerが参加nodeとその生存状態を把握できるようにします。

### 実装対象

- nodeからcontrollerへのjoin
- 安定したnode identity
- heartbeat
- capacityとresource snapshotの送信
- heartbeat timeoutによる`Unreachable`判定
- reconnectと再登録
- `nodes`相当の一覧表示

初期transportはHTTP/JSONを使用します。nodeからcontrollerへ接続し、assignmentはlong pollingで取得します。control messageの性能を実測し、必要性が明確になった場合にのみ別transportを比較します。

### 完了条件

- 2つ以上のnode processをcontrollerへ登録できる。
- controllerがnodeごとのcapacityと最新usageを表示できる。
- node停止後、設定時間内に`Unreachable`になる。
- node再起動後、同じidentityとして復帰できる。
- protocol version mismatchを明示的に報告できる。

### 現在地

完了しています。HTTP/JSONによるnode登録、永続identity、capacityとresource snapshotを含むheartbeat、timeoutによる`Unreachable`判定、指数backoff付きの再接続・再登録、`GET /v1/nodes`による一覧表示、protocol version mismatchの構造化エラーを実装しました。

2つのnode processを同時登録して各nodeのcapacityと最新usageを取得し、一方の停止による`Unreachable`化、同じidentityでの再起動と`Ready`復帰、version mismatchに対するHTTP 426応答をend-to-endで検証しています。

## 5. Phase 3 — Remote Execution MVP

### 目的

利用者がcontrollerへjobを投入し、選ばれたnodeでnative processを1つ実行して結果を受け取れるようにします。ここが最初の製品的な完成点です。

### 実装対象

- job submit API / CLI
- FIFO queue
- Readyかつ要求を満たすnodeの選択
- execution assignmentとacknowledgement
- node上のworking directory
- native process executor
- stdout / stderr / exit codeの回収
- status、logs、cancel
- job / execution timeout

### 完了条件

- `meld run -- <command>` に相当する操作で別node上のcommandが実行される。
- jobの状態をsubmitから完了まで追跡できる。
- stdout、stderr、exit codeを利用者が取得できる。
- 実行中jobをキャンセルできる。
- assignmentの再送がprocessの二重起動を招かない。
- nodeに空きがなければjobが理由付きでQueuedに留まる。

### このphaseでは行わないこと

- 自動retry
- container isolation
- 任意ディレクトリの同期
- controller再起動をまたぐ完全な復旧

まず単一jobの意味を明確にします。

### 現在地

完了しています。`meld` CLIからのjob投入、FIFO queue、Readyかつ要求資源を満たすnodeの選択、long pollingによるcommand配信、assignment acknowledgement、node上の分離workspaceとnative process実行を実装しました。

`meld status / logs / cancel`から状態、実行node、stdout、stderr、exit codeを確認でき、実行中jobのcancel、process開始後のexecution timeout、submitから完了までのjob timeoutを扱えます。Queued jobは`no_ready_nodes`、`insufficient_resources`、`no_available_nodes`、`waiting_for_earlier_job`、`awaiting_assignment`の理由を表示します。

assignmentはprocess起動前にNodeがAcceptedを報告し、同じexecutionの再送で二重起動しない順序にしています。正常終了、起動失敗、cancel、execution timeout、queue中と実行中のjob timeout、部分ログ回収をunit/API testと実processのend-to-end testで検証しました。共有protocol contractはversion 5です。

controllerとnodeの再起動をまたぐdeadline復元や完全なreconciliationはPhase 6、認証・暗号化・実行隔離はPhase 7で扱います。それまではlocalhostまたは信頼された開発用LANに限定します。

## 6. Phase 4 — Scheduling and Resource Accounting

### 目的

複数jobを安全に配置し、実際の空き容量以上に割り当てないようにします。

### 実装対象

- resource reservation
- capacity filter
- least-loadedなどの単純なscoring
- node drain / resume
- queue fairnessの基本方針
- concurrent job limit
- schedule不能理由の説明
- OS / architecture / capability constraints

### 完了条件

- 複数jobの合計予約量がnode capacityを超えない。
- 条件を満たさないnodeへjobを配置しない。
- drain中のnodeへ新規jobを配置しない。
- 同じ入力状態に対するschedulerの判断をunit testできる。
- 利用者が「なぜこのnodeか」「なぜ待機中か」を確認できる。

最初のschedulerは賢さより説明可能性と正しさを優先します。

### 現在地

完了しています。

- 予約: 実行中のexecutionの要求量をnodeごとに合計して予約量とし、capacityと`max_concurrent_executions`を超える配置を防ぎます。予約量は保持せず毎回導出するので、二重管理のずれが起きません。
- 並列実行: nodeは複数のexecutionを同時に管理できます(`MELD_MAX_CONCURRENT_EXECUTIONS`、既定はCPU数)。pollは実行中のexecution一覧を送り、controllerは報告済みのexecutionを再送しません。共有protocol contractはversion 6です。
- drain / resume: `meld drain / resume`で新規配置を止めたり再開したりできます。drainの意図は生存状態と別に保持し、heartbeat、`Unreachable`からの復帰、再登録をまたいで維持します。
- 制約: `meld run --os / --arch / --require`と、nodeの`MELD_CAPABILITIES`でOS、architecture、capabilityによる絞り込みができます。
- 選択: 候補のうち、そのjobを足した後のCPUとメモリの予約率の大きい方が最小のnodeを選びます。同率はNode IDの小さい方です。浮動小数点は使わず、判断は入力だけで決まります。
- 説明: `meld status`が、nodeごとの判定(選択、見送り、drain中、制約不一致、容量不足、空き不足の資源)を表示します。
- queue fairness: 原則FIFOですが、制約を満たすnodeがない、または合計容量を超えるjobは、後続のjobを止めません。単に空きを待っているjobは順番を守るため、大きなjobが小さなjobに追い越され続けて動かなくなることはありません。

### 既知の制約

- 予約は帳簿上のものです。jobが予約を超えてCPUやメモリを使っても止めません(Phase 7で扱います)。
- macOSでは`sysinfo`が利用可能メモリを0と返すため、`meld nodes`の`usage`に反映されません。スケジューリングには使っていません。
- 予約は実測の使用量を見ません。同じマシン上の他のprocessが使うCPUやメモリは考慮されません。

## 7. Phase 5 — Data Movement

### 目的

実行に必要なファイルをnodeへ渡し、成果物を回収できるようにします。

### 実装対象

- 明示的なinput manifest
- controller/clientからnodeへのfile transfer
- checksumによるintegrity確認
- output manifestとresult collection
- temporary workspace cleanup
- サイズ制限、転送失敗、容量不足の扱い

### 完了条件

- jobが宣言したinputだけを転送できる。
- outputを指定場所へ回収できる。
- 転送失敗がjob状態とerror reasonに反映される。
- 同一inputを安全に再利用する最小cacheを検証できる。

この段階までは、controller/client側を正本とする中央集約モデルで構いません。

### 現在地

完了しています。`meld run --input / --output`でファイルを渡して成果物を受け取れます。ファイルは内容のsha256で識別し、controllerが保管します。共有protocol contractはversion 7です。

- manifest: `JobSpec.data`が、nodeへ渡す`inputs`(path、sha256、サイズ、実行権限)と、回収する`outputs`(path)を宣言します。宣言されたファイルだけが移動します。pathは相対のみで、`..`、絶対path、`\`、drive prefix、重複(大文字小文字を無視)、ファイルとディレクトリの入れ子を拒否します。上限は1ファイル1GiB、1 jobの入力合計4GiB、入力と出力それぞれ1,000件です。
- 転送: CLIがディレクトリをファイル単位に展開してhashを計算し、controllerが持たない内容だけを`PUT /v1/blobs/{sha256}`でuploadします。受信側は、streamで書きながらhashとサイズを検証し、一致したときだけ確定します。nodeは`GET`で取得します。同一内容は1回だけ転送します。
- 入力の配置: nodeは取得した内容をローカルcacheに置き、jobのworkspaceへ**コピー**します。jobがファイルを書き換えてもcacheは変わりません。cacheはLRUで`MELD_CACHE_MAX_BYTES`(既定8GiB)に収め、コピー中の内容は削除しません。同一内容の同時取得は1回にまとめ、一時的な失敗は3回まで再試行します。実行前に空き容量を確認します。
- 出力の回収: process が正常終了(exit 0)したときだけ、宣言された各fileをhash→uploadし、完了報告にmanifestを載せます。pathの全成分を`symlink_metadata`で検査し、symbolic link経由でworkspace外のファイルを持ち出せないようにしています。cancelとtimeoutでは回収しません。controllerは、宣言と一致すること、保管されたblobのサイズが一致することを確認してから成功にします。
- 失敗の反映: `DataFailure`(入力の取得不能、checksum不一致、容量不足、出力の未生成、upload失敗、安全でないpathなど)を持つ`DataFailed`で報告します。jobはFailedとなり、理由が`meld status`に出ます。process自体の失敗とは区別されます。
- 保持: 未完了のjobが参照する入力は削除しません。出力は完了から24時間(`MELD_OUTPUT_RETENTION_SECS`)保護します。これは最低限の保証で、期間後は容量が必要になったときだけ古い順に削除します。controllerの合計容量は`MELD_BLOB_QUOTA_BYTES`(既定16GiB)です。
- 利用者向け操作: `meld run --input SRC[=DEST] --output PATH`、`meld fetch <job> [--out-dir DIR] [--force]`、`meld status`の入出力・失敗理由の表示。既存ファイルは`--force`なしでは上書きしません。

unit / API testに加え、controller、node、`meld`の実processを起動するend-to-end test(`crates/meld-cli/tests/data_movement.rs`)で、入力の加工と回収、cacheの再利用、20MiBのファイル、出力の未生成、symbolic linkによる持ち出しの拒否、controllerから消えた入力などを検証しています。

### 完了条件の確認

| 完了条件 | 状態 |
|---|---|
| jobが宣言したinputだけを転送できる | 達成。manifestにないファイルは送らない |
| outputを指定場所へ回収できる | 達成。`meld fetch --out-dir`で検証付きに保存 |
| 転送失敗がjob状態とerror reasonに反映される | 達成。Failedと`data_failure` |
| 同一inputを安全に再利用する最小cacheを検証できる | 達成。nodeのcacheとcontrollerの重複排除 |

### 既知の制約

- 認証と暗号化はありません。ファイルは平文で通信されるため、機密データは扱えません。信頼された開発用LANに限定します(Phase 7)。
- controllerの再起動でjobの情報と出力の保持期限は失われます。保管済みのblobはディスクに残り、再起動時に取り込みます(Phase 6)。
- 空き容量の確認は、同時に準備される複数のjobの間では競合し得ます。controllerのquotaも、進行中のuploadを合計に含めません。
- process が正常終了したあと、バックグラウンドに残った子孫processがworkspaceを書き換え得ます。回収はprocessの終了時点のファイルを読みます。
- 扱うメタデータはパスと内容と実行権限だけです。所有者、時刻、symbolic linkは扱わず、入力ディレクトリにsymbolic linkがあるとエラーにします。Windowsのnodeでは実行権限を設定しません。
- 出力はファイル単位で宣言します。ディレクトリを丸ごと指定することはできません。
- 転送は常にcontroller経由です。node同士の転送や、データの所在を考慮した配置はPhase 8で扱います。

## 8. Phase 6 — Persistence and Recovery

### 目的

processやnetworkの障害を状態として扱い、controller再起動後にもjob履歴を失わないようにします。

### 実装対象

- controller stateのローカルDB永続化
- startup reconciliation
- job execution attemptの履歴
- retry policyとbackoff
- node切断時の`Lost` / `Unknown`状態
- manual retryと、条件付きautomatic retry
- result/reportの重複受信への耐性

### 完了条件

- controller再起動後にnodeとjobの既知状態を復元できる。
- nodeからの再接続情報と永続状態をreconcileできる。
- retryが新しいattemptとして記録される。
- 非冪等jobを無条件に自動再実行しない。
- 障害注入testで切断、timeout、重複messageを検証できる。

### 現在地

進行中です。次のスライスを実装済みで、残りは未着手です。

- 永続化(完了): controllerの状態をローカルのSQLite(`<state dir>/state.db`)へ保存し、起動時に復元します。保存するのはjobとexecution(attemptの順序を含む)、stdout/stderr、データ失敗の理由、出力file、cancelとjob timeoutの要求、キューの順序、配置の判定、nodeのdescriptorとdrainの意図です。attempt一覧や資源の予約のように導出できるものは保存せず、復元時に組み直します。変更はロックを保持したまま保存してから応答します。job投入だけは、保存に失敗すると500を返します。
- job timeout: deadlineは壁時計時刻で保存するので、controllerの停止中も進みます。
- 復元後のnode: heartbeatが届くまで`Unreachable`です。drainの意図は保たれます。
- startup reconciliation(完了): 復元した、確認応答以降のexecution(`Accepted` / `Running` / `Cancelling`)は、そのnodeの最初のpollで確認します。nodeの実行中一覧にあれば継続し、なければprocessも未送信の結果も存在しないので`Lost`にします。通常運用中のpollは古い一覧を持ち得るため、この判定は復元されたexecutionに限ります。`Assigned`は再送されるので対象外です。
- node切断(完了): nodeが`Unreachable`のまま`MELD_HEARTBEAT_TIMEOUT_SECS`に`MELD_LOST_GRACE_SECS`(既定30秒)を足した時間を超えると、そのnodeのexecutionを整理します。確認応答前の`Assigned`はprocessが起動していないので、jobをキューの先頭へ戻して別のnodeへ配置します。確認応答済みのexecutionは、processが生きている可能性があるので`Lost`にします。復元直後でheartbeatの記録がないnodeは、controllerの起動時刻から数えます。
- `Lost`は最終状態ではありません: 通信断はprocessの停止を意味しないので、nodeが戻って終了(成功、失敗、cancel、timeout)を報告したら、その結果が`Lost`を置き換えます。`Lost`から`Accepted`や`Running`などの前の段階へは戻りません。
- 未実装: 手動retryとattempt履歴の操作、条件付きの自動retry、障害注入test。

### 既知の制約

- 保存の失敗は、job投入を除き、ログに残して次の保存で再試行するだけです。
- `Lost`にした後もprocessが動き続けている場合、controllerの帳簿上は資源が空いたことになり、そのnodeへ過剰に配置し得ます。nodeが戻って結果を報告するまでの間です。
- DBは平文です。認証と暗号化はPhase 7で扱います。

## 9. Phase 7 — Security and Execution Isolation

### 目的

「任意コードを別マシンで実行する」システムとして必要な信頼境界を明確にします。

### 実装対象

- controllerとnodeの相互認証
- encrypted transport
- node join approval / revoke
- CLI利用者の認証と認可
- secretの安全な受け渡し
- command allow/deny policyの検討
- container executorの追加
- CPU、memory、filesystem、networkの制限

### 完了条件

- 未承認nodeがclusterへ参加できない。
- 未承認clientがjobを投入できない。
- 通信が暗号化され、identityを検証できる。
- native executorとcontainer executorを同じ上位interfaceから選べる。
- 実行権限と残存ファイルのcleanup方針が文書化・testされている。

安全性はPhase 7まで無視するという意味ではありません。初期phaseは信頼された開発用LANに範囲を限定し、外部公開はこのphaseの完了後とします。

## 10. Phase 8 — Data Locality and Distributed Data

### 目的

大きな入力を毎回中央から転送せず、データが既に存在するnodeを活用します。

### 実装対象

- logical data ID / content hash
- data location metadata
- node local cache
- cache validationとeviction
- schedulerへのdata locality score
- replication policy
- node離脱時のデータ所在更新

### 完了条件

- 同じcontentを識別し、不要な再転送を避けられる。
- schedulerが計算資源と転送costを合わせて判断できる。
- cache消失が正本データの消失を意味しない。
- metadataと実体の不一致を検出・修復できる。

汎用分散filesystemへ進むかは、このphaseの実測結果から判断します。

## 11. Phase 9 — Advanced Capabilities

基礎が安定した後、実際の利用要求に基づいて優先順位を決めます。

- GPU detectionとdevice-aware scheduling
- DAG / workflow execution
- web dashboardと外部API
- WAN接続、P2P discovery、NAT traversal
- controller high availability
- checkpoint / resume
- energy-aware scheduling
- platform別node service化（systemd / launchdなど）

Distributed Shared MemoryやSingle System Imageは、このroadmapの通常機能追加ではなく、別のarchitecture proposalとして扱います。

## 12. 直近の実装順序

次にコードへ進む際は、以下の順番が最短です。

1. 3 crateを作りworkspaceをbuild可能にする。
2. `meld-core` にID、Node、Resource、Job、Stateを定義する。
3. 状態遷移とcapacity filterをunit testする。
4. controllerを起動し、in-memory registryを用意する。
5. nodeを接続し、joinとheartbeatを通す。
6. node一覧とresource snapshotを表示する。
7. 固定nodeへの単一command実行を通す。
8. 結果とlogを返す。
9. schedulerによるnode選択へ置き換える。
10. cancel、timeout、切断を順に扱う。

この10項目が完了するまでは、高度なtransport、GUI、分散storageへ広げないことを推奨します。

## 13. Roadmap の更新ルール

- phase完了時に完了条件を確認し、未達事項を明示する。
- 新機能は、どの責務とphaseに属するかを決めてから追加する。
- 実測や試作で前提が変わった場合は、設計文書とroadmapをコードと同時に更新する。
- 「将来使うかもしれない」だけの抽象化より、現在の境界を守る小さな実装を優先する。
