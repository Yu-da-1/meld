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

完了しています。

- 永続化(完了): controllerの状態をローカルのSQLite(`<state dir>/state.db`)へ保存し、起動時に復元します。保存するのはjobとexecution(attemptの順序を含む)、stdout/stderr、データ失敗の理由、出力file、cancelとjob timeoutの要求、キューの順序、配置の判定、nodeのdescriptorとdrainの意図です。attempt一覧や資源の予約のように導出できるものは保存せず、復元時に組み直します。変更はロックを保持したまま保存してから応答します。job投入だけは、保存に失敗すると500を返します。
- job timeout: deadlineは壁時計時刻で保存するので、controllerの停止中も進みます。
- 復元後のnode: heartbeatが届くまで`Unreachable`です。drainの意図は保たれます。
- startup reconciliation(完了): 復元した、確認応答以降のexecution(`Accepted` / `Running` / `Cancelling`)は、そのnodeの最初のpollで確認します。nodeの実行中一覧にあれば継続し、なければprocessも未送信の結果も存在しないので`Lost`にします。通常運用中のpollは古い一覧を持ち得るため、この判定は復元されたexecutionに限ります。`Assigned`は再送されるので対象外です。
- node切断(完了): nodeが`Unreachable`のまま`MELD_HEARTBEAT_TIMEOUT_SECS`に`MELD_LOST_GRACE_SECS`(既定30秒)を足した時間を超えると、そのnodeのexecutionを整理します。確認応答前の`Assigned`はprocessが起動していないので、jobをキューの先頭へ戻して別のnodeへ配置します。確認応答済みのexecutionは、processが生きている可能性があるので`Lost`にします。復元直後でheartbeatの記録がないnodeは、controllerの起動時刻から数えます。
- `Lost`は最終状態ではありません: 通信断はprocessの停止を意味しないので、nodeが戻って終了(成功、失敗、cancel、timeout)を報告したら、その結果が`Lost`を置き換えます。`Lost`から`Accepted`や`Running`などの前の段階へは戻りません。
- 手動retry(完了): `meld retry <job>`が、`Failed` / `TimedOut` / `Cancelled` / `Lost`のjobを同じjob IDのまま`Queued`に戻し、次の配置で新しいexecutionを作ります。以前のattemptの記録はそのまま残り、`meld status`が全attemptを古い順に表示します。キューでは、すでに待っているjobの後ろに並びます。`Succeeded`や未完了のjobは対象外です。job timeoutは新しいattemptごとに数え直します。
  - 同じ効果は`POST /v1/jobs/{id}/retry`で得られます。retryは通常の状態遷移の表には入れず、`Job::retry`だけが行います。nodeの報告が完了済みのjobをキューに戻すことはありません。
  - `Lost`のjobは元のprocessがまだ動いている可能性があり、二重に実行し得ます。そのため`--allow-duplicate-run`を付けない限り拒否します。
  - retryで置き換えられた古いattemptが後から結果を報告しても、jobには反映しません(409)。新しいattemptが走っているjobを、古い結果で完了させないためです。
  - 入力fileの保持はjobが未完了の間だけなので、retry時に入力がcontrollerから消えていれば422で拒否します。その場合は`meld run`で投入し直します。
- 自動retry(完了): `meld run --max-attempts N --idempotent`で、controllerが条件付きでjobを自動的に再実行します。既定は再実行なしです。
  - 宣言: 2回以上の試行を許すには、jobが冪等であると明示する必要があります。回数だけの指定は投入時に拒否します(400)。再実行は、中断前の副作用を繰り返し得るためです。
  - 対象: nodeの消失(`Lost`)、データ転送の一時的な失敗(`InputUnavailable` / `LocalStorage` / `InsufficientDisk` / `OutputUploadFailed`)、processの起動失敗です。exit codeが0以外の終了、timeout、チェックサム不一致やpath不正などjob自体に起因するデータ失敗、利用者のcancel中のjobは再実行しません。
  - `Lost`の自動retry: 元のprocessが動き続けている場合、処理が二重に走り得ます。冪等と宣言したjobに限り許可し、古いattemptの結果はjobに反映しません。
  - backoff: 5秒から始めて倍々に増やし、上限は5分です。待機中のjobは`Queued`のまま`waiting_to_retry`と表示し、後ろのjobを止めません。待機期限は壁時計で保存するので、再起動をまたいで残ります。
  - 回数: `max_attempts`は1回の実行(run)あたりの試行数で、最初の試行を含みます。手動retryは新しいrunとして数え直します。
  - job timeout: 自動retryでは延びません。最初の投入から通しで数え、期限を過ぎれば待機中に`TimedOut`になります。手動retryは利用者が意図して延長する操作なので、これまでどおり数え直します。
  - 表示: `meld status`が`retry: attempt 2 of 3 in 4s (node lost)`のように、次の試行番号、残り時間、理由を示します。
- 障害注入test(完了): `crates/meld-cli/tests/fault_injection.rs`が、実processのcontroller、node、`meld`を起動して障害を起こします。補助コードは`tests/common`で`data_movement.rs`と共有します。待ち時間は`MELD_HEARTBEAT_TIMEOUT_SECS`、`MELD_LOST_GRACE_SECS`、`MELD_RETRY_BACKOFF_SECS`、`MELD_RETRY_BACKOFF_MAX_SECS`で短縮できます。

  | 起こす障害 | 確認すること |
  |---|---|
  | 実行中にcontrollerをkillして再起動 | jobは中断されず、nodeが新しいcontrollerへ結果を報告して完了する |
  | 実行中にnodeをkill | 猶予後に`lost`になる。retryを求めていないjobは、別のnodeが空いていても再実行されない |
  | 上のjobが冪等でretryを許可 | 別のnodeで2回目が走り、成功する。processは試行ごとに1回だけ起動する |
  | 冪等なjobがexit 3で終了 | 再実行されない |
  | nodeをkillしたままcontrollerを再起動 | 再起動したcontrollerが沈黙を数えなおし、jobを`lost`にする |
  | retryの待機中にcontrollerを再起動 | 待機は残り、別のnodeの追加後に2回目が走る |
  | nodeを`SIGSTOP`で凍結し、`SIGCONT`で再開 | 凍結中に`lost`になり、再開後に報告された実際の結果が`Lost`を置き換える |
  | execution timeoutを超えるprocess(冪等でretry許可) | processは止まり、`failed`になる。再実行されない |
  | job timeoutを超える実行中のjob(冪等でretry許可) | `timed_out`になる。再実行されない |
  | 完了済みexecutionの終了報告を重複して送信 | 同じ報告は受理(200)、矛盾する結果と前の段階の再送は拒否(409)し、jobは変わらない |

  テストが本当に障害を検出できることは、実装を意図的に壊して確認しました。復元後のnodeの沈黙を数えない、retryの待機期限を復元しない、`Lost`への遅れた結果を拒否する、の3つで、対応するテストがそれぞれ失敗します。この確認で、待機期限の復元は当初のテストでは検出できないことがわかり、再起動直後の待機表示を検証に加えました。

### 完了条件の確認

| 完了条件 | 状態 |
|---|---|
| controller再起動後にnodeとjobの既知状態を復元できる | 達成。SQLiteへ保存し、起動時に復元する |
| nodeからの再接続情報と永続状態をreconcileできる | 達成。復元した実行中のexecutionを、nodeの最初のpollで確認する |
| retryが新しいattemptとして記録される | 達成。手動と自動のどちらも、同じjobに新しいexecutionを追加する |
| 非冪等jobを無条件に自動再実行しない | 達成。冪等の明示を必須とし、既定では再実行しない |
| 障害注入testで切断、timeout、重複messageを検証できる | 達成。切断と重複は上の表のとおり。timeoutは、execution timeoutとjob timeoutのどちらも、実processで止まり、冪等でretryを許可したjobでも再実行されないことを確認している |

### 既知の制約

- 保存の失敗は、job投入を除き、ログに残して次の保存で再試行するだけです。
- `Lost`にした後もprocessが動き続けている場合、controllerの帳簿上は資源が空いたことになり、そのnodeへ過剰に配置し得ます。nodeが戻って結果を報告するまでの間です。
- DBは平文です。認証と暗号化はPhase 7で扱います。

## 9. Phase 7 — Security and Resource Limits

### 目的

「任意コードを別マシンで実行する」システムとして必要な信頼境界を明確にします。

### 実装対象

- controllerとnodeの相互認証
- encrypted transport
- node join approval / revoke
- CLI利用者の認証と認可
- secretの安全な受け渡し
- command allow/deny policyの検討
- nativeで動くjobのCPU、memoryの制限

container executorは、このphaseでは行いません(下の「見送ったこと」)。

### 完了条件

- 未承認nodeがclusterへ参加できない。
- 未承認clientがjobを投入できない。
- 通信が暗号化され、identityを検証できる。
- 予約を超えてCPUやmemoryを使うjobを、対応するOSで抑えられる。対応しないOSでは、できないことが明示される。
- 実行権限と残存ファイルのcleanup方針が文書化・testされている。

### 進め方

小さな段階に分けて進めます。

| 段階 | 内容 | 状態 |
|---|---|---|
| 7a | 共有トークンによるclient / nodeの認証 | 完了 |
| 7b-1 | nodeのjoin承認と失効(状態、CLI、永続化) | 完了 |
| 7b-2a | node専用の秘密(個別の失効を有効にし、他のnodeになりすませなくする) | 完了 |
| 7b-2b | 事前登録用の、一回限りの参加トークン | 見送り |
| 7c | TLSによる暗号化とcontrollerの検証(自己署名証明書とフィンガープリントの固定) | 完了 |
| 7d | nativeで動くjobの資源制限(Linuxはcgroup、WindowsはJob Object、macOSはできる範囲) | 未着手 |
| 7e | cleanup方針の文書化、secretの受け渡し | 未着手 |

### 見送ったこと

**一回限りの参加トークン(7b-2b)**: 新しいnodeの初回登録で、参加用トークンを持つ者が先に別の秘密を結び付けられる、という隙を塞ぐ仕組みでした。信頼されたLANで、承認(`meld approve`)の前に運用者が`meld nodes`のIDと実際に起動したnodeを突き合わせる運用なら、隙は小さく、仕組みを増やす価値が見合わないと判断しました。台数が増えて承認の手間が問題になったときに、改めて検討します。

**container executorとExecutorの抽象化**は、Phase 7から外し、Phase 9へ移しました。

- Meldの前提は、自分の環境で自分のjobを動かすことです。想定する脅威は悪意ではなく、事故(暴走、書き間違い、資源の使い切り)で、その多くは認証、暗号化、資源制限で足ります。
- containerの中で動くのはLinuxのprocessだけです。Windowsのnodeでは、nativeのjobとは別物になり、OSごとの保守が増えます。
- 「将来使うかもしれない」だけの抽象化は作らない、という方針に従い、`NativeExecutor`を差し替える境界は、container executorが必要になった時点で作ります。
- containerは強い壁ではありません(カーネルを共有します)。信頼できないコードを動かす用途には、別の隔離が要ります。そのような利用が出た時点で改めて設計します。

### 現在地

7aまで完了しています。

- 役割: controllerのAPIは、`meld`が使うclient用、`meld-node`が使うnode用、ファイル転送の両方用に分けています。経路を追加するときは`router_with_auth`でどのグループに入れるかを決めます。
- トークン: 役割ごとに別のトークンを持ちます(`MELD_CLIENT_TOKEN`、`MELD_NODE_TOKEN`)。controllerはハッシュだけを保持し、ハッシュ同士を定数時間で比較します。ログとエラーには出ません。短いトークン、片方だけの設定、同じ値の設定は起動時に拒否します。
- 失敗の応答: トークンがない、または誤っていると401(`WWW-Authenticate: Bearer`)、他の役割のトークンだと403です。`meld`とnodeは、設定すべき環境変数をエラーに示します。tokenが合わないnodeは再試行せず終了します。
- 既定: loopbackでは認証なしでも動きます(警告あり)。loopback以外で待ち受けるにはトークンが必須です。clientは転送先のhostを越えるリダイレクトにトークンを渡さないよう、リダイレクトを辿りません。
- protocol versionは上げていません。メッセージの形は変わらず、認証は通信の外側にあります。
- test: 全経路について、トークンなし・誤り・役割違い・正しい場合を確認するtest(`crates/meld-controller/tests/auth.rs`)と、実processのcontroller、node、`meld`でのend-to-end test(`crates/meld-cli/tests/authentication.rs`)があります。経路の許可を意図的に緩めると、前者が失敗することを確認しました。

### 既知の制約(7a時点)

- 通信が平文なので、トークンは盗聴で漏れます。7cで暗号化します。
- トークンはclientとnodeでそれぞれ共有です。利用者を区別することはできません。漏れたら全員のトークンを替えます。
- トークンは環境変数で渡します。同じ権限のユーザーは`/proc`などから読めます。

### 7b-1の現在地

nodeの参加に、運用者の承認を必須にしました。

- 承認状態: nodeは`Pending`(承認待ち)、`Approved`、`Revoked`のいずれかの承認状態を持ちます。生存状態(`Ready`や`Unreachable`)とは別の、運用者の決定です。承認状態は永続化され、controllerの再起動後も残ります。承認状態の項目がない古い保存データは、すでに使われていたnodeとして`Approved`にします。
- 既定: 認証が有効なときは、初めて見るnodeは`Pending`になります。認証が無効のとき(loopbackの開発用途)は、承認する相手がいないので、すべて自動で承認します。
- 承認待ちのnode: 登録はでき、`meld nodes`に`pending`と表示されます。heartbeat、割り当ての取得、結果の報告は403(`NodeNotApproved`)で断られ、jobは配置されません。nodeは終了せず、heartbeatの間隔で登録をやり直して待ちます。
- 操作: `meld approve <node-id>`と`meld revoke <node-id>`。どちらも冪等です。失効したnodeは、もう一度`approve`すると復帰できます。
- 失効: そのnodeの要求は403(`NodeRevoked`)で断ります。長いpollで待っていた場合も、すぐに断ります。登録のやり直しでも、記録は書き換えさせません。nodeは理由を表示して終了します。失効した時点で生存の記録を止めるので、実行中だったexecutionは、通信が途絶えたnodeと同じ手順(猶予後に`Lost`)で扱い、後から届く結果は受け付けません。再承認すると、通常の遅れた結果として扱われます。
- protocol: versionを8に上げました。`RegisterNodeResponse`に`admitted`、`ProtocolError`に`NodeNotApproved`と`NodeRevoked`、`NodeState`に`Pending`と`Revoked`を追加しています。
- test: 承認の流れと、失効後の拒否、再登録で承認が戻らないこと、再起動をまたぐ承認、旧形式の保存データをAPIの単体testで確認します。実processでは、承認待ち→承認、実行中の失効、失効したnodeの再起動、controllerの再起動をまたぐ承認を確認します。沈黙の判定を生存状態に戻す変更と、heartbeatの確認を外す変更で、対応するtestが失敗することを確かめました。

### 7b-2aの現在地

nodeが別のnodeになりすませないようにしました。

- 設計: nodeは初回起動時に、自分だけの秘密を作ります(`<state dir>/node-secret`、Unixでは所有者だけが読める権限)。登録時に秘密そのものではなくsha256のダイジェストを送り、controllerは、その`NodeId`に最初に見たダイジェストを結び付けます。以後、その`NodeId`として動けるのは、その秘密を持つ呼び出し元だけです。controllerは秘密を発行も保管もせず、ダイジェストだけを持ちます。
- 呼び出し元の区別: 認証の呼び出し元は3種類で、それぞれが使える経路が違います。
  - client(`MELD_CLIENT_TOKEN`): `meld`の操作と、ファイルの転送。
  - 参加を求めるnode(`MELD_NODE_TOKEN`、cluster共通の参加用トークン): **登録だけ**。heartbeatやpollは、このトークンではできません。
  - 登録済みのnode(自分の秘密): 自分としてのheartbeat、割り当ての取得、結果の報告、ファイルの転送(承認済みの場合)。他のnodeの`NodeId`を名乗る要求は403(`NodeIdentityRejected`)です。
- 登録のやり直し: 登録済みのnodeは、参加用トークンがなくても、自分の秘密で登録し直せます(参加用トークンを変えても動き続けます)。結び付いた秘密と違うダイジェストでの登録は、参加用トークンを持っていても断ります。記録も書き換えさせません。
- 失効と復旧: 失効したnodeの秘密は残し、「失効した」とそのnode自身に伝わるようにします。再承認(`approve`)のときに、結び付きを外して新しい秘密を受け付けます。秘密を失ったnodeは、`revoke`して`approve`してから再起動すると復旧できます。
- 移行: 7b-2a以前に保存されたnodeは秘密を持たないので、最初に提示された秘密を結び付けます(次の既知の制約)。
- protocol: 登録要求に`credential`、`ProtocolError`に`NodeIdentityRejected`を追加しました。versionは7b-1と同じ8のままです(まだ公開していません)。
- test: 全経路×呼び出し元(なし、誤り、client、参加用トークン、承認済み、承認待ち、失効)の表、他のnodeとしてのheartbeat・poll・報告の拒否、登録の乗っ取りの拒否、再起動後も結び付きが残ること、再承認での付け替え、旧形式の保存データの移行を単体testで確認します。実processでは、別のstate dirから同じIDを名乗るprocessが拒否されて終了すること、秘密を失ったnodeの復旧、秘密がログと一覧に出ないことを確認します。照合を外す変更と、別の秘密での再結び付きを許す変更で、対応するtestが失敗することを確かめました。

### 既知の制約(7b-2a時点。7cで暗号化を足した現在の状態は下)

- 秘密は、登録のときも以後の要求でも、平文で送られます。盗聴されると、そのnodeになりすませます。7cで暗号化します。
- 結び付きは「最初に見た秘密」です。初めて登録する瞬間(新しいnodeの、または再承認直後のnode)は、参加用トークンを持つ者が先に別の秘密を結び付けられます。新しいnodeは承認待ちになるので、運用者は`meld nodes`の表示と実際に起動したnodeを突き合わせます。事前に結び付けを確定する仕組みは7b-2bで扱います。
- 7b-2a以前に保存されたnodeは、最初に提示された秘密を結び付けます。更新後、そのnodeが再接続するまでの間は、参加用トークンを持つ者が先に結び付けられます。
- 承認は、運用者がIDを確かめて行います。ただし、そのIDの持ち主が本物かは、結び付きより前には確かめられません。
- 参加用トークンはcluster全体で共有です。`MELD_NODE_TOKEN`を替えるには、controllerと、これから参加するnodeの設定を揃えます。登録済みのnodeは、自分の秘密で動き続けられます。

### 7cの現在地

認証が有効なとき、controllerとの通信を暗号化します。

- 方式: controllerは初回起動時に自己署名証明書を作り、state dirに保存します(`tls-cert.pem`、`tls-key.pem`。鍵はUnixでは所有者だけが読める権限)。証明書のsha256を`tls-fingerprint`に書き、起動時にも表示します。`meld`とnodeは、そのフィンガープリントを`MELD_CONTROLLER_FINGERPRINT`で受け取り、一致する証明書だけを信頼します。CAも、clientの証明書もありません。SSHで既知のホスト鍵を確かめる方式に近いものです。
- 検証: 名前や期限は見ず、フィンガープリントの一致だけを見ます。ハンドシェイクの署名は検証するので、証明書を再生するだけの相手は通りません。
- 有効にする条件: 認証が有効なとき(トークンを設定したとき)だけTLSです。認証なしのloopback(開発用途)は、これまでどおり平文のHTTPです。TLSの有無を別の設定にしないのは、秘密が流れるときは必ず暗号化する、という対応にして迷う余地を減らすためです。
- 接続先: 認証ありのcontrollerには`https://`で接続します。`meld`とnodeは、トークンがあるのにloopback以外へ`http://`で接続しようとすると、警告します。
- 独自の証明書: 運用者が自分の証明書と鍵を`tls-cert.pem`と`tls-key.pem`として置くと、そのまま使います。片方だけがあるときは、勝手に作り直さず、エラーにします。`MELD_CONTROLLER_FINGERPRINT`を設定しなければ、通常の(公的なCAによる)検証になります。
- 接続の受け付け: ハンドシェイクは別のtaskで行い、10秒で打ち切ります。何も話さない相手が、後ろの接続を止めることはありません。
- エラー: 設定の誤りが分かるよう、接続エラーは原因まで表示します(「証明書がピン留めされたフィンガープリントと一致しない」など)。
- test: 単体testで、証明書が一度だけ作られること、鍵の権限、片方だけのときの扱い、フィンガープリントの表記ゆれ、ピン留めした相手とだけ通信できること、別の証明書を拒否することを確認します。実processでは、正しいフィンガープリントで通信でき、設定なし・別の値・値の誤りでは通信できないこと、平文で話しかけても応答しないこと、誤ったフィンガープリントのnodeが参加せず理由を表示することを確認します。ピン留めの検証を外す変更で、対応するtestが失敗することを確かめました。

### 既知の制約(7c時点)

- フィンガープリントの配布は、運用者が手で行います。controllerのログと`tls-fingerprint`から読み取り、各nodeと`meld`の設定へ入れます。誤った値を入れたnodeは、再試行を続けて終了しません(ログに原因が出ます)。
- 証明書を作り直す(state dirの`tls-*`を消す)と、フィンガープリントが変わります。すべてのnodeと`meld`の設定を替える必要があります。
- 認証が有効なときは必ず暗号化されます。暗号化だけを、認証なしで有効にすることはできません。
- 証明書の失効や、期限の管理はありません。固定したフィンガープリントがすべてです。

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
- container executor(Executorの抽象化を含む。Phase 7から移した。理由はPhase 7の「見送ったこと」)

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
