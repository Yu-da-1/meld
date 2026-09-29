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

virtual workspaceと3 crateを作成し、build、test、lint、個別起動の完了条件を満たしています。structured loggingは、実際のHTTP processを起動するPhase 2で導入します。

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
