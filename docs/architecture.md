# Meld Architecture

## 1. プロジェクトの目的

Meld は、複数の物理マシンに分散した CPU、メモリ、ストレージなどを把握し、ジョブを適切なノードへ配置して実行するためのローカル分散コンピューティング基盤です。

中心となる考え方は、利用者が扱う論理的な対象と、実際に処理を担う物理的な対象を分離することです。

```text
Logical Node Pool  -> physical computers
Logical Job        -> process on a selected node
Logical Resource   -> CPU/RAM/storage on physical nodes
Logical Data       -> files stored at one or more physical locations
```

初期バージョンでは「任意のプログラムが複数台を自動的に1台の共有メモリ計算機として使う」ことまでは目指しません。独立したジョブを適切なノードで実行できることを最初の価値とします。

## 2. スコープ

### 初期スコープ

- ノードの登録、一覧、離脱、heartbeat
- CPU・メモリなどの容量と利用状況の収集
- ジョブの投入、状態管理、キャンセル
- 要求資源に基づく実行可能ノードの選択
- 選択されたノードでのプロセス実行
- stdout、stderr、終了コードの回収
- ノード切断とジョブ失敗の検知
- CLIからの状態確認

### 将来スコープ

- 入力データ転送と成果物回収
- データ位置メタデータ、キャッシュ、複製
- data locality を考慮したスケジューリング
- container executor
- 再試行、別ノードへの再配置、checkpoint
- 暗号化通信と鍵ベースのノード認証
- dashboard、外部API、controller冗長化

## 3. 全体アーキテクチャ

Meld は Control Plane、Execution Plane、Data Plane の3領域に分けます。この分割は別プロセスや別crateを必ず意味するものではなく、責務を混ぜないための論理的な境界です。

```text
                          User
                            |
                        CLI / API
                            |
                            v
                 +-----------------------+
                 |   meld-controller     |
                 |     Control Plane     |
                 |                       |
                 | Node Manager          |
                 | Resource Manager      |
                 | Job Manager           |
                 | Scheduler             |
                 | Failure Detector      |
                 | Data Metadata (later) |
                 +-----------+-----------+
                             |
                 assignment / status / control
                             |
              +--------------+--------------+
              |                             |
              v                             v
       +-------------+               +-------------+
       |  meld-node  |               |  meld-node  |
       |  Executor   |               |  Executor   |
       +------+------+               +------+------+
              |                             |
              v                             v
           Process                       Process

       <---------- Data Plane (incremental) ---------->
       input transfer / output collection / cache
```

### 3.1 Control Plane

クラスタ全体の望ましい状態と観測された状態を管理し、ジョブをどこで実行するかを決めます。初期実装では単一の `meld-controller` が担当します。

### 3.2 Execution Plane

controllerから割り当てられたジョブを各物理マシンで実行します。`meld-node` がローカルプロセスの開始、監視、停止、結果報告を担当します。

### 3.3 Data Plane

入力データ、成果物、キャッシュを移動・保持します。現在は、controllerを正本とする単純なモデルです。ファイルは内容のsha256で識別し、controllerが保管します。nodeは取得した入力をローカルcacheに置き、jobのworkspaceへコピーします。汎用分散ストレージは別段階です。

## 4. crate の責務

### 4.1 `meld-controller`

クラスタ全体の判断を担う control plane の実行バイナリです。

- node registry と heartbeat の受付
- 最新のresource snapshotの保持
- jobの作成と状態遷移の管理
- schedulerの実行とnodeへのassignment
- cancel、drain、retryなどの制御
- CLI/APIからの照会受付
- 将来的なdata location metadataの管理

controllerは「ジョブをどこで実行するか」を決めますが、OSプロセスを直接起動しません。

### 4.2 `meld-node`

各物理マシンをMeldクラスタへ参加させる常駐バイナリです。

- node identity とcapabilityの報告
- heartbeat とresource usageの送信
- job assignmentの受信
- working directoryの準備
- executorによるprocessの開始、監視、停止
- stdout、stderr、終了コード、使用資源の報告
- 将来的なローカルcacheとdata transfer

nodeは「割り当てられたジョブをどう実行するか」を担い、クラスタ全体を見た配置判断は行いません。

### 4.3 `meld-core`

controllerとnodeの双方が使うdomain modelとprotocol contractを提供するlibrary crateです。

- `NodeId`、`JobId`などの識別子
- node、resource、job、executionのデータ型
- job/nodeの状態と状態遷移
- controller-node間のrequest、event、response
- 共通error表現とprotocol version

ネットワーク接続、永続化、process起動の具体実装は置きません。共有型を通信ライブラリやOS依存コードから独立させます。

## 5. controller 内部の主要コンポーネント

### Node Manager

どの物理マシンがクラスタに存在し、現在利用可能かを管理します。

- join / approve / leave / remove
- identity、hostname、OS、architecture、capability
- `Ready`、`Draining`、`Unreachable`などの状態
- heartbeatの最終受信時刻

### Resource Manager

各nodeの静的容量と動的利用量を分けて扱います。

- capacity: CPU数、総メモリ、GPU、storageなど
- usage: CPU負荷、空きメモリ、実行中jobなど
- reservation: jobへ割り当て済みだが未使用の資源

OSが報告する「空き」とMeldが割り当て可能と考える「空き」は同じとは限らないため、将来的にはreservationを明示的に管理します。

### Job Manager

利用者の要求を論理的なjobとして管理します。

- submit、query、cancel、retry、history
- command、environment、working directory
- resource requirements
- assigned nodeとexecution attempt
- timestamps、exit code、result

推奨する基本状態遷移は次のとおりです。

```text
Submitted -> Queued -> Assigned -> Running -> Succeeded
                  |         |          |
                  |         |          +------> Failed
                  |         +-----------------> Lost
                  +---------------------------> Cancelled
```

再試行は同じjobに新しいexecution attemptを作ります。これにより、論理jobの履歴と個々の実行を混同しません。

### Scheduler

実行可能なnodeを絞り込み、その中から実行先を選びます。

初期実装では次の2段階で十分です。

1. filter: nodeがReadyで、要求CPU・メモリ・capabilityを満たすか
2. score: 利用率や予約量が低いnodeを優先する

将来はGPU、affinity/anti-affinity、data locality、network cost、電源状態などをscoreへ追加できます。schedulerはprocessを実行せず、選択結果を返す純粋な方針コンポーネントとして保ちます。

### Failure Detector

正常・異常の事実を検知し、状態へ反映します。

- heartbeat timeoutによるnodeの`Unreachable`化
- connection lossの検知
- 非0 exit codeやsignalによるjob failureの検知
- running node消失時のexecution `Lost` 化

再試行するかどうかはjob policyの判断です。検知と復旧方針を分離することで、意図しない重複実行を避けます。

## 6. 主要な処理フロー

### 6.1 node の参加

```text
meld-node -> controller: JoinRequest(identity, capacity, capabilities)
controller -> meld-node: Accepted(node_id, protocol_version)
meld-node -> controller: Heartbeat(resource_snapshot)
controller: node becomes Ready
```

MVPでは設定済みtokenなどの単純な認証でも構いませんが、node IDをhostnameだけに依存させない設計にします。

### 6.2 job の実行

```text
User -> controller: UploadBlob(sha256, content)      # controllerが持たない入力だけ
User -> controller: SubmitJob(command, requirements, input/output manifest)
controller / Job Manager: create Job, state = Queued
controller / Scheduler: query ready nodes and resources
controller / Scheduler: select node
controller -> meld-node: AssignExecution(job, attempt)
meld-node -> controller: Accepted
meld-node <- controller: GetBlob(sha256)            # cacheにない入力だけ。hashとサイズを検証
meld-node: place inputs in workspace, start process
meld-node -> controller: Running
meld-node -> controller: logs / status
meld-node -> controller: UploadBlob(output)          # 正常終了時、宣言された出力だけ
meld-node -> controller: Finished(exit code, usage, output manifest)
controller / Job Manager: Succeeded or Failed
User <- controller: status / logs / result
User <- controller: GetBlob(sha256)                  # meld fetch
```

assignmentには一意なattempt IDを持たせ、再送時にも同じprocessを二重起動しないidempotencyを目指します。

データの移動には次の規則があります。

- 宣言されたファイルだけが移動します。入力はmanifestにあるものだけがnodeへ届き、出力は宣言されたものだけが回収されます。
- 受け取る側が必ずsha256とサイズを検証し、一致したときだけ確定します。途中の失敗が完成したファイルのように見えることはありません。
- 入力の取得と出力の回収に失敗した場合、processは起動せず、または成功扱いにならず、jobはFailedとなり、型付きの理由(`DataFailure`)が記録されます。processの失敗とは区別されます。
- staging中のcancelとjob timeoutは、転送の完了を待たずに反映されます。execution timeoutはprocessの起動後だけを数えます。
- 未完了のjobが参照する入力と、完了から一定期間内の出力は、容量不足でも削除されません。

### 6.3 node 障害

```text
heartbeat stops
    -> node = Unreachable
    -> executions on node = Lost or Unknown
    -> job policy evaluates retry
    -> optional new attempt on another node
```

通信断だけではprocessが停止したと断定できません。MVPで自動retryを行う場合は、重複実行しても安全なjobに限定するか、少なくとも重複の可能性を状態として明示します。

## 7. 通信とprotocol

初期段階ではHTTP/JSONを使用します。nodeからcontrollerへ接続し、assignmentはlong pollingで取得します。QUIC、gRPC、P2P transportは実測で必要性が明確になってから比較します。

transportより先に、次のcontractを安定させます。

- protocol version
- message ID / correlation ID
- node ID / job ID / attempt ID
- request、acknowledgement、eventの区別
- timeoutと再送時の扱い
- unknown fieldやversion mismatch時の扱い

ログ本文などの大量データは、control messageと同じ経路を使い続けるかを後の段階で再評価します。

## 8. 状態と永続化

最初はin-memory stateでend-to-endの流れを検証します。その次に、controller再起動後も必要な状態をローカルDBへ永続化します。

永続化候補は次のとおりです。

- node identityと承認状態
- job specificationと状態履歴
- execution attemptとassignment
- 最終結果とtimestamps
- 将来的なdata location metadata

高頻度なresource sampleやheartbeatをすべて永続化する必要はありません。現在値と監査・debugに必要な履歴を分けます。

## 9. 信頼性と安全性の原則

- at-most-onceを暗黙に仮定せず、再送と重複を設計に含める。
- nodeの切断とprocessの停止を同一視しない。
- schedulerによる選択とresource reservationを将来的にatomicに扱う。
- user commandは任意コード実行であるため、参加nodeと操作主体を認証する。
- jobが作ったworkspaceの中身は信頼しない。出力を読むときはsymbolic linkを辿らず、宣言されたpathだけを読む。
- 外部から受け取ったpath(manifest、controllerの応答)は、使う側で再検証する。
- secretをjob definitionや通常ログへ平文で残さない。
- native executorから始めても、将来container executorへ差し替えられる境界を保つ。
- root権限を前提にせず、最小権限でnodeを動かす。

## 10. Observability と操作

分散システムでは「何が起きているか」が分かること自体が主要機能です。

初期CLIが回答できるべき質問は次のとおりです。

- どのnodeが存在し、Readyか。
- 各nodeの容量と現在の負荷はどうか。
- jobはどの状態で、どのnodeに割り当てられたか。
- processはいつ開始・終了し、exit codeは何か。
- なぜscheduleできない、または失敗したのか。

構造化ログには少なくともnode ID、job ID、attempt ID、message IDを含めます。metricsやdashboardは、その基礎が整ってから追加します。

## 11. 明示的に後回しにする設計課題

### Distributed Shared Memory / Single System Image

複数nodeのメモリやprocessを完全に透過化するには、通常のjob schedulerとは異なる一貫性、page migration、failure semanticsが必要です。初期architectureの延長として安易に実装せず、独立した研究・設計テーマとして扱います。

### Controller High Availability

controllerを複数化すると、leader election、replicated log、split brain対策が必要です。単一controllerで状態モデルと復旧方法を検証してから導入します。

### 汎用分散ストレージ

Meldに必要なのは、まずjob input/outputの移動とlocation metadataです。content addressing、replication、consistencyを備えた汎用ストレージは、それだけで別の大きなシステムになるため段階を分けます。
