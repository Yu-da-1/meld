# 実機で試したいこと

## 1. この文書の目的

この文書は、現在のMeldを家庭内・研究室内の実機で使い、何が動き、どこに制約があり、実際の利用で何が問題になるかを確認するための項目をまとめたものです。

期限や実施順は決めません。まずは試したいこと、知りたいこと、確認したい挙動を整理し、実測した事実を残します。現在の対象は、信頼できる家庭内・研究室内の環境でsingle-node jobを安定して実行することです。

## 2. 現在わかっていること

Meldには、node登録、scheduler、資源予約、job実行、データ転送、状態永続化、障害検知、条件付きretry、認証、TLS、メモリ制限があります。

実機では、macOSのcontrollerとLinux (WSL)のnodeを同じLANで接続し、基本的なjob実行、100MiBの入出力、controller/nodeの再起動、nodeの強制終了、GPUを使う推論jobを確認しています。

一方で、次の点はまだ十分に確認できていません。

- 3〜4台の物理マシンが同時に参加したときの配置
- 実際のWi-Fi切断、sleep/wake、マシンのreboot
- 異なるOS、CPU、メモリ、GPUを持つnode間での継続的な利用
- 数百MiBから数GiB規模の転送
- Rust build、Python、CUDA、LLM inference、大量file処理などの実workload
- 設定の保存、自動起動、upgradeなどの日常運用
- TailscaleまたはWireGuardを経由した家の外からの利用
- 長く使ったときのstate、cache、workspace、resource usageの変化

## 3. 試す構成

家庭内の3〜4台を、できるだけ異なる特性を持つ構成で参加させます。

| 役割・特性 | 例 | 見たいこと |
|---|---|---|
| controller | 常時起動しやすいmacOSまたはLinux機 | 状態保存、再起動、データ保管 |
| CPU node | Linux/macOS、CPUとメモリが比較的多い機体 | build、Python、並列job、資源予約 |
| GPU node | CUDAを使えるLinuxまたはWSL機 | GPU workload、capability、メモリ計測 |
| laptop / Wi-Fi node | sleepや回線断が起きる機体 | 切断検知、再接続、sleep/wake |

試した構成を比較できるように、各実機について次を記録します。

- OS、architecture、CPU数、物理メモリ
- GPU、driver、CUDA/runtime
- 有線、Wi-Fi、Tailscale/WireGuardなどの接続方式
- `MELD_CAPABILITIES`、同時実行数、cache容量、メモリ制限
- controller/nodeのstate dirと利用可能ディスク容量
- 実行したMeldのcommit

## 4. 共通して記録したいこと

個々の検証では、成功・失敗だけでなく次の情報を残します。

- 実行した操作とjobの内容
- job ID、attempt ID、選択されたnode、配置理由
- jobの状態遷移と、それぞれのおおよその時刻
- 要求したCPU・メモリと、表示されたreservation
- stdout、stderr、exit code、失敗・retry・待機の理由
- 入出力サイズ、転送時間、cacheを再利用できたか、sha256
- nodeの切断検知、`Unreachable`、`Lost`、復帰までの挙動
- controller/nodeのCPU、メモリ、ディスク使用量
- 復旧に必要だった手動操作
- 表示や操作で分かりにくかったこと

## 5. 実機クラスタ

### 試したいこと

- 3〜4台を同じcontrollerへ登録し、実機とnode IDを対応づけて承認する。
- 異なるOS、architecture、capacity、capabilityを持つnodeを同時に参加させる。
- 各nodeを条件で指定するjobと、複数nodeが候補になるjobを実行する。
- 複数のjobを同時に投入する。
- nodeを`drain`、`resume`する。
- node processとマシンを再起動する。

### 知りたいこと

- nodeの情報は実機の構成を正しく表しているか。
- node identityと承認状態は、processやマシンの再起動後も維持されるか。
- 複数の物理nodeがいる場合も、配置理由を利用者が理解できるか。
- 異種OS/architectureが混在すると、どのような操作や設定の違いが問題になるか。

### 確認したいこと

- OS、architecture、capabilityの制約に反するnodeへ配置されない。
- `drain`中、`Unreachable`中のnodeへ新しいjobが配置されない。
- nodeの重複登録や、意図しないidentityの変更が起きない。
- 再接続後、不要な再承認なしで元のnodeとして復帰する。
- `meld nodes`と`meld status`から、配置結果または待機理由を確認できる。

## 6. Schedulerとresource accounting

### 試したいこと

- CPU・メモリ要求量を変えたjobを複数同時に投入する。
- `MELD_MAX_CONCURRENT_EXECUTIONS`の境界までjobを実行する。
- 小さいjobと大きいjobを混在させる。
- OS、architecture、`gpu`などのcapability制約を組み合わせる。
- nodeを`drain`中、`Unreachable`中、復帰直後にしてjobを投入する。
- Meld外のprocessでCPU・メモリを消費させた状態でjobを投入する。
- 予約を超えるメモリを使うjobと、同じnode上で別のjobを実行する。

### 知りたいこと

- 複数jobのreservationは、実機上でもcapacityを超えずに管理されるか。
- least-loadedの判断は、異なる大きさのnodeでも納得できる結果になるか。
- 大きいjobと小さいjobが並ぶとき、FIFOの方針は実利用に合うか。
- 帳簿上のreservationと、OS全体の実際の負荷はどれくらいずれるか。
- 現在のCPU数とメモリ量だけで、実用上十分な配置判断ができるか。

### 確認したいこと

- Meldが管理するreservationの合計がnode capacityを超えない。
- 同時実行数を超えるjobは、理由付きで待機する。
- 制約不一致、容量不足、空き待ち、FIFO待ちを表示から区別できる。
- 同じ状態で、説明できない配置の揺れや二重実行が起きない。
- メモリ超過jobだけが停止し、同じnode上の他jobは継続する。

## 7. Failure test

### 試したいこと

実行中、入力転送中、出力回収中、idle中のそれぞれで、可能な範囲の障害を起こします。

| 起こすこと | 見たいこと |
|---|---|
| `meld-node`の強制終了 | `Unreachable`と`Lost`、子process、再起動後のcleanup |
| controllerの強制終了・再起動 | 実行継続、DB復元、nodeとのreconciliation、結果回収 |
| Wi-Fi切断・再接続 | timeout、backoff、遅れて届く結果、重複実行 |
| laptopのsleep/wake | sleep防止の限界、到達不能判定、wake後の復帰 |
| nodeマシンのreboot | identity、承認状態、実行中job、workspace cleanup |
| controllerマシンのreboot | 状態復元、証明書とfingerprint、自動起動 |
| 転送中の切断 | 一時file、不完全なblob、checksum、再試行 |
| ディスク不足 | upload、staging、output回収の失敗理由とcleanup |

非冪等jobと、`--idempotent --max-attempts`を指定したjobの両方を試します。

### 知りたいこと

- 通信断とprocess停止を、利用者から見て適切に区別できるか。
- nodeが消えたあと、実際のprocessはどのOSでどのように残るか。
- `Lost`になったjobの遅い結果は、復帰後にどう見えるか。
- どの障害なら自動的に復帰し、どの障害では人の判断が必要か。
- 転送の途中で障害が起きても、壊れたデータが完成済みとして扱われないか。

### 確認したいこと

- jobが理由なく中間状態に残り続けない。
- 非冪等jobが自動的に重複実行されない。
- retryしたjobはattempt履歴を保つ。
- 古いattemptの遅延報告が新しいattemptの結果を上書きしない。
- controller/nodeの再起動後もidentity、承認、job履歴、TLS fingerprintが意図せず変化しない。
- 不完全な転送物や古いworkspaceが、正常な成果物として扱われない。

## 8. 実workload

### 試したいこと

| Workload | 例 | 見たいこと |
|---|---|---|
| Rust build | Meld自身または中規模crateのclean/rebuild | 多数の小file、CPU並列度、長いlog、成果物回収 |
| CUDA | device query、小規模kernel、CUDAを使うbuild | `gpu` capability、driver/runtime依存、終了状態 |
| LLM inference | 小〜中規模modelで複数prompt | model転送/cache、GPUメモリ、初回と再実行の差、出力回収 |
| Python | venvを使うscript、NumPy/PyTorch処理 | interpreterとpackage依存、環境差、再現性 |
| 大量file処理 | 多数fileのhash、圧縮、変換 | manifest上限、staging時間、metadata負荷 |
| CPU/RAM負荷 | 並列計算とメモリ確保 | reservation、メモリ制限、同居jobへの影響 |

### 知りたいこと

- 短いtest commandでは現れなかった問題が、実workloadで発生するか。
- CPU、メモリ、GPU、disk、networkのどれが実際のボトルネックになるか。
- nodeごとの環境差が、jobの失敗や手作業をどの程度生むか。
- 大量のlogやfileを扱うと、controllerとnodeの応答性はどう変化するか。
- LLM modelやbuild treeを再利用するとき、現在のcacheはどこまで役立つか。

### 確認したいこと

- capability指定により、workloadが実行可能なnodeへ配置される。
- stdout、stderr、exit code、成果物を失わず取得できる。
- workloadの失敗が、Meld、OS/driver、依存package、job自身のどこに由来するか判断できる情報が残る。
- メモリ制限とtimeoutが、長い実workloadでも意図したとおりに働く。
- 大量logや大量fileによってcluster全体が応答不能にならない。

## 9. Data transfer stress

### 試したいこと

- 10MiB、100MiB、500MiB、1GiB付近の単一fileを転送する。
- 多数のfileを、合計10MiB、100MiB、1GiB、数GiBになるように転送する。
- 同じ入力を同じnodeと別nodeで再利用する。
- 大きなfileと、多数の小fileを分けて試す。
- 複数の大容量jobを同時に投入する。
- upload、node download、output upload、client fetchの途中で回線を切断する。
- controller quota、node cache上限、node/controllerの空き容量が少ない状態を試す。
- 入力と取得した出力のsha256を比較する。

既定では1 fileの上限が1GiB、1 jobの入力合計が4GiBです。数GiBの検証は、複数fileで既定範囲を試す場合と、上限設定を明示的に変えて試す場合を分けます。

### 知りたいこと

- fileの大きさとfile数が、hash、upload、download、workspace準備、output回収へそれぞれどれくらい影響するか。
- controller経由の転送が、どの規模から実用上のボトルネックになるか。
- controllerとnodeのcacheにより、実際にどれくらい転送を省けるか。
- 同時転送時に、controllerのCPU、メモリ、disk、networkがどう変化するか。
- 外部回線では、どの程度の入出力まで無理なく扱えるか。

### 確認したいこと

- 転送後の内容がsha256と一致する。
- quotaやsize上限を超えた場合、明確な理由で拒否される。
- 同じcontentを再利用した場合、期待したcache hitが起きる。
- 中断後に不完全なblobが完成済みとして参照されない。
- 再接続、再試行、または再投入により転送をやり直せる。

## 10. 日常利用のUX

### 試したいこと

- controller URL、fingerprint、token、state dir、capabilityなどの設定を、再起動後も使える形で管理する。
- shellの環境変数、権限を制限した環境file、OSのcredential storeなどを比較する。
- systemd、launchd、Windows service/Task Schedulerからcontroller/nodeを起動する。
- network確立前やcontroller停止中にnodeを起動する。
- binaryの更新、設定変更、token更新、停止、再起動、rollbackを行う。
- state、blob、cache、logのbackupと復元を試す。
- 普段使う`run`、`status`、`logs`、`fetch`、`cancel`、`retry`、`drain`を繰り返す。

### 知りたいこと

- 毎回手作業になる設定や操作は何か。
- 設定fileまたはservice化が必要なら、どの情報と操作を扱う必要があるか。
- secretを安全に保ちながら、自動起動をどこまで簡単にできるか。
- upgradeや設定変更で、identity、承認、job履歴、証明書を維持できるか。
- 普段の利用で、CLIから不足している情報や操作は何か。

### 確認したいこと

- controllerとnodeを再起動後に、同じclusterとして復帰できる。
- secretがcommand line、job definition、通常logへ意図せず露出しない。
- state dir、cache、blob、logの場所と役割を説明できる。
- 新しいnodeを追加し、不要になったnodeを安全に外せる。
- 起動失敗や接続失敗の理由が、serviceのlogから確認できる。

## 11. 家の外からのjob投入

### 試したいこと

- TailscaleまたはWireGuardを使い、外部回線のclientからcontrollerへ接続する。
- `nodes`、`run`、`status`、`logs`、`fetch`、`cancel`を行う。
- 小さなjobと、入力・出力を伴うjobを実行する。
- mobile tetheringなど、家庭内LANよりlatencyと帯域が悪い回線を使う。
- VPNを切断し、再接続する。
- VPN内のIPやnameが変わる場合の設定を確認する。
- firewallでMeldのportがpublic interfaceへ直接公開されていないことを確認する。

### 知りたいこと

- VPN越しでも、LAN内と同じ認証・TLS fingerprintの仕組みを使えるか。
- latencyと帯域がCLI操作、job投入、log取得、file転送へどう影響するか。
- VPN切断中と、controller/node障害を利用者が区別できるか。
- 外部から大きな入力を送る運用が現実的か。
- clientだけがVPN越しに接続する構成で十分か、node側にもVPN接続が必要になるか。

### 確認したいこと

- VPN接続中、外部clientから基本操作を行える。
- VPN切断中、Meldがpublic internetから直接到達できない。
- 再接続後も、既存jobとnodeの状態を確認できる。
- 接続不能時に、CLIが分かる形でエラーを返す。
- nodeは引き続きcontrollerへ接続する側であり、外部clientから各nodeへの直接接続を必要としない。

## 12. 継続して使う中で見たいこと

普段の検証を続ける中で、時間経過によって現れる次の点を観察します。

### 知りたいこと

- controller/nodeのメモリ使用量が増え続けないか。
- DB、blob、cache、workspace、logがどのように増減するか。
- job retention、output保持、cache eviction、workspace cleanupが期待どおり働くか。
- 繰り返す再接続、再起動、drain/resumeで状態がずれないか。
- 完了または失敗したjobが、理由なく中間状態に残らないか。
- 日常的に人の介入を必要とする事象は何か。

## 13. 結果の残し方

検証結果は、この文書の項目に対応させて次の形で残します。

- 構成とMeldのcommit
- 実行した操作またはworkload
- 期待した挙動
- 実際の挙動と測定値
- 成功、制約、不具合、未確認の区別
- 回避策または次に確かめたいこと
- 自動testへ追加した場合は、そのtest
