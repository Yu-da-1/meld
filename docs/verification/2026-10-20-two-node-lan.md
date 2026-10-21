# 2026-10-20 2 node構成のLAN検証

対象: plan.md の §5(一部)、§6(一部)、§7(BigWaveのnode停止、偶発的なsleep)、§9(一部)
構成: macOS controller(MacBook A)、macOS node(MacBook B)、WSL2 Linux node(BigWave)。3台の物理マシンを同じLANで接続
Meld: `f88c77a`(作業ツリーにREADMEの未コミット変更あり。コードの差分はなし)
結論: 3台構成で、基本動作、OS制約、同時実行上限、drain、1GiBまでの転送、node停止後の復帰とretryは期待どおり。一方、`kill -9` で止めたnodeの子processが残り、再起動後も片付かず、resource accountingにも数えられない点を確認した。controllerの強制終了と再起動(最大90秒の停止)では、jobも状態も失われなかった。quota、転送途中の切断は未確認。

## 1. 構成

| | controller(MacBook A) | Mac node(MacBook B) | BigWave |
|---|---|---|---|
| OS / arch | macOS | macOS / aarch64 | Linux (WSL2) / x86_64 |
| CPU | 15 | 11 | 8 |
| 物理メモリ | 24GiB | 36GiB | 約7.7GiB |
| 同時実行数 | nodeなし | 2(`MELD_MAX_CONCURRENT_EXECUTIONS=2`) | 8(既定) |
| 接続 | - | LAN(Wi-Fiか有線かは未記録) | LAN(同左) |

- 両nodeとも、controllerとは別の機体で動かした。転送時間にはどちらもLAN越しの通信が含まれる。
- MacBook Bは普段使いの機体のため、負荷を抑えて(同時実行2)参加させ、強制終了やreboot、ディスク逼迫のテストは行わない方針。
- controllerのstate dirは検証用の新規ディレクトリ。token、fingerprint、IPは記録しない。
- GPU、Wi-Fi切断、VPNは今回の対象外。

## 2. §5 実機クラスタ

| 確認 | 結果 | 判定 |
|---|---|---|
| nodeが `pending` で現れ、`approve` で `ready` になる | 両nodeとも承認から約5〜8秒で `ready`。hostname、platform、capacityが実機と一致 | 成功 |
| 配置理由の表示 | `selected: lowest load (9.0% after placement)`、`eligible, not chosen: another node was preferred (12.5% ...)` のように、選ばれた理由と選ばれなかった理由が出る | 成功 |
| 重複登録、identityの変更 | 起こらなかった(Mac nodeの再接続後も同じnode ID) | 成功 |

## 3. §6 Schedulerとresource accounting

| 確認 | 結果 | 判定 |
|---|---|---|
| `--os linux` / `--os macos` | 該当nodeにだけ配置。不一致側は `does not satisfy the placement constraints` | 成功 |
| 満たすnodeがない制約(`--os windows`) | `queued`、`queue_reason: constraints_not_satisfied`。理由付きで待機し続ける | 成功 |
| 同時実行の上限(Macの上限2に5件、各 `sleep 20`) | 2件実行、3件待機。待機理由は `no_available_nodes` と `waiting_for_earlier_job` に分かれた。5件とも succeeded | 成功 |
| FIFO | 待機中のjobは先に投入した順に実行された(完了状態のみで確認。開始時刻は未測定) | 成功(粗い確認) |
| `drain` | draining中のMacには配置されず `not accepting jobs (draining)` と表示。BigWaveに配置された | 成功 |
| `resume` | 約6秒後に `ready`(ただしこの時点でスリープによる `unreachable` と重なった。§4参照) | 成功 |
| CPU律速の予約(BigWave 8 CPU に 4CPU/3GiB を3件) | 2件が実行、3件目は `no free CPU right now` で待機。全件 succeeded | 成功 |
| メモリ律速の予約(1CPU/3GiB を3件、約7.7GiB) | 2件が実行、3件目は `no free memory right now` で待機。全件 succeeded | 成功 |
| FIFOの先頭ブロック(上の3件目の後ろに 1CPU/256MiB) | 空きに入る大きさでも `waiting_for_earlier_job` で待たされた。FIFOの方針どおり(不具合ではない) | 制約 |
| メモリ超過job(予約256MiBで約870MBまで使用) | 超過jobだけが `failed`。`memory limit exceeded: used 870801408 bytes, over the limit of 295279001 bytes (268435456 reserved, plus 10%); run it again with a larger --memory` と表示。exit_code は `unavailable` | 成功 |
| 超過jobと同居する別job | 超過の影響を受けず succeeded(exit 0) | 成功 |

BigWave(WSL2)では、メモリ制限が測定でき、超過jobを止められた。

未確認: Meld外のprocessが資源を使っている状態での配置、帳簿上の予約とOS全体の負荷のずれ、孤児processがあるときの予約(§4.2)、MacBook Bでのメモリ制限、least-loadedが大きさの違うnodeで納得できるか(今回は常に1台しか候補がない構成が多く、比較は不十分)。

`windows` 指定のjobは検証後も `queued` のまま残っている(キャンセルはまだしていない)。

## 4. §7 Failure test

### 4.1 偶発的なsleep(MacBook B)

意図した試験ではない。検証中にMacBook Bがスリープした。

- `resume` の約3秒後にMac nodeが `unreachable` になった。controllerのログに `node became unreachable` が出た。
- 復帰後、約3秒で `ready` に戻った。node IDは同じで、再承認は不要だった。ログに再登録の行はなく、スリープ中もnodeプロセスは生きていて、ハートビートが再開しただけと見られる。
- `unreachable` だった時間は約20秒で、`MELD_LOST_GRACE_SECS`(30秒)に入らなかった。そのため `Lost` には到達しておらず、実行中のjobがあった場合の挙動は未確認。
- スリープしたのはMacBook B(nodeの機体)。controllerの機体ではない。controllerの機体から見て `meld-node` のプロセスが存在しなかったのは、別の機体で動いていたため。
- 復帰時、スリープ中もnodeプロセスが生きていたかは、MacBook B側で確認していない。

### 4.2 BigWaveのnode停止(Ctrl+C と `kill -9`)

BigWave上で実行中のjobを2種類用意し、node(`meld-node`)を止めた。非冪等job(`sleep 301` など)と、`--idempotent --max-attempts 3` のjob(`sleep 302`)を使った。時刻はcontrollerのログ(UTC。JSTは+9時間)。

| | Ctrl+C(SIGINT) | `kill -9` |
|---|---|---|
| 停止 | 15:54:44 | 15:59:45 |
| `Unreachable` | 15:54:56(約12秒後) | 15:59:46(約1秒後) |
| `Lost` | 15:55:26(さらに30秒後) | 16:00:16(同左) |
| retry期日(冪等job) | 15:55:31(5秒後) | 16:00:26(10秒後) |
| 子process(`sleep`) | 残らなかった | **残った** |

| 確認 | 結果 | 判定 |
|---|---|---|
| 非冪等job | `lost` で止まり、自動で再実行されなかった | 成功 |
| 冪等job(`--max-attempts 3`) | 1回目・2回目は `lost` として履歴に残り、nodeの復帰後に再実行された。3回目で実行中(最後のattempt) | 成功 |
| nodeの再起動 | 再承認なしで同じnode IDのまま `ready` に復帰 | 成功 |
| `Lost` 後の遅い報告 | 発生しなかったため未確認 | 未確認 |

**不具合・制約(`kill -9` のとき)**

1. nodeを `kill -9` で止めても、job用の子processは終了しない。BigWave上で `sleep 302` と `sleep 303` が生き残った。Ctrl+Cの正常停止では子processは終了した。
2. nodeを再起動しても、孤児processは片付けられない。再起動後に `pgrep -a sleep` で、孤児2つと新しいattemptの1つが同時に動いているのを確認した。
3. controllerの表示は `1 executions running` だったが、実際には3つ動いていた。Meldの資源予約は孤児を数えないため、孤児が重いと予約の合計がcapacityを超える。
4. 冪等jobでは、孤児の古いattemptと新しいattemptが並走した(冪等指定なので許容される範囲だが、副作用のあるjobでは問題になり得る)。非冪等jobでは、`lost` になったあとも実体が走り続けたが、重複実行にはならなかった。

**対応(B案): 起動時に前回のprocess groupを探して止める**

調査と実装は同じ日に行った。作業ツリーの変更として、`crates/meld-node/src/executor.rs` と `main.rs` にある(未コミット)。

- 実行開始時に、process group のIDと開始時刻を `<state dir>/processes/<execution_id>` に書く。実行が終わると消す。
- nodeの起動時、workspaceを消す前に、記録を読んで、同じIDで同じ開始時刻のprocessが生きていれば group ごと `SIGKILL` する。IDだけが同じで開始時刻が違えば、何も止めない(IDの再利用対策)。
- 追加したテスト3件: 孤児が止められる、記録が合わなければ何も止めない、正常終了すると記録が残らない。`cargo test --workspace`、`cargo fmt --check`、`cargo clippy` は通過。
- MacBook A での実機確認: 検証用の controller と node を別ポートで起動し、`sleep 311` の実行中に node を `kill -9`。孤児の `sleep` が残ったことを確認したあと node を再起動すると、`stopped processes left running by an earlier run stopped=1` と出て、孤児が消えた。

この対応で**直らないこと**:

- nodeが再起動するまでは、孤児が走り続ける(node が戻らなければ、そのまま)。
- process group の先頭のprocessがすでに終わっていて、孫processだけが残っている場合は見つからない。
- **Linux(BigWave)では効いていない可能性がある(未解決)。** 同じ手順で `kill -9` のあとに node を再起動したが、`sleep 321` が残っていた。ただし、再起動のログ(`stopped=...` の行)は確認していない。記録ファイル(`processes/`)は作られていた。原因は不明で、開始時刻の照合がLinux(WSL2)でずれている可能性を疑っているが、確認していない。この修正は `009f945` として push 済みのため、Linuxでは効かない前提で扱う。

**幽霊実行(node が `Lost` の猶予内に再起動した場合)**

`kill -9` で止めた node を、`Unreachable` から約22秒後(`MELD_LOST_GRACE_SECS` の30秒より前)に再起動した。

- controller は node が `registered` で戻ったので、実行は続いていると見なした。node は再起動後に前回の実行を覚えておらず、`0 executions running` と報告する。
- 実行は controller 上で `running` のまま、少なくとも30分以上変わらなかった。job は `sleep 321` で、実体はとうに終わっている。
- その実行の予約(1 CPU)が解放されないため、8 CPU を要求する別の job が `no free CPU right now`(`no_available_nodes`)で、20分以上待たされ続けた。
- `meld cancel` は受け付けられたが、`cancelling` のまま進まなかった(node が実行を知らず、取り消しを受けて返答できないため、と見ている)。
- 復旧: node を Ctrl+C で止め、60秒待ってから起動し直した。`unreachable` から30秒後に `execution marked lost` となり、予約が解放されて、待たされていた job が動き出した。これは人の手が必要な復旧である。

**対応(修正済み、未コミット)**

突き合わせの仕組み(`reconcile_node`)は元からあり、node は poll のたびに実行中の実行IDの一覧を送っている。ただし、突き合わせの対象は「controller の再起動で復元された実行」だけだった(通常運用中の poll の一覧は古いことがあり、一覧にないことは「消えた」証拠にならないため)。今回は node だけが再起動したので、対象外だった。

- node が再登録したとき、その node の `Accepted` / `Running` / `Cancelling` の実行を「未確認」に加える(`JobManager::await_confirmation_from`、`register_node` から呼ぶ)。再起動した node の最初の poll の一覧は確かな情報なので、一覧にない実行は `lost` になり、一覧にある実行は残る。`Assigned`(node がまだ受け取っていない実行)は対象にしない。
- テスト3件を追加(再登録後に一覧にない実行は `lost`、一覧にある実行は残る、他の node や `Assigned` は対象外)。`cargo test --workspace`、`cargo fmt --check`、`cargo clippy` は通過。
- macOS での実機確認: 検証用の controller と node を別ポートで起動し、`sleep 200` の実行中に node を `kill -9` して約5秒後に再起動した。再起動の約3秒後に実行が `lost` となり、node は `ready`。猶予(45秒)を待たずに、幽霊実行は残らなかった。
- BigWave では未確認(controller を新しいバイナリで動かす必要がある)。

**表示の問題**

- 冪等jobがnode停止後に待機したとき、`queue_reason` が `constraints_not_satisfied` になった。実際には制約(`--os linux`)は満たしていて、該当nodeが `unreachable` で受け付けないのが原因。個別のnode欄には `not accepting jobs (unreachable)` と出ているので原因は読み取れるが、見出しの理由が実態とずれている。
- `Unreachable` が約1秒で検出された理由(Ctrl+Cでは約12秒)は不明。

### 4.3 controllerの強制終了と再起動

`kill -9` で止め、同じstate dirで起動し直した。node(BigWave)は動かしたまま。時刻はUTC(2026-10-21)。

| 試験 | 内容 | 結果 | 判定 |
|---|---|---|---|
| 停止 約29秒 | 停止前に `sleep 20`(停止中に完了)と `sleep 120`(停止をまたぐ)の非冪等jobを投入 | 再起動後、前者は succeeded でログ `x-done` を回収。後者も succeeded、`y-done` | 成功 |
| 停止 90秒 | 停止前に `sleep 150` の非冪等jobを投入。ハートビートの猶予(15秒 + 30秒 = 45秒)を超える停止 | 再起動の直後は両nodeが `unreachable`。約25秒で `ready` に復帰。jobは `lost` にされず succeeded、`z-done` | 成功 |
| 識別情報 | TLS fingerprint | 再起動前後で同一 | 成功 |
| 過去のjob履歴 | 以前の冪等jobのattempt履歴 | 保たれていた | 成功 |
| 再承認 | 再起動後のnode | 不要 | 成功 |
| 再起動後のログ | 警告・再登録の行 | 再起動直後の `unreachable` を除き、なし | 成功 |

- CLIは、controller停止中に `Connection refused` を分かる形で返した。
- 実行中のjobのログは、完了前は `execution logs are not available yet`(404)になる。controller再起動の影響ではなく、元の設計かもしれない(未確認)。

未確認: `Lost` になったjobの遅い結果の扱い、意図的なスリープ試験と `MELD_PREVENT_SLEEP` の効果、reboot、転送中の障害。

## 5. §9 Data transfer

`cp in.bin out.bin` を実行するjobで、ランダムデータを入力としてアップロードし、出力を `meld fetch` で取得して、入力とのsha256を比較した。所要時間は1秒間隔のポーリングによる実測で、粒度は約1秒。

| 入力 | node | 実行 | 取得 | sha256 |
|---|---|---|---|---|
| 10MiB | BigWave / Mac | 1.0秒 / 1.0秒 | 0.1秒 / 0.1秒 | 一致 |
| 100MiB | BigWave / Mac | 3.1秒 / 3.1秒 | 0.6秒 / 0.6秒 | 一致 |
| 500MiB | BigWave / Mac | 11.2秒 / 11.2秒 | 3.1秒 / 3.1秒 | 一致 |
| 1024MiB(1回目) | BigWave / Mac | 37.5秒 / 24.3秒 | 6.3秒 / 6.5秒 | 一致 |
| 1024MiB(再投入) | BigWave / Mac | 19.3秒 / 5.1秒 | 6.4秒 / 6.3秒 | 一致 |

取得(fetch)はcontroller側の機体へのダウンロードで、node側の機体には依存しない。そのためnode間でほぼ同じ値になる。

| 確認 | 結果 | 判定 |
|---|---|---|
| 1fileの上限(1GiB+1バイト) | CLIが送信前に `exceeds the 1073741824 byte file limit` で拒否 | 成功 |
| 1GiBちょうど | 転送できた | 成功 |
| 1jobの入力file数(2000file) | `a job may have at most 1000 input files` で拒否 | 成功 |
| 1000file(各4KiB、計約4MB) | 投入4.4秒、実行8.1秒、succeeded。全ファイルのsha256が一致 | 成功 |
| nodeのcache | 同じ1GiBの再投入で実行時間が短くなった(BigWave 37.5→19.3秒、Mac 24.3→5.1秒) | 推測のみ(ログでcache hitを確認していない) |

- `meld run` は、入力fileごとに `uploading <path> (<bytes>)` を標準出力に1行出す。1000fileなら1000行になり、出力が読みにくくスクリプトでも扱いにくい。改善候補。
- 一連の転送のあと、controllerのstate dirは約1.6GiBになった。output retention(既定24時間)で解放されるかは未確認。

未確認: 転送途中の切断と不完全なblobの扱い、controller quota、nodeのcache上限、ディスク不足、複数の大容量jobの同時投入、1jobの入力合計の上限(4GiB)、外部回線での転送。

## 6. §8 実workload(Rust build、BigWave)

Meld 自身を BigWave で clean build した。入力は `Cargo.toml`、`Cargo.lock`、`rust-toolchain.toml`、`crates/`(53 file、約971KB)で、`cargo build --release --locked` を実行し、3つのバイナリとログを `--output` で回収した。BigWave の `rustc` は 1.97.1 で、リポジトリのピン留めと同じ。依存 crate は BigWave の `~/.cargo` に取得済みだった。

| 試行 | 設定 | 結果 |
|---|---|---|
| 1 | `--cpu 8 --memory 6GiB`、並列度は既定 | 開始から約8秒で `failed`。`memory limit exceeded: used 8748810240 bytes, over the limit of 7086696038 bytes (6442450944 reserved, plus 10%)` |
| 2 | 同じ設定に `-j 3` を追加 | 約100秒で succeeded(`Finished ... in 1m 37s`、163 crate を compile)。成果物の ELF x86-64 バイナリ3つとログを回収 |

- 試行1の「使用量 8.15GiB」は、BigWave の物理メモリ(約7.7GiB)を超えていて、実際の使用量ではない。Meld は job の process を足し合わせて測るため、`rustc` の多数の process が同じ共有ライブラリを読むと、共有分が重複して数えられる(`limits.rs` に注意書きがある)。試行2の最中、BigWave の空きメモリは約7.78GiB から約7.0GiB までしか減らなかった(10秒ごとの粗い観測)。
- そのため、実 workload(`cargo` の並列 build)で、メモリ制限の誤停止が実際に起きた。表示の「`--memory` を増やして再実行」という案内も、搭載量を超える予約ができないので役に立たない。回避は、並列度を下げる(`-j`)か、node で `MELD_MEMORY_LIMIT=off` にすること。
- 同じ入力の2回目の投入では `53 already on the controller` と表示され、upload は省かれた。
- 取得した成果物は、実行権限(`x`)が付いていなかった(`-rw-r--r--`)。そのまま実行するには `chmod +x` が要る。仕様か不具合かは未確認。
- 取得したバイナリの動作(BigWave 上で実行して `--version` など)は未確認。

未確認: Python、CUDA、LLM推論、大量 file の処理、長い log、リビルド(incremental)。

## 7. 次に確かめたいこと

1. 幽霊実行の対応。node の再登録時に、実行中の実行を突き合わせる。
2. Rust build で起きたメモリ計測の過大評価。共有メモリを重複して数えない測り方(PSS など)、またはその回避策の整理。
3. 孤児processの対応(B案)がLinuxで効かなかった件の原因調査。
4. 意図的なスリープ試験(実行中のjob、`MELD_PREVENT_SLEEP` の効果)。
5. nodeのcache hitをログで確認する。
6. 転送途中の切断、quota、ディスク不足。
7. §8 の残り: Python、CUDA、LLM推論、大量file、長いlog。
