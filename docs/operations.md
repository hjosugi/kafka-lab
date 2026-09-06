# 実行・復旧・運用の補足

## コマンドの使い分け

| コマンド | 用途 | 既存データへの影響 |
|---|---|---|
| `bash scripts/lab.sh demo` | 空の環境にfixtureを入れ5用途を照合 | 初回専用。outboxがあれば停止 |
| `bash scripts/lab.sh query` | 現在のビューを表示 | 読み取りのみ |
| `bash scripts/lab.sh events` | 同じ11レコードを再送 | Kafkaは増加。RisingWaveは重複除去 |
| `bash scripts/lab.sh publish` | 未配送outboxを最大1000件送る | ack後に配送済みを記録 |
| `bash scripts/lab.sh consume` | 保存済みoffsetから起動時末尾まで適用 | SQLite・group offsetを更新 |
| `bash scripts/lab.sh replay` | 保持中の注文イベントを再読 | inboxで重複を除去。offsetも更新 |
| `bash scripts/lab.sh verify` | 期待値とsource/target全行を照合 | 読み取りのみ |
| `bash scripts/lab.sh recovery` | commit間の失敗と再配送を試験 | 同じfixtureを再送。デモ実行後専用 |
| `bash scripts/lab.sh down` | サービスとnetworkを停止・削除 | named volumeは残る |
| `bash scripts/lab.sh reset --delete-lab-data` | ラボのvolumeも削除 | このプロジェクトの状態を全消去 |

追加例は各READMEのComposeファイルも指定して停止してください。
Flink例は物理レコード数を数えるため、`events` / `recovery` 実行前の環境で試します。
rootのCompose project名は `kafka-lab`、Flussは `kafka-lab-fluss` です。

## 起動失敗を切り分ける

```bash
docker compose ps -a
docker compose logs --tail=150 kafka postgres risingwave
docker compose config
```

| 症状 | 確認する箇所 |
|---|---|
| imageを取得できない | registryへの接続、tag、CPU architecture。tagの存在と説明記事は別々に確認 |
| Kafkaへの接続が繰り返し失敗 | ホストは`localhost:19092`、コンテナ内は`kafka:9092`。advertised listenerを確認 |
| CDCだけ追いつかない | `wal_level`、publicationの対象、slotの状態、RisingWaveログ |
| 新規SQLやschemaが反映されない | PostgreSQL初期化SQLは空volumeの初回のみ。既存DBには明示的なmigrationが必要 |
| `demo needs a fresh lab` | 既存デモは`query` / `verify` / `replay`を使う |
| `Committed offset has expired` | 保持期間を越えた。snapshotから再構築し照合。通常consumeでskipしない |
| SQLiteだけ失った | 書き込みを止め、完全な注文履歴がKafkaに残ると確認した場合に限りreplayし照合 |
| 一部の集計値が増えた | Flink/Arroyoは重複を数える。RisingWaveは同じpayloadを除去。比較条件を揃える |
| Podmanのshort-name解決エラー | このラボはregistryを含むimage名を使用。Compose providerも確認 |
| Podmanで`mount overlay ... no such device` | ホスト側のoverlayfs対応を確認。fuse-overlayfsなどホストに適したstorage設定が必要 |

`verify` は固定fixtureの検証用です。自分の入力を追加したら、期待値も独立に計算し直します。
タイムアウトを伸ばしてSQLエラーや恒常的な欠損を隠さないでください。

## 再起動と状態の保存場所

```bash
docker compose restart kafka postgres risingwave
bash scripts/lab.sh verify
bash scripts/lab.sh replay
bash scripts/lab.sh verify
```

| volume | 保存する状態 |
|---|---|
| `kafka-data` | Kafkaログ、offset topic、KRaft metadata |
| `pg-data` | 注文、outbox、logical replication slot |
| `rw-data` | standalone RisingWaveのmetadataとstate |
| `target-data` | SQLite projection・inbox・WAL |

コンテナ再起動はホストやvolumeの喪失試験とは異なります。volumeはbackupでもありません。
SQLiteのlive backupはbackup API等で整合性を保ち、DBファイルだけを動作中にコピーしない設計にします。

## 本番へ進むときの設計課題

- Kafka: broker障害を許容するreplica配置、`min.insync.replicas`、TLS/SASL/ACL、容量とretention。
- CDC: 最小権限のreplication user、WAL量、snapshot中の更新、DDL変更、slot喪失時の再同期。
- State: 重複除去期間、lateness、TTL、deleteの保持期限。有限メモリと業務上の正確性を両立させる。
- Serving: readの整合性、P95/P99応答時間、更新反映までの遅延、クエリ単位の負荷制限。
- Secrets: 本例の`lab-only`を実環境へ持ち込まない。秘密情報をGit・image・ログへ含めない。
- 移行: sourceのwrite fence、確定LSN/offset、全キー照合、段階切り替え、rollback期間。

本例のpublisherは有限バッチです。並行publisherは`SKIP LOCKED`によって同じentityの新旧versionを
異なる順序で送る可能性があります。targetのversion比較はそれを扱いますが、全consumerに同じ契約が必要です。
実運用ではentity単位の配送順序、polling間隔、batchサイズ、バックプレッシャーを設計します。

## 容量を見積もる例

仮に10,000 events/s、平均1,000 bytes、7日保持、replication factor=3なら、
圧縮前のpayloadだけで `10,000 × 1,000 × 604,800 × 3 = 18.144 TB`（十進）です。
index、metadata、segment、圧縮率、ディスク空き容量を別に見積もります。これは実測benchmarkではありません。

追う指標は、consumer lagだけでなく、event-timeからview反映までの遅延、checkpoint時間、
outbox最古未配送時刻、CDC slotのWAL保持量、再照合の不一致数です。
APIが速くても更新反映が遅ければ、リアルタイム要件を満たしたことにはなりません。

公式資料: [Kafka operations](https://kafka.apache.org/41/operations/),
[PostgreSQL logical replication](https://www.postgresql.org/docs/17/logical-replication.html),
[RisingWave persistence](https://docs.risingwave.com/operate/rw-standalone-mode),
[SQLite online backup](https://www.sqlite.org/backup.html)。
