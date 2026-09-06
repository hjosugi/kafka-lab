# 学習ロードマップと設計演習

## まず理解する4つの役割

| 役割 | 問うこと | このラボの例 |
|---|---|---|
| Event log | 誰がどこまで読んだか。どこから再処理できるか | Kafka |
| Compute | 更新・集計・joinと状態をどう計算するか | Flink、RisingWave、Materialize、Arroyo |
| Storage | 更新可能な表や履歴をどう共有・永続化するか | Fluss、Iceberg |
| Serving | 計算済み結果をアプリからどう読むか | RisingWave / MaterializeのSQL、SQLite projection |

RisingWaveは複数の役割をまとめます。Icebergはtable formatで、SQL実行engineを別に選びます。
Flussはstorageで、Flinkのwatermarkやcheckpointを学ぶ代わりにはなりません。

## 手を動かす順番

1. `offline` と `test` を実行し、11レコードからERRORが3件になる理由を説明する。
2. `demo` のSQLを読み、Kafka保持・consumer・MVを構成図の各箱に対応させる。
3. `query` で注文1002の削除と1001の金額更新を見る。
4. Flink例でwatermark markerを送り、窓が確定する条件を確認する。
5. `recovery` で同じメッセージが再配送されても注文と集計が変わらないことを確認する。
6. Materialize、Arroyo、Flussを個別に試し、同じ「新しい技術」でも役割が異なることを説明する。
7. Iceberg例を2回実行し、snapshot IDが変わり、現在の集計値は変わらないことを確認する。
8. [英語スクリプト](interview.md)を使い、要件→構成→保証→障害→運用の順に説明する。

## 発展演習

| 演習 | 期待する検証 |
|---|---|
| 1分遅れのERRORを入れる | RisingWaveの過去bucketが更新される。Flinkの確定済み窓との違いを記録 |
| 同じIDで異なる金額を送る | source / targetのどちらで拒否され、offsetがどうなるかを確認 |
| 注文削除後に古いupsertを再送 | tombstoneのversionが復活を防ぐ |
| 推薦を直近30分にする | 時間経過でもscoreが減り、stateが無期限に増えないことを確認 |
| alert通知を設計する | 新規発火・同一窓再評価・解消・cooldownを区別する |
| Kafka保持期間より長く停止する | snapshot再構築の必要性を再現し、欠損を成功扱いしない |
| CDC→Iceberg sinkを追加する | catalog commit、delete表現、読み手の対応、snapshot間の結果を照合 |

演習用の変更は別のCompose projectか初期化できる環境で行います。固定fixture用の`verify`は
新しいイベントの期待値を自動で推測しません。

## よくある誤解

- `acks=all` は現在のin-sync replicaへのack条件です。このラボの1 brokerを冗長化しません。
- consumer groupのoffsetと処理先DBのtransactionは別です。commit順序と重複処理が必要です。
- checkpoint設定だけではメール送信やprint sinkの副作用をexactly-onceにできません。
- CDCはDB変更の転送で、必ずしも「ユーザーが購入した」のようなdomain eventと一致しません。
- streaming databaseがあっても、複数用途の保持・replay・疎結合のためKafkaを残す判断があります。
- 日付が新しい製品やRust実装というだけでは、特定workloadでの速さ・安さは決まりません。

公式の概念説明: [Kafka design](https://kafka.apache.org/41/design/),
[Flink event time](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/time/),
[Iceberg table specification](https://iceberg.apache.org/spec/)。
