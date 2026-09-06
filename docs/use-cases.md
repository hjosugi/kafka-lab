# 5用途をコードとつなぐ

## 1. Log processing

イベントを送信する部分は `transport.emit_events()`、保持と集計は
`sql/risingwave/001-events.sql` にあります。Kafkaは受信イベントを保持し、RisingWaveが
独立したconsumerとして読み取ります。読み終わったことだけでKafkaのデータは消えません。
削除はretentionやcompactionなどトピックの設定によって決まります。

11件の入力には同一payloadの重複が1件あります。`events` の `DISTINCT` を通すため
checkoutのERRORは4件ではなく3件になります。SQLは同じIDで異なるpayloadが届くケースを
検出しません。付属producerの入力検証がimmutableなID契約を確認します。
実システムでは契約違反を取り込み前に隔離し、重複除去stateの保存期間も決めます。

## 2. Recommendations

閲覧=1、カート=3、購入=5というスコアをuser/categoryで集計します。
u1がbooksを閲覧・カート追加・購入した結果は9です。
これは「最近の行動を素早く推薦に反映する」流れの最小例です。
候補商品取得、在庫除外、ランキングモデル、A/Bテストは実装範囲外です。

実用化する場合、無期限の累計を直近30分などの特徴量に置き換えます。
時間減衰・event-time join・削除要求・モデルバージョンも設計項目です。
vector DBを追加しただけで推薦品質が上がるとは限りません。

## 3. Monitoring and alerting

serviceとイベント発生時刻の分で集計し、3件以上かつERROR率50%以上を判定します。
checkoutは4件中3件ERROR、catalogは1件中0件です。
遅れて届いたイベントも、そのイベント時刻の過去の分に反映します。

このSQLは `DATE_TRUNC` による更新可能な履歴集計であり、watermarkで窓を確定させる例ではありません。
古いアラートも残るため、そのまま現在の障害通知に使わないでください。
通知サービスには対象時刻の絞り込み、再通知抑制、回復通知、cooldownが必要です。

一般的なmetrics/logs/tracesの収集にはOpenTelemetryと各バックエンドを使えます。
Kafkaは大量イベントのbuffer、再処理、複数consumerへの配信が必要なときに追加します。
OpenTelemetry自体は長期保存DBではありません。

## 4. CDC

PostgreSQLは `wal_level=logical`、publicationは `orders` のみに設定しています。
RisingWaveはnative CDC connectorでsnapshotと変更ストリームを取り込みます。
別サービスとしてDebeziumとKafka Connectを立てない構成です。

`demo` は初期2行の到着を待ち、その後UPDATE・DELETE・INSERTを行います。
最後に `cdc_orders` と `revenue_by_customer` の正確な期待値を待ちます。
単に `COUNT(*) > 0` を見るテストではありません。

snapshot/replication slotの管理はconnectorが担当します。停止中に保持されるWAL量、
slotの遅れ、DDL互換性、レプリカでの制約は本番運用で監視してください。
RisingWaveはPostgreSQL wire protocolに対応しますが、PostgreSQLの全機能と互換ではありません。

## 5. System migration

`write_order()` は注文変更とoutboxレコードを同じPostgreSQL transactionで保存します。
`publish_outbox()` はKafkaのackを確認してから配送済みにします。
`consume_snapshot()` はSQLiteのtransactionをcommitしてからKafka offsetをcommitします。

この順序では重複が起き得ます。移行先のinboxでevent IDを記録し、entity versionで古い変更を無視します。
削除後もversionを残すため、遅れて届く古いINSERTで注文が復活しません。
失敗したイベントはoffsetを進めず処理を失敗させます。暗黙にskipしません。

本例は単一writerの増分移行と照合を学ぶものです。大規模DBの初期snapshot、
無停止cutover、二重書き、ロールバックの自動化は[別途必要](semantics.md)です。
