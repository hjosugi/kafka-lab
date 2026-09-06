# Flink: event timeとwatermark

Kafka connectorの公式対応表に合わせてFlink 2.2.1 + connector 5.0.0-2.2を使用します。
Flink 2.3.0が公開済みでも、別配布connectorの互換性を確認して採用する必要があります。
[公式対応表](https://flink.apache.org/downloads/)

新規の主要デモ実行後、`python3 scripts/compare.py flink` で起動・SQL投入・期待値照合を自動実行できます。
この例は入力11レコードの環境専用です。`events` / `recovery` / Arroyoによる再送より先に動かします。

以下は手動で追う場合の手順です。自動手順と同じ環境で重ねてジョブを投入しないでください。
ルートから主要デモを実行してtopicと入力を用意します。

```bash
bash scripts/lab.sh demo
docker compose -f compose.yaml -f examples/flink/compose.yaml up -d --build flink-jobmanager flink-taskmanager
```

[Flink UI](http://localhost:8081)でTaskManagerが登録されたことを確認してからSQLを投入します。

```bash
docker compose -f compose.yaml -f examples/flink/compose.yaml exec -T flink-jobmanager /opt/flink/bin/sql-client.sh -f /sql/windows.sql
```

ジョブがRUNNINGとなり入力11件を読み終えたら、全partitionへ先の時刻のmarkerを送ります。
有限fixtureだけではwatermarkが窓末尾を越えず、出力されないためです。

```bash
docker compose --profile tools run --rm --entrypoint python lab examples/flink/advance_watermark.py
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic lab.flink.windows --from-beginning --max-messages 2 --timeout-ms 30000
```

00:00〜00:01のcheckoutはrequests=5、errors=4、catalogはrequests=1、errors=0を期待します。
このbaselineは物理レコードを数えるため、RisingWaveの重複除去後の値と異なります。
markerは `kind=watermark` なので集計から除外されます。

`event_time - 5 seconds` のwatermark、1分tumbling window、idle partition timeout、
checkpoint設定を確認できます。すでに確定した窓へ遅れて到着するデータの扱いを考えてください。
Kafka sinkは `at-least-once` です。障害回復後の出力重複を防ぎません。
checkpointからのクラスタ全体の自動復元・HA・savepoint restoreはこの例に含みません。

停止:

```bash
docker compose -f compose.yaml -f examples/flink/compose.yaml down
```

初回のDocker buildはMaven Centralからconnector JARを取得します。プロキシ・ネットワーク制約がある環境では取得設定が必要です。
実行結果は[検証記録](../../docs/validation.md)を参照してください。

自動検証は入力offsetのcheckpointを待ってからmarkerを送信し、出力のservice・件数・窓の開始/終了時刻を照合します。
JARのSHA-256、読み取り権限、checkpoint volumeの所有権をDockerfileで設定しています。
