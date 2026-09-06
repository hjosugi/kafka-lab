# Arroyo: KafkaからKafkaへのフィルタ

主要デモ実行後、`python3 scripts/compare.py arroyo` でAPI経由のpipeline作成とKafka出力照合を行えます。
手動でUIを使う場合はルートから:

```bash
docker compose -f compose.yaml -f examples/arroyo/compose.yaml up -d arroyo
```

[Arroyo UI](http://localhost:5115)のCreate pipelineで `errors.sql` を入力して実行します。
UIでpipelineが稼働したことを確認してから、入力を再送します。

```bash
bash scripts/lab.sh events
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic lab.arroyo.alerts --from-beginning --max-messages 4 --timeout-ms 30000
```

1回の入力再送につき、ERROR 4レコードを期待します。event_id=e002が2回含まれます。
sourceが過去の入力から読む設定なら最初の入力も出力されるので総件数は増えます。
この例は単純なフィルタで、重複除去・集計・通知抑制はしません。

0.15.0の[公式発表](https://www.arroyo.dev/blog/arroyo-0-15-0)ではIceberg sinkが紹介されています。
Flinkの全API・connectorと互換であるという意味ではありません。
UI/SQLで処理を組み立てる感触を比較する例です。

Docker imageの起動方法は[公式Getting started](https://doc.arroyo.dev/getting-started/)に基づきます。
このComposeではpipeline metadataの永続化を設定していません。再作成時にはSQLを再投入します。
実行結果は[検証記録](../../docs/validation.md)を参照してください。
