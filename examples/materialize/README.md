# Materialize: 同じKafkaデータで比較

公式のローカル用Materialize Emulatorを使います。
`v26.39.0` は確認時に[公式install guide](https://materialize.com/docs/get-started/install-materialize-emulator/)が
示していたimage tagです。実Kafka入力とSQLの期待値一致を確認しています。
Emulatorは本番のself-managed deploymentではありません。

主要デモ実行後、`python3 scripts/compare.py materialize` で自動検証できます。
手動の場合はルートから:

```bash
docker compose -f compose.yaml -f examples/materialize/compose.yaml up -d materialize
docker compose -f compose.yaml -f examples/materialize/compose.yaml --profile tools run --rm --entrypoint python lab examples/materialize/run.py
```

Kafka connection、source、source由来のtable、JSONのtyped view、materialized viewを作ります。
RisingWave例と同じcheckout ERROR=3、INFO=1、catalog INFO=1が期待値です。
`run.py` はSQL接続を待ち、期待値に一致しなければ失敗します。

sourceとdecode用tableを分ける[新しいKafka source構文](https://materialize.com/docs/sql/create-source/kafka-v2/)を使います。
SQLの記法やcluster管理はRisingWaveと同一ではありません。
この最小例の重複除去はevent_id/service/level単位で、入力IDがimmutableであることを前提とします。

このComposeにはEmulatorの永続化を設定していません。比較後にコンテナを作り直すとSQLを再登録します。
[検証記録](../../docs/validation.md)を参照してください。
