# Versions and official references

確認日: 2026-09-06。最新releaseの紹介と、実装で使う依存関係を区別します。
Webの公式記載を確認したことは、Docker imageのpull・結合試験の成功を意味しません。

| 対象 | 確認した公式情報 | 本実装 |
|---|---|---|
| Kafka | [4.3.1発表、2026-06-25](https://kafka.apache.org/blog/2026/06/25/apache-kafka-4.3.1-release-announcement/) | apache/kafka:4.3.1 |
| Flink | [2.3.0発表、2026-06-25](https://flink.apache.org/2026/06/25/apache-flink-2.3.0-release-announcement/) | Kafka例は2.2.1 |
| Flink Kafka connector | [5.0.0の対応表は2.1.x/2.2.x](https://flink.apache.org/downloads/) | 5.0.0-2.2 |
| RisingWave | [release notes: v3.0.3、2026-08-17](https://docs.risingwave.com/changelog/release-notes) | v3.0.3 |
| Fluss | [0.9系quickstart](https://fluss.apache.org/docs/quickstart/flink/) | 0.9.1-incubating、公式Flink 1.20同梱image |
| Materialize | [Emulator install guide](https://materialize.com/docs/get-started/install-materialize-emulator/) | 公式guide記載のv26.39.0 |
| Arroyo | [0.15.0発表、2025-12-01](https://www.arroyo.dev/blog/arroyo-0-15-0) | 0.15.0 |
| PostgreSQL | [RisingWave CDC対応: PostgreSQL 10〜17](https://docs.risingwave.com/ingestion/sources/postgresql/pg-cdc) | postgres:17 |
| Python client | [Confluent Python client](https://docs.confluent.io/kafka-clients/python/current/overview.html) | confluent-kafka 2.11.0 |
| PostgreSQL adapter | [Psycopg binary install](https://www.psycopg.org/psycopg3/docs/basic/install.html) | psycopg[binary] 3.2.9 |
| PyIceberg | [公式package履歴](https://pypi.org/project/pyiceberg/) | 0.10.0 |
| DuckDB | [1.4.0発表](https://duckdb.org/2025/09/16/announcing-duckdb-140.html) | 1.4.0 |

上記Python client、PyIceberg、DuckDBは最新版の主張ではありません。実装例の固定版です。
Docker imageはversion tagで指定していますがdigest固定ではありません。
`postgres:17` と `python:3.12-slim` はpatch更新を含む可変tag、Pythonの間接依存も完全lockしていません。
厳密な再現性が必要な場合は、必要なplatformでimage digestと依存lockを取得してください。
本ラボのFlink JARはSHA-256をbuild時に検証します。

## 引用文から補正・明確化した点

- RisingWave 3.0.0は公式release notesでは2026-06-11です。引用文の「2026年7月」を補正します。
- Flussの卒業日は[Apache Incubator記録では2026-07-16](https://incubator.apache.org/projects/fluss.html)、
  [公式ブログ発表は2026-08-06](https://fluss.apache.org/blog/apache-fluss-graduates-to-top-level-project/)です。
- Flussはstreaming storageです。Flinkの代替processorやKafkaのdrop-in replacementとして扱いません。
- RisingWaveのPostgreSQL接続互換性は、PostgreSQL全機能の互換性を保証しません。
- Iceberg、Paimon、Trino、Spark、DuckDBを全て同時に採用する必要はありません。
- newerという理由だけで低遅延・低メモリ・低コストと断定しません。workloadと運用条件で検証します。
- Kafka Share GroupsやFlink AI関数は本例に使っていません。配送保証をそれらの機能に依存させていません。

## 実装の参照資料

- [RisingWave standalone](https://docs.risingwave.com/operate/rw-standalone-mode)
- [RisingWave sourceとtable](https://docs.risingwave.com/sql/commands/sql-create-source)
- [Fluss Docker deploy](https://fluss.apache.org/docs/install-deploy/deploying-with-docker/)
- [Materialize new Kafka source syntax](https://materialize.com/docs/sql/create-source/kafka-v2/)
- [Arroyo SQL DDL](https://doc.arroyo.dev/sql/ddl/)
- [Iceberg sink](https://docs.risingwave.com/iceberg/deliver-to-iceberg)
- [PyIceberg API](https://py.iceberg.apache.org/api/)

## 実イメージで確認した差分

- RisingWave v3.0.3の実行オプションは `single_node --store-directory /data` です。
  参照したstandalone guideの `--state-store-directory` はこの実イメージでは受け付けません。
- release notes掲載の `v3.0.3-standalone` は検証時のregistryで取得できず、取得できた `v3.0.3` を使います。
- `config/risingwave.toml` は小規模ラボ用にSSTを32 MiBにします。
  デフォルト256 MiBでは3 GiB制限のstandalone compactorが起動時のメモリ検査で停止するためです。
  `[system]` は初期volume作成時の設定です。本番向けtuning値ではありません。
- FlinkのHTTP `ADD` は明示的にJARを0644にし、実行userが読み取れるようにします。checkpoint用directoryもflink user所有で作成します。

根拠: [RisingWave v3.0.3 single_node実装](https://github.com/risingwavelabs/risingwave/blob/v3.0.3/src/cmd_all/src/single_node.rs)、
[compactorのメモリ検査](https://github.com/risingwavelabs/risingwave/blob/v3.0.3/src/storage/compactor/src/server.rs)。
