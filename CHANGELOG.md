# Changelog

## v1.0.0 — 2026-09-06

- Kafkaの5用途をRisingWave・PostgreSQL・Pythonで実装。固定fixtureと独立した期待値を添付。
- Transactional outbox、SQLite inbox、entity version、delete tombstone、replayと全行照合を実装。
- Kafka保持期限を越えたoffsetを検出し、新規groupでも欠損を黙って読み飛ばさないよう修正。
- Flink、Fluss、Materialize、Arroyo、Icebergの比較例と自動検証を追加。
- 実イメージの起動引数、compactor memory設定、JAR・checkpoint権限、Fluss batch SQLを修正。
- 日本語解説、構成図、運用・障害復旧、学習ロードマップ、英語面接スクリプトを収録。
- 25 unit tests、実サービスの結合試験、再起動、commit間の失敗・再配送を検証。
- GitHub Actions、配布用ZIP/tar.gz作成スクリプト、SHA-256 checksumを追加。

検証の条件・制約は[validation](docs/validation.md)、配布内容は[release notes](docs/releases/v1.0.0.md)を参照。
