# 検証記録

確認日: 2026-09-06。手元の実行とGitHub Actionsを分けて記録します。
CIの実行commitと結果は[validate workflow](https://github.com/hjosugi/kafka-lab/actions/workflows/ci.yml)から参照できます。

| 項目 | ローカル結果 | 確認した内容 |
|---|---|---|
| Unit tests | 25件成功 | 固定期待値、重複、順序逆転、delete、rollback、入力契約、offset保持期限 |
| オフライン集計 | 成功 | logs、recommendations、alertsの期待値一致 |
| 構文・fixture・文書リンク・SVG | 成功 | `python3 scripts/check.py` |
| Compose schema | 5構成成功 | Docker Compose v5.5.1の`config --quiet` |
| 主要5用途 | 成功 | 実Kafka/RisingWave/PostgreSQL、UPDATE/DELETE、SQLite全行照合 |
| 再起動とreplay | 成功 | Kafka・PostgreSQL・RisingWave再起動後の照合、注文5レコード再読 |
| commit間の失敗・再配送 | 成功 | `scripts/recovery.py`、inbox件数と集計の不変性 |
| Flink | 成功 | 入力offsetのcheckpoint、watermark、Kafka出力がcheckout=5/4・catalog=1/0 |
| Fluss | 成功 | u1=1700→1800、u2=700。主キーpoint query、batch UPDATE/DELETE |
| Materialize | 成功 | 実Kafka sourceからERROR=3・INFO=1/1へ継続集計 |
| Arroyo | 成功 | APIでpipeline作成・Running確認、Kafka sinkのERRORイベント照合 |
| Iceberg | 成功 | 実metadata・Parquet・snapshot、overwrite再実行、DuckDB集計 |
| 負荷・HA・ホスト喪失 | 未実施 | 本番throughput、P99、ホスト障害への保証はしない |

## ローカル環境の制約

Linux x86_64、Podman 6.1.1、コンテナ内Python 3.12、IcebergはホストPython 3.13で検証しました。
実行中カーネルのbridge/overlayモジュールがなく、標準Compose networkは作成できませんでした。
作業専用fuse-overlayfs storageとpastaのpod networkで、同じimage・SQL・source・volume構成を実行しました。
ローカル実行は標準Docker bridge経路の成功を意味しません。その経路はCIが確認します。
ホスト全体のPodman設定や他プロジェクトの設定は変更していません。

## GitHub Actionsの検証対象

| job | 内容 |
|---|---|
| unit | 25テスト、構文・相対リンク、5つのCompose schema |
| integration (main) | 空volumeでdemo、3サービス再起動、replay、commit間の障害注入 |
| integration (flink) | 独立したfixtureからcheckpointとevent-time窓の結果 |
| integration (materialize) | 実KafkaとSQL集計 |
| integration (arroyo) | API経由のpipelineと実Kafka出力 |
| fluss | 新規クラスタ、初期集計、UPDATE/DELETE、point query |
| iceberg | Python 3.12でテーブル作成とoverwriteの2回実行 |

各jobは独立したrunnerを使い、比較例の再送やmarkerが他の例の期待値を変えないようにします。
imageはversion tag、Pythonは直接依存のversionを固定しています。完全な推移依存lockや全platformの検証は行っていません。

## 再現コマンド

```bash
bash scripts/lab.sh test
python3 scripts/check.py
docker compose config --quiet
bash scripts/lab.sh demo
docker compose restart kafka postgres risingwave
bash scripts/lab.sh verify
bash scripts/lab.sh replay
bash scripts/lab.sh verify
```

比較例は初期fixtureから、該当するものを別環境で実行します。

```bash
python3 scripts/compare.py flink
python3 scripts/compare.py materialize
python3 scripts/compare.py arroyo
python3 scripts/compare.py fluss
```

`bash scripts/lab.sh recovery`は主要デモの後に実行します。
Flinkの物理レコード数が変わるためFlink検証とは分けます。

RisingWaveの3 GiB設定・起動引数、Flink JAR・checkpointの権限、Fluss SQLの実行モードは修正済みです。
[バージョン補足](versions.md)と[復旧ガイド](operations.md)も参照してください。
