# Fluss: storageとcomputeを分ける

Flussのorders表をFlinkが読み、customer別合計をFlussの主キー付き表へ書き戻します。
同じorderを更新すると古い値を差し引き、新しい値で合計が更新される流れを学びます。

公式quickstartの配布組み合わせであるFluss 0.9.1-incubating / Flink 1.20を使用します。
`incubating` はこのリリースの名前です。プロジェクトの現在のApache上の区分とは別です。
[公式quickstart](https://fluss.apache.org/docs/quickstart/flink/)

主要デモと独立したCompose projectです。新規環境で `python3 scripts/compare.py fluss` を実行すると、
初期集計・更新・削除・期待値照合まで自動化できます。手動の場合はルートから:

```bash
docker compose -f examples/fluss/compose.yaml up -d
```

[Flink UI](http://localhost:8083)でTaskManagerの登録、Flussのログで起動を確認後:

```bash
docker compose -f examples/fluss/compose.yaml exec -T jobmanager /opt/flink/bin/sql-client.sh -f /sql/01-pipeline.sql
docker compose -f examples/fluss/compose.yaml exec -T jobmanager /opt/flink/bin/sql-client.sh -f /sql/02-query.sql
```

初期期待値: u1=1700、u2=700。集計は非同期なので直後は未反映のことがあります。

```bash
docker compose -f examples/fluss/compose.yaml exec -T jobmanager /opt/flink/bin/sql-client.sh -f /sql/03-mutate.sql
docker compose -f examples/fluss/compose.yaml exec -T jobmanager /opt/flink/bin/sql-client.sh -f /sql/02-query.sql
```

変更後期待値: u1=1800、u2=700。u1の1001を1800へ変更し、1002を削除しています。
`01-pipeline.sql` は新規環境専用です。二度実行すると既存tableで失敗します。

この例のremote data directoryは共有ローカルvolumeです。S3やIceberg/Paimonへのtieringは構成していません。
全サービスから同じパスを見せる簡易環境で、本番の分散storage耐障害性を再現しません。
Flink jobは再起動時に自動で再投入されません。データを保持したままのrestart運用例ではありません。

```bash
docker compose -f examples/fluss/compose.yaml down
```

volumeも削除する場合だけ `down -v` を使います。実行結果は[検証記録](../../docs/validation.md)を参照してください。

Lake tieringを使わないこの例では、batch読み取りを主キーのpoint queryに限定します。
UPDATE/DELETEもbatch sessionで実行します。集計ジョブ自体はstreamingで継続実行します。
