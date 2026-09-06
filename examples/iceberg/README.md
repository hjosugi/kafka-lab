# Iceberg: table metadataとsnapshotを確認する

この例は同じイベントfixtureを実際のIceberg tableへ保存し、PyIcebergでscanしたArrowデータをDuckDBで集計します。
通常のJSON/CSV保存との違いを確認する有限データの例です。常駐stream processorではありません。

Python 3.12環境でルートから:

```bash
python3 -m venv .venv-iceberg
.venv-iceberg/bin/python -m pip install -r examples/iceberg/requirements.txt
.venv-iceberg/bin/python examples/iceberg/landing.py
```

期待値はcheckout ERROR=3、INFO=1、catalog INFO=1です。
`runtime/iceberg` にSQLite catalog、Iceberg metadata、Parquet dataが生成されます。
再実行はfixture全体を `overwrite()` し、新しいsnapshotを作ります。
appendを繰り返して同一イベントを二重計上する方式ではありません。

DuckDBはこの例ではIceberg extensionを直接使わず、PyIcebergがcurrent snapshotから読んだArrowを照会します。
PyIcebergのscanはIceberg metadataを解釈します。単なるParquet directory scanではありません。
[PyIceberg API](https://py.iceberg.apache.org/api/)

## 常時ストリーミングへ進める場合

RisingWaveのCDC table → Iceberg sinkを構成し、次を用意します。

1. Iceberg REST/JDBC等のcatalogとobject storage。
2. 主キーを使うupsert sinkと、update/deleteに対応する読み手。
3. sink commit間隔、small-file対策、compaction、snapshot expiration。

SQLオプションを自己流で推測せず、[公式sink設定](https://docs.risingwave.com/iceberg/deliver-to-iceberg)と
[REST catalog設定](https://docs.risingwave.com/iceberg/catalogs/rest)を使用してください。
この追加インフラとRisingWave sinkは本リポジトリの起動対象に含まれません。
実テーブルの作成・再実行と期待値一致を検証しています。[検証記録](../../docs/validation.md)
