# 公開・配布

対象は [hjosugi/kafka-lab](https://github.com/hjosugi/kafka-lab)、既定branchは `main` です。
[GitHub Actions](https://github.com/hjosugi/kafka-lab/actions/workflows/ci.yml)で検証結果、
[Releases](https://github.com/hjosugi/kafka-lab/releases)で配布物を確認できます。

## 配布物を再生成する

tagに含まれる追跡ファイルだけを取り出すため、venv・runtime DB・未追跡ファイルは同梱されません。

```bash
git clone https://github.com/hjosugi/kafka-lab.git
cd kafka-lab
bash scripts/package.sh v1.0.0
cd dist
sha256sum --check SHA256SUMS
```

生成物は `kafka-lab-v1.0.0.zip`、`kafka-lab-v1.0.0.tar.gz`、`SHA256SUMS` です。
GitHubの自動生成Source code archiveとは別に、prefixを揃えた配布物を添付します。
SHA-256はダウンロード整合性の確認用です。発行者の電子署名ではありません。

## 次のリリース

1. 該当例を実行し、[検証記録](validation.md)とCHANGELOG、release notesを更新する。
2. `main` の対象commitでCI成功を確認する。
3. 対象commitに未使用のversion tagを作り、`scripts/package.sh` を実行する。
4. tagを公開し、GitHub Releaseへ配布物とchecksumを添付する。
5. 公開先・tag・commit・配布物checksumが一致することを再確認する。

この文書や既存tagは、agentによる将来のリモート変更への許可ではありません。
変更はその作業の明示的な依頼と適用されるrepository safety ruleに従います。
外部upstreamへのpush、PR作成、fork、CLA同意を公開手順に含めません。
