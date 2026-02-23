# 🎓 Databricks 資格試験 練習ボット

Databricks Data Engineer Associate 認定試験の練習問題を出題するチャットボットアプリです。  
Databricks 公式ドキュメントを RAG のソースとして活用し、AI で動的に問題を生成します。

## 機能

| モード | 説明 |
|---|---|
| 📝 **試験勉強モード** | 全5分野からランダムに20問出題（本番試験に近いシミュレーション） |
| 📂 **トピック別モード** | 分野を選択して20問集中出題 |

- ✅ 正誤判定 + 詳細な解説表示
- 📊 分野別スコアトラッキング
- 🤖 RAG + LLM による AI 問題生成（静的問題のフォールバック付き）

## プロジェクト構成

```
databricks-sample-qa-bot/
├── app.py                  # Gradio メインアプリ
├── app.yaml                # Databricks Apps 設定
├── rag_engine.py           # RAG エンジン（Vector Search + LLM）
├── questions.json          # 静的問題データ（30問）
├── requirements.txt        # Python 依存パッケージ
├── README.md
└── notebooks/
    ├── 01_crawl_docs.py          # ドキュメントクロール Notebook
    └── 02_setup_vectorsearch.py  # Vector Search セットアップ Notebook
```

## セットアップ手順

### 1. 前提条件

- Databricks ワークスペース（Unity Catalog 有効）
- Serverless コンピュートが利用可能

### 2. Vector Search のセットアップ（Databricks 上で実行）

1. `notebooks/01_crawl_docs.py` を Databricks ワークスペースにインポート
2. Notebook 冒頭の設定セルでカタログ名・スキーマ名を変更
3. Notebook を実行 → Delta テーブルにドキュメントが保存されます

4. `notebooks/02_setup_vectorsearch.py` をインポート・実行
5. Vector Search エンドポイントとインデックスが作成されます

### 3. Databricks Apps へのデプロイ

1. `app.yaml` の環境変数を確認・修正:
   - `VS_ENDPOINT_NAME`: Vector Search エンドポイント名
   - `VS_INDEX_NAME`: Vector Search インデックス名
   - `SERVING_ENDPOINT`: LLM サービングエンドポイント名

2. Databricks CLI でデプロイ:
```bash
databricks apps create exam-bot -p dbw-aws
databricks apps deploy exam-bot --source-code-path . -p dbw-aws
```

3. アプリのリソース設定で以下を追加:
   - Serving Endpoint: `databricks-meta-llama-3-1-70b-instruct` (Can Query)
   - Serving Endpoint: `databricks-gte-large-en` (Can Query)
   - Vector Search Endpoint: `exam-bot-vs-endpoint`

## ローカル開発

```bash
pip install -r requirements.txt
python app.py
```

ブラウザで `http://localhost:8000` にアクセスしてください。  
（ローカルでは AI 生成問題は利用できません。静的問題のみ動作します。）

## 試験対象分野

| 分野 | 試験比率 |
|---|---|
| Databricks Intelligence Platform | 10% |
| Development & Ingestion | 30% |
| Data Processing & Transformations | 31% |
| Productionizing Data Pipelines | 18% |
| Data Governance & Quality | 11% |
