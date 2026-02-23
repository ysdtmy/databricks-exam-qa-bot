# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Vector Search セットアップ
# MAGIC
# MAGIC Delta テーブルに保存したドキュメントチャンクに対して
# MAGIC Databricks Vector Search の Delta Sync Index を作成します。
# MAGIC
# MAGIC **前提条件:** `01_crawl_docs` Notebook を先に実行してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 設定

# COMMAND ----------

# 設定 - 01_crawl_docs.py と同じ値を指定してください
CATALOG_NAME = "exam_qa_bot"
SCHEMA_NAME = "default"
TABLE_NAME = "docs_chunks"

FULL_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

# Vector Search 設定
VS_ENDPOINT_NAME = "exam-bot-vs-endpoint"
VS_INDEX_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.docs_chunks_index"

# エンベディングモデル（Foundation Model API のエンドポイント名）
EMBEDDING_MODEL_ENDPOINT = "databricks-gte-large-en"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vector Search エンドポイントの作成

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# エンドポイントが存在するか確認
existing_endpoints = [ep["name"] for ep in vsc.list_endpoints().get("endpoints", [])]

if VS_ENDPOINT_NAME in existing_endpoints:
    print(f"✅ エンドポイント '{VS_ENDPOINT_NAME}' は既に存在します")
else:
    print(f"🔧 エンドポイント '{VS_ENDPOINT_NAME}' を作成中...")
    vsc.create_endpoint(
        name=VS_ENDPOINT_NAME,
        endpoint_type="STANDARD",
    )
    print(f"✅ エンドポイント '{VS_ENDPOINT_NAME}' を作成しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## エンドポイントの準備待ち

# COMMAND ----------

import time

def wait_for_endpoint_ready(vsc, endpoint_name, timeout=600):
    """エンドポイントが ONLINE になるまで待機"""
    start = time.time()
    while time.time() - start < timeout:
        endpoint = vsc.get_endpoint(endpoint_name)
        status = endpoint.get("endpoint_status", {}).get("state", "UNKNOWN")
        print(f"  ⏳ ステータス: {status}")
        if status == "ONLINE":
            print(f"✅ エンドポイント '{endpoint_name}' が ONLINE になりました")
            return
        time.sleep(30)
    raise TimeoutError(f"エンドポイントが {timeout} 秒以内に ONLINE になりませんでした")

wait_for_endpoint_ready(vsc, VS_ENDPOINT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Sync Index の作成（Databricks 管理エンベディング）

# COMMAND ----------

# インデックスが存在するか確認
try:
    existing_index = vsc.get_index(
        endpoint_name=VS_ENDPOINT_NAME,
        index_name=VS_INDEX_NAME,
    )
    print(f"✅ インデックス '{VS_INDEX_NAME}' は既に存在します")
    print(f"   ステータス: {existing_index.describe()}")
except Exception:
    print(f"🔧 インデックス '{VS_INDEX_NAME}' を作成中...")
    vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT_NAME,
        index_name=VS_INDEX_NAME,
        source_table_name=FULL_TABLE_NAME,
        pipeline_type="TRIGGERED",
        primary_key="chunk_id",
        embedding_source_column="content",
        embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
        columns_to_sync=["chunk_id", "category", "source_url", "content"],
    )
    print(f"✅ インデックス '{VS_INDEX_NAME}' を作成しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## インデックス同期の確認

# COMMAND ----------

def wait_for_index_ready(vsc, endpoint_name, index_name, timeout=1200):
    """インデックスの同期が完了するまで待機"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name)
            status = index.describe().get("status", {})
            detailed_state = status.get("detailed_state", "UNKNOWN")
            message = status.get("message", "")
            print(f"  ⏳ ステータス: {detailed_state} - {message}")

            if detailed_state == "ONLINE_NO_PENDING_UPDATE":
                print(f"✅ インデックス '{index_name}' の同期が完了しました")
                return index
        except Exception as e:
            print(f"  ⏳ 待機中... ({e})")

        time.sleep(30)
    raise TimeoutError(f"インデックスが {timeout} 秒以内に準備完了しませんでした")

index = wait_for_index_ready(vsc, VS_ENDPOINT_NAME, VS_INDEX_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## テスト検索

# COMMAND ----------

# テストクエリ
results = index.similarity_search(
    query_text="Auto Loader でクラウドストレージからファイルを取り込む方法",
    columns=["chunk_id", "category", "content", "source_url"],
    num_results=3,
)

print("🔍 テスト検索結果:")
for row in results.get("result", {}).get("data_array", []):
    print(f"\n--- chunk_id: {row[0]} | category: {row[1]} ---")
    print(f"URL: {row[3]}")
    print(f"内容: {row[2][:200]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ完了
# MAGIC
# MAGIC 以下の情報をアプリの `app.yaml` に設定してください:
# MAGIC - `VS_ENDPOINT_NAME`: エンドポイント名
# MAGIC - `VS_INDEX_NAME`: インデックス名

# COMMAND ----------

print(f"""
========================================
セットアップ完了！
========================================

以下の値を app.yaml に設定してください:

  VS_ENDPOINT_NAME = {VS_ENDPOINT_NAME}
  VS_INDEX_NAME    = {VS_INDEX_NAME}

Databricks Apps にデプロイする際は、
アプリのリソースに以下を追加してください:

  1. Serving Endpoint: {EMBEDDING_MODEL_ENDPOINT} (Can Query)
  2. Serving Endpoint: databricks-meta-llama-3-1-70b-instruct (Can Query)
  3. Vector Search Endpoint: {VS_ENDPOINT_NAME}
""")
