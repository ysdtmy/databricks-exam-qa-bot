# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Databricks 公式ドキュメント クロール
# MAGIC
# MAGIC Databricks 公式ドキュメントの試験範囲ページをクロールし、
# MAGIC テキストをチャンク分割して Delta テーブルに保存します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 設定

# COMMAND ----------

# 設定 - ご自身の環境に合わせて変更してください
CATALOG_NAME = "main"
SCHEMA_NAME = "exam_bot"
TABLE_NAME = "docs_chunks"

FULL_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

# チャンク設定
CHUNK_SIZE = 500  # 文字数
CHUNK_OVERLAP = 100  # オーバーラップ文字数

# COMMAND ----------

# MAGIC %pip install langchain-text-splitters beautifulsoup4 requests
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## カタログ・スキーマの作成

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## クロール対象 URL の定義

# COMMAND ----------

# Data Engineer Associate 試験範囲に沿ったドキュメント URL
CRAWL_URLS = {
    "Databricks Intelligence Platform": [
        "https://docs.databricks.com/en/getting-started/concepts.html",
        "https://docs.databricks.com/en/introduction/index.html",
        "https://docs.databricks.com/en/compute/index.html",
        "https://docs.databricks.com/en/sql/index.html",
        "https://docs.databricks.com/en/repos/index.html",
    ],
    "Development & Ingestion": [
        "https://docs.databricks.com/en/delta/index.html",
        "https://docs.databricks.com/en/ingestion/auto-loader/index.html",
        "https://docs.databricks.com/en/ingestion/copy-into/index.html",
        "https://docs.databricks.com/en/sql/language-manual/delta-create-table.html",
        "https://docs.databricks.com/en/delta/merge.html",
        "https://docs.databricks.com/en/tables/multi-hop.html",
    ],
    "Data Processing & Transformations": [
        "https://docs.databricks.com/en/spark/index.html",
        "https://docs.databricks.com/en/pyspark/index.html",
        "https://docs.databricks.com/en/sql/language-manual/index.html",
        "https://docs.databricks.com/en/structured-streaming/index.html",
        "https://docs.databricks.com/en/udf/index.html",
        "https://docs.databricks.com/en/spark/caching.html",
    ],
    "Productionizing Data Pipelines": [
        "https://docs.databricks.com/en/delta-live-tables/index.html",
        "https://docs.databricks.com/en/workflows/index.html",
        "https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html",
        "https://docs.databricks.com/en/jobs/schedule.html",
    ],
    "Data Governance & Quality": [
        "https://docs.databricks.com/en/data-governance/unity-catalog/index.html",
        "https://docs.databricks.com/en/tables/constraints.html",
        "https://docs.databricks.com/en/delta-live-tables/expectations.html",
        "https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html",
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## クロール & チャンク分割
# MAGIC
# MAGIC LangChain の `RecursiveCharacterTextSplitter` を使用して、
# MAGIC 意味のある境界（段落・文）でテキストを分割します。

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re
import time
from langchain_text_splitters import RecursiveCharacterTextSplitter

# テキストスプリッターの初期化
# セパレータの優先順位: 段落 → 改行 → 句点 → ピリオド → スペース → 文字
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    length_function=len,
    separators=["\n\n", "\n", "。", ".", " ", ""],
    is_separator_regex=False,
)


def fetch_page_text(url: str) -> str:
    """URL からページのメインテキストを取得"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; DatabricksExamBot/1.0)"
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # ナビゲーション、ヘッダー、フッターを除去
        for tag in soup.find_all(["nav", "header", "footer", "script", "style", "aside"]):
            tag.decompose()

        # メインコンテンツ領域を取得
        main = soup.find("main") or soup.find("article") or soup.find("div", {"role": "main"})
        if main:
            text = main.get_text(separator="\n", strip=True)
        else:
            text = soup.get_text(separator="\n", strip=True)

        # 連続改行・空白を整理
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text.strip()
    except Exception as e:
        print(f"  ⚠ クロール失敗: {url} - {e}")
        return ""

# COMMAND ----------

# MAGIC %md
# MAGIC ## クロール実行

# COMMAND ----------

all_chunks = []
chunk_id = 0

for category, urls in CRAWL_URLS.items():
    print(f"\n📂 カテゴリ: {category}")
    for url in urls:
        print(f"  🔗 クロール中: {url}")
        text = fetch_page_text(url)
        if not text:
            continue

        chunks = text_splitter.split_text(text)
        print(f"  ✅ {len(chunks)} チャンク取得")

        for chunk in chunks:
            all_chunks.append({
                "chunk_id": chunk_id,
                "category": category,
                "source_url": url,
                "content": chunk,
            })
            chunk_id += 1

        # レート制限対策
        time.sleep(1)

print(f"\n📊 合計: {len(all_chunks)} チャンク")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta テーブルに保存

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("chunk_id", IntegerType(), False),
    StructField("category", StringType(), False),
    StructField("source_url", StringType(), False),
    StructField("content", StringType(), False),
])

df = spark.createDataFrame(all_chunks, schema=schema)

# Change Data Feed を有効にして保存（Vector Search の Delta Sync に必要）
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable(FULL_TABLE_NAME)

print(f"✅ テーブル {FULL_TABLE_NAME} に {df.count()} 行を保存しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 確認

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10"))

# COMMAND ----------

# カテゴリ別のチャンク数を確認
display(spark.sql(f"""
    SELECT category, COUNT(*) as chunk_count
    FROM {FULL_TABLE_NAME}
    GROUP BY category
    ORDER BY category
"""))
