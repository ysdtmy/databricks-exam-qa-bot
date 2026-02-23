# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Databricks 公式ドキュメント クロール
# MAGIC
# MAGIC Databricks 公式ドキュメントの試験範囲ページをクロールし、
# MAGIC テキストをチャンク分割して Delta テーブルに保存します。

# COMMAND ----------

# MAGIC %pip install langchain-text-splitters beautifulsoup4 requests
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 設定

# COMMAND ----------

# 設定 - ご自身の環境に合わせて変更してください
CATALOG_NAME = "exam_qa_bot"
SCHEMA_NAME = "default"
TABLE_NAME = "docs_chunks"

FULL_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

# チャンク設定
CHUNK_SIZE = 500  # 文字数
CHUNK_OVERLAP = 100  # オーバーラップ文字数

# クロール設定
MAX_PAGES_PER_SEED = 20  # 各シード URL から辿るサブページの最大数
CRAWL_DELAY = 0.5  # リクエスト間隔（秒）

# COMMAND ----------

# MAGIC %md
# MAGIC ## カタログ・スキーマの作成

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## クロール対象 URL の定義
# MAGIC
# MAGIC 各カテゴリのシード URL を起点に、同一セクション内のサブページも
# MAGIC 自動的にクロールします（各シードから最大 `MAX_PAGES_PER_SEED` ページ）。

# COMMAND ----------

# Data Engineer Associate 試験範囲に沿ったシード URL
# index ページからリンクされている詳細ページも自動クロールされます
CRAWL_SEEDS = {
    "Databricks Intelligence Platform": [
        "https://docs.databricks.com/en/getting-started/concepts.html",
        "https://docs.databricks.com/en/introduction/index.html",
        "https://docs.databricks.com/en/compute/index.html",
        "https://docs.databricks.com/en/compute/sql-warehouse/index.html",
        "https://docs.databricks.com/en/sql/index.html",
        "https://docs.databricks.com/en/repos/index.html",
        "https://docs.databricks.com/en/notebooks/index.html",
        "https://docs.databricks.com/en/dbfs/index.html",
    ],
    "Development & Ingestion": [
        "https://docs.databricks.com/en/delta/index.html",
        "https://docs.databricks.com/en/delta/create-tables.html",
        "https://docs.databricks.com/en/delta/merge.html",
        "https://docs.databricks.com/en/delta/update.html",
        "https://docs.databricks.com/en/delta/delete-on.html",
        "https://docs.databricks.com/en/delta/history.html",
        "https://docs.databricks.com/en/delta/time-travel.html",
        "https://docs.databricks.com/en/ingestion/auto-loader/index.html",
        "https://docs.databricks.com/en/ingestion/copy-into/index.html",
        "https://docs.databricks.com/en/tables/multi-hop.html",
        "https://docs.databricks.com/en/connect/external-systems/index.html",
    ],
    "Data Processing & Transformations": [
        "https://docs.databricks.com/en/spark/index.html",
        "https://docs.databricks.com/en/pyspark/index.html",
        "https://docs.databricks.com/en/pyspark/basics.html",
        "https://docs.databricks.com/en/sql/language-manual/index.html",
        "https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select.html",
        "https://docs.databricks.com/en/structured-streaming/index.html",
        "https://docs.databricks.com/en/structured-streaming/triggers.html",
        "https://docs.databricks.com/en/structured-streaming/watermarks.html",
        "https://docs.databricks.com/en/udf/index.html",
        "https://docs.databricks.com/en/spark/caching.html",
        "https://docs.databricks.com/en/optimizations/index.html",
        "https://docs.databricks.com/en/delta/data-skipping.html",
    ],
    "Productionizing Data Pipelines": [
        "https://docs.databricks.com/en/delta-live-tables/index.html",
        "https://docs.databricks.com/en/delta-live-tables/tutorial.html",
        "https://docs.databricks.com/en/delta-live-tables/updates.html",
        "https://docs.databricks.com/en/delta-live-tables/observability.html",
        "https://docs.databricks.com/en/workflows/index.html",
        "https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html",
        "https://docs.databricks.com/en/workflows/jobs/schedule-jobs.html",
        "https://docs.databricks.com/en/workflows/jobs/monitor-jobs.html",
        "https://docs.databricks.com/en/jobs/index.html",
        "https://docs.databricks.com/en/jobs/schedule.html",
    ],
    "Data Governance & Quality": [
        "https://docs.databricks.com/en/data-governance/unity-catalog/index.html",
        "https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html",
        "https://docs.databricks.com/en/data-governance/unity-catalog/create-catalogs.html",
        "https://docs.databricks.com/en/data-governance/unity-catalog/create-schemas.html",
        "https://docs.databricks.com/en/data-governance/unity-catalog/create-tables.html",
        "https://docs.databricks.com/en/tables/constraints.html",
        "https://docs.databricks.com/en/delta-live-tables/expectations.html",
        "https://docs.databricks.com/en/data-governance/index.html",
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## クロール & チャンク分割
# MAGIC
# MAGIC LangChain の `RecursiveCharacterTextSplitter` を使用して、
# MAGIC 意味のある境界（段落・文）でテキストを分割します。
# MAGIC シード URL からリンク先のサブページも自動発見してクロールします。

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import time
from langchain_text_splitters import RecursiveCharacterTextSplitter

# テキストスプリッターの初期化
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    length_function=len,
    separators=["\n\n", "\n", "。", ".", " ", ""],
    is_separator_regex=False,
)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DatabricksExamBot/1.0)"}
DOCS_DOMAIN = "https://docs.databricks.com"


def fetch_page(url: str):
    """URL からページの BeautifulSoup オブジェクトを取得"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"    ⚠ 取得失敗: {url} - {e}")
        return None


def extract_text(soup) -> str:
    """BeautifulSoup からメインテキストを抽出"""
    # 不要要素を除去
    for tag in soup.find_all(["nav", "header", "footer", "script", "style", "aside"]):
        tag.decompose()

    main = soup.find("main") or soup.find("article") or soup.find("div", {"role": "main"})
    text = (main or soup).get_text(separator="\n", strip=True)

    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def _extract_section(url: str) -> str:
    """URL からドキュメントのセクション名を抽出
    例: /en/compute/index.html → 'compute'
        /aws/en/delta/merge.html → 'delta'
    """
    path = urlparse(url).path
    # /en/ または /<cloud>/en/ の後のセグメントを取得
    match = re.search(r"/en/([^/]+)", path)
    return match.group(1) if match else ""


def discover_links(soup, seed_url: str) -> list[str]:
    """ページ内から Databricks ドキュメントのサブページリンクを発見

    Databricks ドキュメントはリンクに /aws/en/, /gcp/en/, /azure/en/
    などのクラウドプレフィックスを使用するため、セクション名で照合する。
    """
    section = _extract_section(seed_url)
    if not section:
        return []

    links = []
    seen = set()

    # ページ全体からリンクを探す（<main> 内だけでなく）
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        full_url = urljoin(seed_url, href)

        # フラグメントとクエリを除去
        full_url = full_url.split("#")[0].split("?")[0]

        # Databricks ドメイン内に限定
        if not full_url.startswith(DOCS_DOMAIN):
            continue

        # 同じセクションに属するかチェック（/en/<section>/ のパターン）
        full_path = urlparse(full_url).path
        if f"/en/{section}" not in full_path:
            continue

        # HTML ページのみ
        if not (full_path.endswith(".html") or full_path.endswith("/")):
            continue

        # 重複排除・自分自身排除
        if full_url in seen or full_url == seed_url:
            continue

        seen.add(full_url)
        links.append(full_url)

    return links


def crawl_seed(seed_url: str, max_pages: int) -> list[tuple[str, str]]:
    """シード URL とそのサブページをクロールし、(url, text) のリストを返す"""
    results = []

    # まずシードページを取得
    soup = fetch_page(seed_url)
    if soup is None:
        return results

    text = extract_text(soup)
    if text and len(text) > 100:
        results.append((seed_url, text))

    # サブページリンクを発見
    sub_links = discover_links(soup, seed_url)
    print(f"    → {len(sub_links)} 件のサブページを発見")

    # サブページをクロール（上限あり）
    for sub_url in sub_links[:max_pages]:
        time.sleep(CRAWL_DELAY)
        sub_soup = fetch_page(sub_url)
        if sub_soup is None:
            continue
        sub_text = extract_text(sub_soup)
        if sub_text and len(sub_text) > 100:
            results.append((sub_url, sub_text))
            print(f"    📄 {sub_url} ({len(sub_text)} 文字)")

    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## クロール実行

# COMMAND ----------

all_chunks = []
crawled_urls = set()  # グローバル重複排除

import hashlib
from datetime import datetime

crawled_at = datetime.utcnow().isoformat()


def make_chunk_id(source_url: str, chunk_index: int) -> str:
    """URL とチャンク番号から決定的な chunk_id を生成（冪等性の担保）"""
    raw = f"{source_url}::{chunk_index}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


for category, seed_urls in CRAWL_SEEDS.items():
    print(f"\n📂 カテゴリ: {category}")
    category_page_count = 0
    category_chunk_count = 0

    for seed_url in seed_urls:
        print(f"\n  🔗 シード: {seed_url}")
        pages = crawl_seed(seed_url, MAX_PAGES_PER_SEED)

        for url, text in pages:
            if url in crawled_urls:
                continue
            crawled_urls.add(url)

            chunks = text_splitter.split_text(text)
            for i, chunk in enumerate(chunks):
                all_chunks.append({
                    "chunk_id": make_chunk_id(url, i),
                    "category": category,
                    "source_url": url,
                    "content": chunk,
                    "crawled_at": crawled_at,
                })

            category_page_count += 1
            category_chunk_count += len(chunks)

        time.sleep(CRAWL_DELAY)

    print(f"\n  📊 {category}: {category_page_count} ページ, {category_chunk_count} チャンク")

print(f"\n{'='*50}")
print(f"📊 合計: {len(crawled_urls)} ページ, {len(all_chunks)} チャンク")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta テーブルに保存（冪等・定期実行対応）
# MAGIC
# MAGIC - `chunk_id` は URL + チャンク番号のハッシュで決定的に生成
# MAGIC - `MERGE INTO` で既存データを更新（upsert）
# MAGIC - 何度実行しても同じ結果になります（冪等性）
# MAGIC - `crawled_at` で最終クロール日時を追跡

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("source_url", StringType(), False),
    StructField("content", StringType(), False),
    StructField("crawled_at", StringType(), False),
])

# テーブルが存在しない場合のみ作成（CDF 有効）
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
        chunk_id STRING NOT NULL,
        category STRING NOT NULL,
        source_url STRING NOT NULL,
        content STRING NOT NULL,
        crawled_at STRING NOT NULL
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print(f"✅ テーブル {FULL_TABLE_NAME} を確認/作成しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE INTO（Upsert）

# COMMAND ----------

df_new = spark.createDataFrame(all_chunks, schema=schema)
df_new.createOrReplaceTempView("new_chunks")

# MERGE: chunk_id が一致したら更新、なければ挿入
merge_result = spark.sql(f"""
    MERGE INTO {FULL_TABLE_NAME} AS target
    USING new_chunks AS source
    ON target.chunk_id = source.chunk_id
    WHEN MATCHED THEN UPDATE SET
        target.category = source.category,
        target.source_url = source.source_url,
        target.content = source.content,
        target.crawled_at = source.crawled_at
    WHEN NOT MATCHED THEN INSERT *
""")

# 古いチャンク（今回のクロールに含まれないもの）を削除
spark.sql(f"""
    DELETE FROM {FULL_TABLE_NAME}
    WHERE crawled_at < '{crawled_at}'
""")

row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {FULL_TABLE_NAME}").first()["cnt"]
print(f"✅ MERGE 完了: {FULL_TABLE_NAME} に {row_count} 行")
print(f"🕐 クロール日時: {crawled_at}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 確認

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {FULL_TABLE_NAME} LIMIT 10"))

# COMMAND ----------

# カテゴリ別のチャンク数を確認
display(spark.sql(f"""
    SELECT category, COUNT(*) as chunk_count, MIN(crawled_at) as oldest, MAX(crawled_at) as latest
    FROM {FULL_TABLE_NAME}
    GROUP BY category
    ORDER BY category
"""))
