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
# sitemap.xml からセクション内のサブページを自動発見します
CRAWL_SEEDS = {
    "Databricks Intelligence Platform": [
        "https://docs.databricks.com/aws/en/getting-started/concepts",
        "https://docs.databricks.com/aws/en/introduction",
        "https://docs.databricks.com/aws/en/compute",
        "https://docs.databricks.com/aws/en/compute/sql-warehouse/",
        "https://docs.databricks.com/aws/en/sql/",
        "https://docs.databricks.com/aws/en/repos/",
        "https://docs.databricks.com/aws/en/notebooks/",
        "https://docs.databricks.com/aws/en/dbfs/",
    ],
    "Development & Ingestion": [
        "https://docs.databricks.com/aws/en/delta/",
        "https://docs.databricks.com/aws/en/delta/merge",
        "https://docs.databricks.com/aws/en/delta/history",
        "https://docs.databricks.com/aws/en/ingestion/auto-loader/",
        "https://docs.databricks.com/aws/en/ingestion/copy-into/",
        "https://docs.databricks.com/aws/en/data-engineering/",
        "https://docs.databricks.com/aws/en/connect/",
    ],
    "Data Processing & Transformations": [
        "https://docs.databricks.com/aws/en/spark/",
        "https://docs.databricks.com/aws/en/pyspark/",
        "https://docs.databricks.com/aws/en/sql/language-manual/",
        "https://docs.databricks.com/aws/en/structured-streaming/",
        "https://docs.databricks.com/aws/en/udf/",
        "https://docs.databricks.com/aws/en/optimizations/",
        "https://docs.databricks.com/aws/en/delta/data-skipping",
    ],
    "Productionizing Data Pipelines": [
        "https://docs.databricks.com/aws/en/delta-live-tables/",
        "https://docs.databricks.com/aws/en/workflows/",
        "https://docs.databricks.com/aws/en/jobs/",
    ],
    "Data Governance & Quality": [
        "https://docs.databricks.com/aws/en/data-governance/unity-catalog/",
        "https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/",
        "https://docs.databricks.com/aws/en/tables/constraints",
        "https://docs.databricks.com/aws/en/delta-live-tables/expectations",
        "https://docs.databricks.com/aws/en/data-governance/",
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
import xml.etree.ElementTree as ET
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
SITEMAP_URL = "https://docs.databricks.com/sitemap.xml"


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


def fetch_sitemap_urls() -> list[str]:
    """sitemap.xml から全ドキュメント URL を取得（1 回だけ取得してキャッシュ）"""
    try:
        response = requests.get(SITEMAP_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        # sitemap.xml の namespace
        ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = [loc.text for loc in root.findall(".//s:loc", ns) if loc.text]
        print(f"📡 sitemap.xml から {len(urls)} 件の URL を取得")
        return urls
    except Exception as e:
        print(f"⚠ sitemap.xml の取得に失敗: {e}")
        return []


# sitemap を 1 回だけ取得してキャッシュ
SITEMAP_URLS = fetch_sitemap_urls()


def discover_links_from_sitemap(seed_url: str) -> list[str]:
    """sitemap.xml からシード URL と同一セクションのリンクを発見

    シード URL のパスプレフィックスに一致する URL をフィルタリングする。
    例: seed = .../aws/en/compute → .../aws/en/compute/* を全て返す
    """
    # セクションのパスプレフィックスを決定
    parsed = urlparse(seed_url)
    seed_path = parsed.path.rstrip("/")

    # セクションのベースディレクトリを取得
    # 例: /aws/en/compute/sql-warehouse → /aws/en/compute/sql-warehouse/
    # 例: /aws/en/delta/merge → /aws/en/delta/
    # ページ個別 URL の場合は親ディレクトリをセクションとする
    section_prefix = seed_path
    if not seed_path.endswith("/"):
        # 最後のセグメントがディレクトリかページかを判定
        # sitemap 内に seed_path + "/" で始まる URL があればディレクトリ
        has_children = any(
            urlparse(u).path.startswith(seed_path + "/") for u in SITEMAP_URLS
        )
        if not has_children:
            # ページ個別 URL → 親ディレクトリをセクションとする
            section_prefix = seed_path.rsplit("/", 1)[0]

    section_prefix = section_prefix.rstrip("/") + "/"

    links = []
    seen = set()
    seed_normalized = seed_url.rstrip("/")

    for sitemap_url in SITEMAP_URLS:
        sitemap_path = urlparse(sitemap_url).path
        if not sitemap_path.startswith(section_prefix):
            continue

        url_normalized = sitemap_url.rstrip("/")
        if url_normalized in seen or url_normalized == seed_normalized:
            continue

        seen.add(url_normalized)
        links.append(sitemap_url)

    return links


def discover_links_from_content(soup, seed_url: str) -> list[str]:
    """メインコンテンツ内のリンクも抽出（sitemap の補完用）"""
    section = ""
    path = urlparse(seed_url).path
    # /aws/en/section/... または /en/section/... の両方に対応
    match = re.search(r"/en/([^/]+)", path)
    if match:
        section = match.group(1)

    if not section:
        return []

    links = []
    seen = set()

    # メインコンテンツからリンクを探す
    main = soup.find("main") or soup.find("article") or soup
    for a_tag in main.find_all("a", href=True):
        href = a_tag["href"]
        full_url = urljoin(seed_url, href)
        full_url = full_url.split("#")[0].split("?")[0]

        if not full_url.startswith(DOCS_DOMAIN):
            continue

        full_path = urlparse(full_url).path
        if f"/en/{section}" not in full_path:
            continue

        if full_url in seen or full_url == seed_url:
            continue

        seen.add(full_url)
        links.append(full_url)

    return links


def crawl_seed(seed_url: str, max_pages: int) -> list[tuple[str, str]]:
    """シード URL とそのサブページをクロールし、(url, text) のリストを返す"""
    results = []

    # まずシードページを取得
    raw_soup = fetch_page(seed_url)
    if raw_soup is None:
        return results

    # sitemap.xml からセクション内のリンクを発見
    sitemap_links = discover_links_from_sitemap(seed_url)
    # コンテンツ内リンクで補完
    content_links = discover_links_from_content(raw_soup, seed_url)

    # sitemap 優先、コンテンツで補完（重複排除）
    all_links = list(dict.fromkeys(sitemap_links + content_links))
    print(f"    → sitemap: {len(sitemap_links)} 件, コンテンツ: {len(content_links)} 件, 合計: {len(all_links)} 件")

    # シードページ自体のテキストを取得
    text = extract_text(raw_soup)
    if text and len(text) > 100:
        results.append((seed_url, text))

    # サブページをクロール（上限あり）
    for sub_url in all_links[:max_pages]:
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
