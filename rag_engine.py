"""
RAG Engine for Databricks Exam Bot

Databricks Vector Search と Foundation Model API を使用して、
試験問題を動的に生成するモジュール。
"""

import json
import os
import re
import logging

logger = logging.getLogger(__name__)

# Databricks 上で実行時のみインポート
try:
    from databricks.vector_search.client import VectorSearchClient
    from databricks.sdk import WorkspaceClient

    HAS_DATABRICKS = True
except ImportError:
    HAS_DATABRICKS = False
    logger.warning("Databricks SDK が見つかりません。AI 生成モードは使用できません。")


# 試験カテゴリ一覧
EXAM_CATEGORIES = [
    "Databricks Intelligence Platform",
    "Development & Ingestion",
    "Data Processing & Transformations",
    "Productionizing Data Pipelines",
    "Data Governance & Quality",
]

# カテゴリ別の出題比率（試験勉強モード用）
CATEGORY_WEIGHTS = {
    "Databricks Intelligence Platform": 0.10,
    "Development & Ingestion": 0.30,
    "Data Processing & Transformations": 0.31,
    "Productionizing Data Pipelines": 0.18,
    "Data Governance & Quality": 0.11,
}

# 問題生成プロンプト
QUESTION_GENERATION_PROMPT = """あなたは Databricks 認定資格試験の問題作成者です。

以下のドキュメントコンテキストに基づいて、Databricks Data Engineer Associate 認定試験に出題されそうな問題を1つ作成してください。

## ドキュメントコンテキスト:
{context}

## カテゴリ:
{category}

## 要件:
- 4択の選択問題を作成すること
- 問題は日本語で作成すること
- 実際の試験に近い難易度にすること
- 必ず1つの正解があること
- 詳細な解説を日本語で付けること
- コードスニペットが関係する場合は具体的な構文を含めること

## 出力形式（JSON）:
以下の JSON 形式で出力してください。JSON以外のテキストは含めないでください。
```json
{{
  "question": "問題文",
  "choices": ["A. 選択肢1", "B. 選択肢2", "C. 選択肢3", "D. 選択肢4"],
  "answer": "正解の記号（A/B/C/D）",
  "explanation": "解説文"
}}
```
"""


class RAGEngine:
    """Databricks Vector Search + LLM による問題生成エンジン"""

    def __init__(self):
        self.vs_endpoint_name = os.environ.get("VS_ENDPOINT_NAME", "exam-bot-vs-endpoint")
        self.vs_index_name = os.environ.get("VS_INDEX_NAME", "main.exam_bot.docs_chunks_index")
        self.serving_endpoint = os.environ.get(
            "SERVING_ENDPOINT", "databricks-meta-llama-3-1-70b-instruct"
        )

        self.vsc = None
        self.index = None
        self.workspace_client = None

        if HAS_DATABRICKS:
            try:
                # Databricks Apps: サービスプリンシパル認証
                # 環境変数 DATABRICKS_HOST, DATABRICKS_CLIENT_ID,
                # DATABRICKS_CLIENT_SECRET が自動設定される
                db_host = os.environ.get("DATABRICKS_HOST", "")
                client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
                client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

                if db_host and client_id and client_secret:
                    # OAuth M2M 認証（Databricks Apps 上での実行）
                    workspace_url = db_host if db_host.startswith("https://") else f"https://{db_host}"
                    logger.info(f"OAuth M2M 認証を使用: {workspace_url}")

                    self.workspace_client = WorkspaceClient(
                        host=workspace_url,
                        client_id=client_id,
                        client_secret=client_secret,
                    )
                    self.vsc = VectorSearchClient(
                        workspace_url=workspace_url,
                        service_principal_client_id=client_id,
                        service_principal_client_secret=client_secret,
                    )
                else:
                    # デフォルト認証（ノートブック等）
                    logger.info("デフォルト認証を使用")
                    self.workspace_client = WorkspaceClient()
                    self.vsc = VectorSearchClient()

                self.index = self.vsc.get_index(
                    endpoint_name=self.vs_endpoint_name,
                    index_name=self.vs_index_name,
                )
                logger.info("RAG Engine 初期化完了")
            except Exception as e:
                logger.error(f"RAG Engine 初期化エラー: {e}")
                self.vsc = None
                self.index = None
                self.workspace_client = None

    @property
    def is_available(self) -> bool:
        """AI 生成モードが利用可能かどうか"""
        return self.index is not None and self.workspace_client is not None

    def search_documents(self, query: str, category: str | None = None, num_results: int = 5) -> list[dict]:
        """Vector Search でドキュメントを検索"""
        if not self.is_available:
            return []

        try:
            filters = {}
            if category:
                filters["category"] = category

            results = self.index.similarity_search(
                query_text=query,
                columns=["chunk_id", "category", "content", "source_url"],
                num_results=num_results,
                filters=filters if filters else None,
            )

            docs = []
            for row in results.get("result", {}).get("data_array", []):
                docs.append({
                    "chunk_id": row[0],
                    "category": row[1],
                    "content": row[2],
                    "source_url": row[3],
                })
            return docs
        except Exception as e:
            logger.error(f"検索エラー: {e}")
            return []

    def generate_question(self, category: str | None = None) -> dict | None:
        """RAG で問題を動的に生成"""
        if not self.is_available:
            return None

        try:
            # カテゴリに基づいた検索クエリ
            search_queries = {
                "Databricks Intelligence Platform": "Databricks workspace cluster compute SQL notebook",
                "Development & Ingestion": "Delta Lake Auto Loader COPY INTO merge upsert medallion",
                "Data Processing & Transformations": "Spark DataFrame SQL transformation streaming window",
                "Productionizing Data Pipelines": "Delta Live Tables DLT workflow job schedule pipeline",
                "Data Governance & Quality": "Unity Catalog governance access control data quality",
            }

            if category and category in search_queries:
                query = search_queries[category]
            else:
                # ランダムクエリ
                import random
                query = random.choice(list(search_queries.values()))
                if not category:
                    category = [k for k, v in search_queries.items() if v == query][0]

            # ドキュメント検索
            docs = self.search_documents(query, category=category, num_results=3)
            if not docs:
                logger.warning("検索結果が0件。静的問題にフォールバックします。")
                return None

            # コンテキスト作成
            context = "\n\n---\n\n".join([doc["content"] for doc in docs])

            # LLM で問題生成
            prompt = QUESTION_GENERATION_PROMPT.format(
                context=context,
                category=category or "全般",
            )

            response = self.workspace_client.serving_endpoints.query(
                name=self.serving_endpoint,
                messages=[
                    {"role": "system", "content": "あなたは Databricks 認定資格試験の問題作成エキスパートです。指定された JSON 形式で正確に出力してください。"},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=1024,
                temperature=0.7,
            )

            # レスポンスのパース
            response_text = response.choices[0].message.content.strip()
            question_data = self._parse_question_response(response_text)

            if question_data:
                question_data["category"] = category
                question_data["source"] = "ai_generated"
                return question_data

        except Exception as e:
            logger.error(f"問題生成エラー: {e}")

        return None

    def _parse_question_response(self, response_text: str) -> dict | None:
        """LLM レスポンスから問題 JSON をパース"""
        try:
            # JSON ブロックを抽出
            json_match = re.search(r"```json\s*(.*?)\s*```", response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # JSON ブロックがない場合、テキスト全体を試行
                json_str = response_text

            data = json.loads(json_str)

            # 必須フィールドの検証
            required_fields = ["question", "choices", "answer", "explanation"]
            if all(field in data for field in required_fields):
                if len(data["choices"]) == 4 and data["answer"] in ["A", "B", "C", "D"]:
                    return data

            logger.warning(f"問題データの検証に失敗: {data}")
            return None

        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.error(f"JSON パースエラー: {e}")
            logger.error(f"レスポンス: {response_text[:500]}")
            return None
