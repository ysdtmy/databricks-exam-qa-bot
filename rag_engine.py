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
    from databricks.sdk.service.serving import ChatMessage
    from databricks.sdk.service.serving import ChatMessageRole

    HAS_DATABRICKS = True
except ImportError:
    HAS_DATABRICKS = False
    logger.warning("Databricks SDK が見つかりません。AI 生成モードは使用できません。")


# シラバスの読み込み
SYLLABUSES_PATH = os.path.join(os.path.dirname(__file__), "syllabuses.json")
try:
    with open(SYLLABUSES_PATH, "r", encoding="utf-8") as f:
        SYLLABUSES = json.load(f)
except Exception as e:
    logger.error(f"シラバスの読み込みエラー: {e}")
    SYLLABUSES = {"Data Engineer Associate": {"categories": []}}

# デフォルトの対象試験
TARGET_EXAM = "Data Engineer Associate"

# 現在の試験のカテゴリと比率を取得
EXAM_CATEGORIES = []
CATEGORY_WEIGHTS = {}
if TARGET_EXAM in SYLLABUSES:
    for cat in SYLLABUSES[TARGET_EXAM].get("categories", []):
        EXAM_CATEGORIES.append(cat["name"])
        CATEGORY_WEIGHTS[cat["name"]] = cat["weight"]

# 問題生成プロンプトの共通部分
QUESTION_GENERATION_PROMPT = """あなたは Databricks 認定資格試験の問題作成者です。

以下のドキュメントコンテキストに基づいて、Databricks {exam} 認定試験に出題されそうな問題を1つ作成してください。

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
- 解説には必ず、参考にしたドキュメントのURL（[Source URL: ...] のもの）と、そのドキュメントからの引用文を記載すること。
- （Professional試験の場合）可能な限り「あるデータエンジニアが〜という要件を満たすシステムを構築しています」のような、具体的な業務シナリオ（状況設定）をベースとした長文の応用問題を作成すること。単なる用語定義を問う問題は避けること。

## 出力例（Few-Shot Example）:
{few_shot_example}

## 出力形式（JSON）:
上記の出力例と同じ構造の JSON 形式で出力してください。JSON以外のテキストは絶対に含めないでください。
"""

# 試験別の Few-Shot サンプル
FEW_SHOT_EXAMPLES = {
    "Data Engineer Associate": """```json
{
  "question": "DataFrame の書き込み操作中に Delta Lake でスキーマ展開（Schema Evolution）を有効にするには、どのオプションを使用する必要がありますか？",
  "choices": [
    "A. スキーマ展開はデフォルトで有効になっており、新しい列が自動的に追加される。",
    "B. .option(\\"mergeSchema\\", \\"true\\") を使用して明示的に有効にする必要がある。",
    "C. Delta Lake はスキーマ展開をサポートしておらず、テーブルを再作成する必要がある。",
    "D. ALTER TABLE SQL コマンドを使用した場合のみスキーマ展開が可能である。"
  ],
  "answer": "B",
  "explanation": "Delta Lake では、書き込み操作による誤ったスキーマ変更を防ぐため、スキーマ展開はデフォルトで無効になっています。DataFrame API を使用して新しい列を追加し、ターゲットテーブルのスキーマを展開する場合は、書き込みオプションとして `.option(\\"mergeSchema\\", \\"true\\")` を指定して明示的にスキーマの変更を許可する必要があります。\\n\\n**参考ドキュメント:** https://docs.databricks.com/ja/delta/update-schema.html\\n**引用:** > You can explicitly allow schema evolution by specifying the option `mergeSchema` to `true`."
}
```""",
    "Data Engineer Professional": """```json
{
  "question": "あるデータエンジニアが、Databricks Auto Loader を使用してオンプレミスのシステムからクラウドストレージに継続的に到着する JSON ファイルを取り込み、ブロンズテーブルを構築するデータパイプラインを設計しています。ソースシステムは仕様が頻繁に変更されるため、JSON データには予期しない新しい列や、既存のスキーマとデータ型が異なる競合データが混入する可能性があります。\\nターゲットテーブルへの取り込みにおいて、パイプラインを停止させることなく、これらの未知のデータや型崩れしたデータを確実に保持し、データエンジニアが後から確認・回復できるようにするための最も推奨されるアプローチは何ですか？",
  "choices": [
    "A. cloudFiles.inferColumnTypes を true に設定し、cloudFiles.schemaEvolutionMode を addNewColumns にして、すべての新しい列を自動的にブロンズテーブルのスキーマに追加する。",
    "B. cloudFiles.schemaEvolutionMode を rescue に設定し、予期しないデータやスキーマに一致しないデータを _rescued_data という単一の列にキャプチャするよう構成する。",
    "C. Delta Lake の .option(\\"mergeSchema\\", \\"true\\") オプションのみでストリームを書き込み、スキーマの変更があった場合は常に例外をスローさせてデータエンジニアに通知する。",
    "D. 取り込み前に Apache Spark のバッチ処理で一度 json データを読み込み、欠損値や未知の列を持つレコードをフィルタリングして除外してからパイプラインを再開する。"
  ],
  "answer": "B",
  "explanation": "Auto Loader の `rescue` モード（rescued data column）は、予期しないデータ（スキーマに定義されていない新しい列や、データ型の不一致が生じたデータなど）によるデータの損失やパイプラインの停止を防ぐための強力な機能です。このモードを有効にすると、パースに失敗したデータや未知のデータフィールドはすべて JSON 文字列として一時的に `_rescued_data` 列に安全に格納されます。これにより、データエンジニアはパイプラインの継続的な稼働を維持したまま、後から仕様変更を分析して対応することが可能になります。選択肢Aは後方互換性を壊す可能性があり、CとDは継続的な取り込みを妨げたりデータを欠損させるため不適切です。\\n\\n**参考ドキュメント:** https://docs.databricks.com/ja/ingestion/auto-loader/schema.html\\n**引用:** > The rescued data column ensures that you never lose or miss out on data during ETL. The rescued data column contains any data that wasn't parsed..."
}
```"""
}


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

    def generate_question(self, category: str | None = None, exam: str = TARGET_EXAM) -> dict | None:
        """RAG で問題を動的に生成"""
        if not self.is_available:
            return None

        import random

        try:
            # 指定された試験のシラバス情報を取得
            exam_data = SYLLABUSES.get(exam, {"categories": []})
            
            # カテゴリ別サブカテゴリキーワードをJSONから構築
            subcategory_queries = {
                cat["name"]: cat["keywords"] for cat in exam_data.get("categories", [])
            }

            # フォールバック処理（万が一シラバスが空の場合など）
            if not subcategory_queries:
                logger.error(f"試験 '{exam}' のシラバスが見つかりません。")
                return None

            if category and category in subcategory_queries:
                query = random.choice(subcategory_queries[category])
            else:
                cat_weights = {cat["name"]: cat["weight"] for cat in exam_data.get("categories", [])}
                category = random.choices(
                    list(cat_weights.keys()),
                    weights=list(cat_weights.values()),
                    k=1,
                )[0]
                query = random.choice(subcategory_queries[category])

            logger.info(f"検索クエリ: [{category}] {query}")

            # ドキュメント検索（多様性を持たせるため TOP 20 を取得）
            docs = self.search_documents(query, category=category, num_results=20)
            if not docs:
                logger.warning("検索結果が0件。静的問題にフォールバックします。")
                return None

            # 検索結果からランダムに 3〜5 件をサンプリング（問題の重複を防ぐため）
            docs_sample = random.sample(docs, min(len(docs), random.randint(3, 5)))

            # コンテキスト作成 (URLを含める)
            context_parts = []
            for doc in docs_sample:
                context_parts.append(f"[Source URL: {doc.get('source_url', 'URL不明')}]\n{doc['content']}")
            context = "\n\n---\n\n".join(context_parts)

            # 使用する Few-Shot サンプルを選択（見つからない場合は Associate をフォールバック）
            few_shot_example = FEW_SHOT_EXAMPLES.get(exam, FEW_SHOT_EXAMPLES["Data Engineer Associate"])

            # LLM で問題生成
            prompt = QUESTION_GENERATION_PROMPT.format(
                exam=exam,
                context=context,
                category=category or "全般",
                few_shot_example=few_shot_example,
            )

            response = self.workspace_client.serving_endpoints.query(
                name=self.serving_endpoint,
                messages=[
                    ChatMessage(
                        role=ChatMessageRole.SYSTEM,
                        content="あなたは Databricks 認定資格試験の問題作成エキスパートです。指定された JSON 形式で正確に出力してください。",
                    ),
                    ChatMessage(role=ChatMessageRole.USER, content=prompt),
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
