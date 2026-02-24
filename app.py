"""
Databricks 資格試験 練習ボットアプリ

Gradio ベースの試験練習アプリ。トップページでモード選択後、
出題ページで問題に回答する2画面構成。

モード:
- 📝 試験勉強モード: 全5分野からランダムに20問出題
- 📂 トピック別モード: 分野を選択して20問集中出題
"""

import json
import os
import random
import logging
import gradio as gr

from rag_engine import RAGEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# シラバスの読み込み
SYLLABUSES_PATH = os.path.join(os.path.dirname(__file__), "syllabuses.json")
try:
    with open(SYLLABUSES_PATH, "r", encoding="utf-8") as f:
        SYLLABUSES = json.load(f)
except Exception as e:
    logger.error(f"シラバスの読み込みエラー: {e}")
    SYLLABUSES = {"Data Engineer Associate": {"categories": []}}

AVAILABLE_EXAMS = list(SYLLABUSES.keys())

# 設定
QUESTIONS_PER_SESSION = 20
STATIC_QUESTIONS_PATH = os.path.join(os.path.dirname(__file__), "questions.json")


def load_static_questions() -> list[dict]:
    try:
        with open(STATIC_QUESTIONS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"静的問題の読み込みエラー: {e}")
        return []


rag_engine = RAGEngine()
static_questions = load_static_questions()


# ============================================================
# ビジネスロジック
# ============================================================

def get_static_questions_for_session(category: str | None = None) -> list[dict]:
    if category:
        filtered = [q for q in static_questions if q["category"] == category]
    else:
        filtered = list(static_questions)
    random.shuffle(filtered)
    return filtered


def get_next_question(state: dict) -> dict | None:
    exam = state.get("exam", AVAILABLE_EXAMS[0])
    category = state.get("topic")

    # 指定試験のカテゴリリストとウェイトを取得
    exam_data = SYLLABUSES.get(exam, {"categories": []})
    cat_weights = {cat["name"]: cat["weight"] for cat in exam_data.get("categories", [])}

    if not category and cat_weights:
        cats = list(cat_weights.keys())
        weights = list(cat_weights.values())
        category = random.choices(cats, weights=weights, k=1)[0]

    question = None
    if rag_engine.is_available:
        question = rag_engine.generate_question(category=category, exam=exam)

    if question is None:
        pool = state.get("static_pool", [])
        idx = state.get("static_index", 0)
        if idx < len(pool):
            question = pool[idx].copy()
            question["source"] = "static"
            state["static_index"] = idx + 1
        elif static_questions:
            if category:
                pool = [q for q in static_questions if q["category"] == category]
            else:
                pool = list(static_questions)
            random.shuffle(pool)
            if pool:
                question = pool[0].copy()
                question["source"] = "static"

    return question


def init_state(exam: str, mode: str, topic: str) -> dict:
    state = {
        "exam": exam,
        "mode": mode,
        "topic": topic if mode == "📂 トピック別モード" else None,
        "current_index": 0,
        "score": 0,
        "answered": 0,
        "current_question": None,
        "category_scores": {cat["name"]: [0, 0] for cat in SYLLABUSES.get(exam, {"categories": []}).get("categories", [])},
        "finished": False,
    }
    category = state["topic"]
    state["static_pool"] = get_static_questions_for_session(category)
    state["static_index"] = 0
    return state


# ============================================================
# UI ヘルパー
# ============================================================

def format_progress(answered: int, score: int) -> str:
    accuracy = (score / answered * 100) if answered > 0 else 0
    bar = "█" * int(accuracy / 10) + "░" * (10 - int(accuracy / 10))
    return f"問題 {answered + 1} / {QUESTIONS_PER_SESSION}　|　正答率: {score}/{answered} ({accuracy:.0f}%) {bar}"


def format_final_score(score: int, total: int, category_scores: dict) -> str:
    accuracy = (score / total * 100) if total > 0 else 0
    status = "🎉 合格ライン達成！" if accuracy >= 70 else "📖 もう少し頑張りましょう！"

    lines = [
        f"# 🏁 試験終了！",
        f"## 最終スコア: {score}/{total} ({accuracy:.0f}%)",
        f"### {status}",
        f"（合格ライン: 70%）",
        "",
        "### 分野別成績",
    ]
    for cat, (correct, total_cat) in category_scores.items():
        if total_cat > 0:
            cat_acc = correct / total_cat * 100
            bar = "█" * int(cat_acc / 10) + "░" * (10 - int(cat_acc / 10))
            lines.append(f"- **{cat}**: {correct}/{total_cat} ({cat_acc:.0f}%) {bar}")

    return "\n".join(lines)


# ============================================================
# イベントハンドラ
# ============================================================

def on_start(exam: str, mode: str, topic: str):
    """開始ボタン → 出題ページに切り替え"""
    state = init_state(exam, mode, topic)
    question = get_next_question(state)
    if question is None:
        gr.Warning("問題の取得に失敗しました")
        return (
            gr.update(visible=True),   # top_page
            gr.update(visible=False),  # quiz_page
            gr.update(visible=False),  # result_page
            state,
            gr.update(),  # category_label
            gr.update(),  # question_text
            gr.update(),  # answer_radio
            gr.update(),  # progress_text
            gr.update(),  # feedback_box
            gr.update(),  # feedback_content
            gr.update(),  # submit_btn
            gr.update(),  # next_btn
        )

    state["current_question"] = question
    source_label = "🤖 AI生成" if question.get("source") == "ai_generated" else "📋 静的問題"
    progress = format_progress(0, 0)

    choices = question["choices"]

    return (
        gr.update(visible=False),   # top_page を非表示
        gr.update(visible=True),    # quiz_page を表示
        gr.update(visible=False),   # result_page
        state,
        # quiz_page の中身
        f"**[{question['category']}]**　{source_label}",  # category_label
        question["question"],       # question_text
        gr.update(choices=choices, value=None, interactive=True),  # answer_radio
        progress,                   # progress_text
        gr.update(visible=False),   # feedback_box
        "",                         # feedback_content
        gr.update(visible=True, interactive=True),   # submit_btn
        gr.update(visible=False),   # next_btn
    )


def on_submit(selected_answer: str, state: dict):
    """回答送信ボタン"""
    if not selected_answer:
        gr.Warning("選択肢を選んでください")
        return (
            state,
            gr.update(),  # feedback_box
            "",           # feedback_content
            gr.update(),  # submit_btn
            gr.update(),  # next_btn
            gr.update(),  # answer_radio
            gr.update(),  # progress_text
        )

    question = state["current_question"]
    user_answer = selected_answer[0]  # "A. ..." → "A"
    is_correct = user_answer == question["answer"]

    state["answered"] += 1
    if is_correct:
        state["score"] += 1

    cat = question.get("category", "")
    if cat in state["category_scores"]:
        state["category_scores"][cat][1] += 1
        if is_correct:
            state["category_scores"][cat][0] += 1

    # 正解の選択肢テキストを取得
    correct_choice = ""
    for c in question["choices"]:
        if c.startswith(f"{question['answer']}."):
            correct_choice = c
            break

    if is_correct:
        icon = "✅ **正解！**"
    else:
        icon = f"❌ **不正解** — 正解: **{correct_choice}**"

    feedback = f"{icon}\n\n📖 **解説:**\n{question['explanation']}"
    progress = format_progress(state["answered"], state["score"])

    is_last = state["answered"] >= QUESTIONS_PER_SESSION

    return (
        state,
        gr.update(visible=True),   # feedback_box
        feedback,                  # feedback_content
        gr.update(visible=False),  # submit_btn を消す
        gr.update(visible=True, value="📊 結果を見る" if is_last else "次の問題 →"),  # next_btn
        gr.update(interactive=False),  # answer_radio を無効化
        progress,                  # progress_text
    )


def on_next(state: dict):
    """次の問題ボタン or 結果表示"""
    if state["answered"] >= QUESTIONS_PER_SESSION:
        # 結果画面へ
        result_text = format_final_score(
            state["score"], state["answered"], state["category_scores"]
        )
        return (
            gr.update(visible=False),  # quiz_page
            gr.update(visible=True),   # result_page
            result_text,               # result_content
            state,
            gr.update(),  # category_label
            gr.update(),  # question_text
            gr.update(),  # answer_radio
            gr.update(),  # progress_text
            gr.update(),  # feedback_box
            gr.update(),  # feedback_content
            gr.update(),  # submit_btn
            gr.update(),  # next_btn
        )

    # 次の問題
    question = get_next_question(state)
    if question is None:
        gr.Warning("問題の取得に失敗しました")
        return (
            gr.update(),  # quiz_page
            gr.update(),  # result_page
            "",           # result_content
            state,
            gr.update(),  # category_label
            gr.update(),  # question_text
            gr.update(),  # answer_radio
            gr.update(),  # progress_text
            gr.update(),  # feedback_box
            gr.update(),  # feedback_content
            gr.update(),  # submit_btn
            gr.update(),  # next_btn
        )

    state["current_question"] = question
    source_label = "🤖 AI生成" if question.get("source") == "ai_generated" else "📋 静的問題"
    progress = format_progress(state["answered"], state["score"])

    return (
        gr.update(visible=True),   # quiz_page
        gr.update(visible=False),  # result_page
        "",                        # result_content
        state,
        # quiz_page の中身
        f"**[{question['category']}]**　{source_label}",
        question["question"],
        gr.update(choices=question["choices"], value=None, interactive=True),
        progress,
        gr.update(visible=False),   # feedback_box
        "",                         # feedback_content
        gr.update(visible=True, interactive=True),   # submit_btn
        gr.update(visible=False),   # next_btn
    )


def on_back_to_top():
    """トップページに戻る"""
    return (
        gr.update(visible=True),   # top_page
        gr.update(visible=False),  # quiz_page
        gr.update(visible=False),  # result_page
    )


# ============================================================
# Gradio UI
# ============================================================

CUSTOM_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;500;700;800&display=swap');

* { font-family: 'Noto Sans JP', sans-serif !important; }
.gradio-container { max-width: 850px !important; margin: auto !important; }
footer { display: none !important; }
.top-header {
    text-align: center; padding: 30px 0 10px;
    background: linear-gradient(135deg, #FF6B35 0%, #FF3860 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    font-size: 2em; font-weight: 800;
}
.mode-card {
    border: 2px solid #e0e0e0; border-radius: 12px;
    padding: 20px; transition: all 0.3s;
}
.mode-card:hover { border-color: #FF6B35; box-shadow: 0 4px 16px rgba(255,107,53,0.15); }
.start-btn {
    background: linear-gradient(135deg, #FF6B35, #FF3860) !important;
    border: none !important; font-weight: bold !important; font-size: 18px !important;
    padding: 14px 0 !important; border-radius: 10px !important;
}
.quiz-question {
    font-size: 17px !important; line-height: 1.8 !important;
    padding: 20px !important; border-radius: 10px !important;
    border-left: 4px solid #FF6B35 !important;
}
/* ラジオボタン縦並び（1選択肢1行） */
.choice-radio .wrap {
    display: flex !important; flex-direction: column !important;
    gap: 8px !important;
}
.choice-radio .wrap label {
    padding: 12px 16px !important; border: 2px solid #e0e0e0 !important;
    border-radius: 10px !important; cursor: pointer !important;
    transition: all 0.2s !important; font-size: 15px !important;
}
.choice-radio .wrap label:hover {
    border-color: #FF6B35 !important; background: rgba(255,107,53,0.05) !important;
}
.choice-radio .wrap label.selected {
    border-color: #FF6B35 !important; background: rgba(255,107,53,0.1) !important;
}
.feedback-box {
    padding: 18px !important; border-radius: 10px !important;
    line-height: 1.7 !important;
}
.next-btn {
    font-size: 16px !important; font-weight: bold !important;
    padding: 12px 0 !important; border-radius: 10px !important;
}
.result-box { line-height: 1.8 !important; }
"""


def create_app():
    with gr.Blocks(
        title="Databricks 資格試験 練習ボット",
        css=CUSTOM_CSS,
        theme=gr.themes.Soft(
            primary_hue="orange",
            secondary_hue="blue",
            neutral_hue="slate",
        ),
    ) as app:
        state = gr.State({})

        # ============================
        # トップページ
        # ============================
        with gr.Column(visible=True) as top_page:
            gr.Markdown(
                "# 🎓 Databricks 資格試験 練習ボット\n"
                "対象試験を選んで、練習問題を解きましょう！",
                elem_classes=["top-header"],
            )
            gr.Markdown(
                "Databricks 公式ドキュメントに基づいた問題で、本番試験に向けた効果的な学習ができます。\n"
                f"各モード **{QUESTIONS_PER_SESSION}問** 出題されます。",
            )

            ai_status = "✅ AI生成問題 利用可能" if rag_engine.is_available else "ℹ️ 静的問題のみ（AI生成はDatabricks上で利用可能）"
            gr.Markdown(f"**ステータス:** {ai_status}")

            with gr.Row():
                with gr.Column(elem_classes=["mode-card"]):
                    gr.Markdown("### 📝 試験勉強モード\n全5分野からランダムに出題。\n本番試験のシミュレーションに最適！")
                with gr.Column(elem_classes=["mode-card"]):
                    gr.Markdown("### 📂 トピック別モード\n苦手分野を集中的に学習。\n分野を選んでトレーニング！")

            with gr.Row():
                exam_selector = gr.Dropdown(
                    choices=AVAILABLE_EXAMS,
                    value=AVAILABLE_EXAMS[0],
                    label="📝 対象の認定試験を選択",
                    elem_classes=["exam-selector"]
                )

            with gr.Row():
                mode_selector = gr.Dropdown(
                    choices=["📝 試験勉強モード", "📂 トピック別モード"],
                    value="📝 試験勉強モード",
                    label="モードを選択",
                )
                
                # 初期表示のカテゴリリスト（一番目の試験のもの）
                initial_cats = [cat["name"] for cat in SYLLABUSES.get(AVAILABLE_EXAMS[0], {"categories": []}).get("categories", [])]
                topic_selector = gr.Dropdown(
                    choices=initial_cats,
                    value=initial_cats[0] if initial_cats else None,
                    label="トピック（トピック別モード時）",
                )

            start_btn = gr.Button("🚀 試験を開始する", variant="primary", size="lg", elem_classes=["start-btn"])

        # ============================
        # 出題ページ
        # ============================
        with gr.Column(visible=False) as quiz_page:
            progress_text = gr.Markdown("問題 1 / 20")
            category_label = gr.Markdown("")
            question_text = gr.Markdown("", elem_classes=["quiz-question"])
            answer_radio = gr.Radio(choices=[], label="回答を選択してください", interactive=True, elem_classes=["choice-radio"])
            submit_btn = gr.Button("✔ 回答する", variant="primary", size="lg")

            with gr.Column(visible=False) as feedback_box:
                feedback_content = gr.Markdown("", elem_classes=["feedback-box"])

            with gr.Row():
                next_btn = gr.Button("次の問題 →", variant="secondary", size="lg", visible=False, elem_classes=["next-btn"])
                back_to_top_btn_quiz = gr.Button("🔄 トップに戻る", variant="stop", size="lg", visible=True)

        # ============================
        # 結果ページ
        # ============================
        with gr.Column(visible=False) as result_page:
            result_content = gr.Markdown("", elem_classes=["result-box"])
            back_btn = gr.Button("🔄 トップに戻る", variant="primary", size="lg")

        # ============================
        # イベント接続
        # ============================
        
        def update_topics_for_exam(exam: str):
            cats = [cat["name"] for cat in SYLLABUSES.get(exam, {"categories": []}).get("categories", [])]
            return gr.update(choices=cats, value=cats[0] if cats else None)

        exam_selector.change(
            fn=update_topics_for_exam,
            inputs=[exam_selector],
            outputs=[topic_selector]
        )

        def set_loading_state_start():
            return gr.update(value="🚀 問題を生成中... (約10秒)", interactive=False)

        def set_loading_state_next():
            return gr.update(value="⏳ 次の問題を生成中...", interactive=False)

        start_btn.click(
            fn=set_loading_state_start,
            inputs=[],
            outputs=[start_btn],
        ).then(
            fn=on_start,
            inputs=[exam_selector, mode_selector, topic_selector],
            outputs=[
                top_page, quiz_page, result_page, state,
                category_label, question_text, answer_radio, progress_text,
                feedback_box, feedback_content, submit_btn, next_btn,
            ],
            show_progress="full",
        ).then(
            fn=lambda: gr.update(value="🚀 試験を開始する", interactive=True),
            inputs=[],
            outputs=[start_btn],
        )

        submit_btn.click(
            fn=on_submit,
            inputs=[answer_radio, state],
            outputs=[
                state, feedback_box, feedback_content,
                submit_btn, next_btn, answer_radio, progress_text,
            ],
        )

        next_btn.click(
            fn=set_loading_state_next,
            inputs=[],
            outputs=[next_btn],
        ).then(
            fn=on_next,
            inputs=[state],
            outputs=[
                quiz_page, result_page, result_content, state,
                category_label, question_text, answer_radio, progress_text,
                feedback_box, feedback_content, submit_btn, next_btn,
            ],
            show_progress="full",
        ).then(
            fn=lambda: gr.update(value="次の問題 →", interactive=True),
            inputs=[],
            outputs=[next_btn],
        )

        back_btn.click(
            fn=on_back_to_top,
            inputs=[],
            outputs=[top_page, quiz_page, result_page],
        )

        back_to_top_btn_quiz.click(
            fn=on_back_to_top,
            inputs=[],
            outputs=[top_page, quiz_page, result_page],
        )

    return app


if __name__ == "__main__":
    app = create_app()
    app.launch(
        server_name="0.0.0.0",
        server_port=int(os.environ.get("PORT", 8000)),
    )

