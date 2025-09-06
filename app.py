import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import IntegrityError

# ---- Flask + DB setup ----
app = Flask(__name__)

# DATABASE_URL があればそれを使い、なければローカル用に SQLite を使う
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
else:
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///scores.db"

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

# ---- DB model ----
class Score(db.Model):
    __tablename__ = "scores"
    id = db.Column(db.Integer, primary_key=True)
    song = db.Column(db.String(500), nullable=False)    # 曲名
    singer = db.Column(db.String(300), nullable=True)   # 歌手名
    user = db.Column(db.String(200), nullable=False)    # ユーザー名
    score = db.Column(db.Float, nullable=False)         # スコア
    date = db.Column(db.DateTime, nullable=False)       # 日付（日時）
    __table_args__ = (UniqueConstraint('song', 'user', 'date', name='_song_user_date_uc'),)

    def to_record(self):
        return {
            "曲名": self.song,
            "歌手名": self.singer,
            "ユーザー": self.user,
            "スコア": self.score,
            "日付": self.date
        }

# テーブル作成（起動時に存在しなければ作る）
with app.app_context():
    db.create_all()

# ---- Config / constants ----
AI_SCORE_URL = "https://www.clubdam.com/app/damtomo/scoring/GetScoringAiListXML.do"

# 事前登録の Cookie 情報（まずは自分のみ）
USER_COOKIES = {
    "まつりく": {
        "dam-uid": "5c6851a696db3f0488cea63754f839d70810012f131432c88a487c2227af8429",
        "scr_cdm": "ODAwMDEwMjY3MjIwOTY5",
        "scr_dt": "MTI0NTUwMDU0",
        "webmember": "1",
        "wm_ac": "matsuriku0331",
        "wm_dm": "softbank.ne.jp"
    }
}

# ---- Helper: fetch DAM★とも AI scores (multi-page) ----
def fetch_damtomo_ai_scores(username, cookies, max_pages=40):
    """
    username: 表示名（ユーザー）
    cookies: dict of cookie keys required by DAM★とも
    max_pages: 最大ページ数（デフォルト40）
    戻り値: pandas.DataFrame with columns ["曲名","歌手名","ユーザー","スコア","日付"]
    """
    all_scores = []

    for page in range(1, max_pages + 1):
        params = {
            "cdmCardNo": cookies.get("scr_cdm", ""),
            "pageNo": page,
            "detailFlg": 0
        }
        try:
            res = requests.get(AI_SCORE_URL, cookies=cookies, params=params, timeout=15)
            res.raise_for_status()
        except Exception as e:
            # 通信エラーやHTTPエラーはログに出して中断
            print(f"[fetch] {username} page {page} request failed: {e}")
            break

        try:
            root = ET.fromstring(res.content)
        except Exception as e:
            print(f"[fetch] {username} page {page} XML parse failed: {e}")
            break

        ns = {"ns": "https://www.clubdam.com/app/damtomo/scoring/GetScoringAiListXML"}
        status = root.find(".//ns:status", ns)
        if status is None or status.text != "OK":
            # ステータスが OK でない場合、終了
            # ただし最初のページでNGなら空DFを返す
            print(f"[fetch] {username} page {page} status not OK; stopping.")
            break

        scorings = root.findall(".//ns:scoring", ns)
        if not scorings:
            # ページにデータが無ければ終了
            break

        for data in scorings:
            song = data.attrib.get("contentsName", "").strip()
            singer = data.attrib.get("artistName", "").strip()
            date_str = data.attrib.get("scoringDateTime", "").strip()
            # score は element のテキスト。例: "90000" -> 90.0
            try:
                raw = data.text
                if raw is None:
                    continue
                score_val = float(raw) / 1000.0
            except (ValueError, TypeError):
                continue

            # そのまま文字列として格納し、DataFrame化後に to_datetime
            all_scores.append([song, singer, username, score_val, date_str])

    df = pd.DataFrame(all_scores, columns=["曲名", "歌手名", "ユーザー", "スコア", "日付"])
    if not df.empty:
        df["日付"] = pd.to_datetime(df["日付"], errors="coerce")
    return df

# ---- Helper: insert DataFrame rows into DB (skip duplicates) ----
def insert_scores_from_df(df_new):
    """
    df_new: DataFrame with columns ["曲名","歌手名","ユーザー","スコア","日付"]
    日付は datetime 型（または変換可能な文字列）であることを想定。
    重複判定は (song, user, date) が同一（秒まで）ならスキップ。
    戻り値: 挿入した行数
    """
    if df_new.empty:
        return 0

    # Ensure 日付 is datetime
    df_new = df_new.copy()
    df_new["日付"] = pd.to_datetime(df_new["日付"], errors="coerce")
    inserted = 0

    session = db.session
    for _, r in df_new.iterrows():
        if pd.isna(r["日付"]) or pd.isna(r["スコア"]):
            continue
        song = str(r["曲名"])
        singer = str(r["歌手名"]) if not pd.isna(r["歌手名"]) else None
        user = str(r["ユーザー"])
        score_val = float(r["スコア"])
        date_val = r["日付"].to_pydatetime() if hasattr(r["日付"], "to_pydatetime") else r["日付"]

        # DBに同じ (song,user,date) があるかクエリでチェックする（秒まで完全一致）
        exists = session.query(Score).filter_by(song=song, user=user, date=date_val).first()
        if exists:
            continue

        s = Score(song=song, singer=singer, user=user, score=score_val, date=date_val)
        session.add(s)
        try:
            session.commit()
            inserted += 1
        except IntegrityError:
            # 一意制約に抵触した場合はロールバックしてスキップ
            session.rollback()
        except Exception as e:
            session.rollback()
            print(f"[insert] unexpected error inserting {song}/{user}/{date_val}: {e}")
    return inserted

# ---- Helper: read all data from DB into DataFrame ----
def df_from_db():
    rows = db.session.query(Score).all()
    data = []
    for r in rows:
        data.append({
            "曲名": r.song,
            "歌手名": r.singer,
            "ユーザー": r.user,
            "スコア": r.score,
            "日付": r.date
        })
    if not data:
        return pd.DataFrame(columns=["曲名", "歌手名", "ユーザー", "スコア", "日付"])
    df = pd.DataFrame(data)
    return df

# ---- Routes ----
@app.route("/", methods=["GET"])
def home():
    return render_template("home.html")


@app.route("/ranking", methods=["GET"])
def ranking():
    # 検索・ソートパラメータ（検索フォームから GET）
    song_query = request.args.get("song", "")
    singer_query = request.args.get("singer", "")
    sort_order = request.args.get("sort", "asc")

    # DB から読み出し
    df_all = df_from_db()

    # フィルタリング（部分一致）
    if song_query:
        df_all = df_all[df_all["曲名"].str.contains(song_query, na=False)]
    if singer_query:
        df_all = df_all[df_all["歌手名"].str.contains(singer_query, na=False)]

    # 曲名でソート（あいうえお順をエミュレート、単純ソート）
    df_all = df_all.sort_values("曲名", ascending=(sort_order == "asc"))

    # 曲ごと・ユーザーごとの最高得点を算出してトップ3を作る
    ranking_data = {}
    if not df_all.empty:
        best_scores = df_all.groupby(["曲名", "ユーザー"], as_index=False).agg(
            {"歌手名": "first", "スコア": "max", "日付": "max"}
        )
        for song, group in best_scores.groupby("曲名"):
            ranking_data[song] = group.sort_values("スコア", ascending=False).head(3).to_dict(orient="records")

    return render_template("ranking.html",
                           ranking_data=ranking_data,
                           song_query=song_query,
                           singer_query=singer_query,
                           sort_order=sort_order)


@app.route("/update_ranking", methods=["POST"])
def update_ranking():
    # 検索条件保持（フォームの hidden から）
    song_query = request.form.get("song", "")
    singer_query = request.form.get("singer", "")
    sort_order = request.form.get("sort", "asc")

    total_inserted = 0
    # 全ての事前登録ユーザー分を取得して DB に挿入（重複スキップ）
    for user, cookies in USER_COOKIES.items():
        df_new = fetch_damtomo_ai_scores(user, cookies)
        if not df_new.empty:
            inserted = insert_scores_from_df(df_new)
            total_inserted += inserted
            print(f"[update] {user}: inserted {inserted} rows")

    # 更新後に DB から読み直してランキング作成
    df_all = df_from_db()
    ranking_data = {}
    if not df_all.empty:
        best_scores = df_all.groupby(["曲名", "ユーザー"], as_index=False).agg(
            {"歌手名": "first", "スコア": "max", "日付": "max"}
        )
        for song, group in best_scores.groupby("曲名"):
            ranking_data[song] = group.sort_values("スコア", ascending=False).head(3).to_dict(orient="records")

    # （任意）テンプレートで「追加件数」を表示したい場合は渡せます
    return render_template("ranking.html",
                           ranking_data=ranking_data,
                           song_query=song_query,
                           singer_query=singer_query,
                           sort_order=sort_order)


# ---- 起動（Render 対応） ----
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    # debug=False for production; local testing is fine
    app.run(host="0.0.0.0", port=port, debug=False)