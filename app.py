import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from flask import Flask, render_template, request

app = Flask(__name__)

# DAM★とも AI採点URL
AI_SCORE_URL = "https://www.clubdam.com/app/damtomo/scoring/GetScoringAiListXML.do"

# 自分のCookie情報
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

def fetch_damtomo_ai_scores(username, cookies, max_pages=40):
    all_scores = []

    for page in range(1, max_pages + 1):
        params = {
            "cdmCardNo": cookies["scr_cdm"],
            "pageNo": page,
            "detailFlg": 0
        }
        res = requests.get(AI_SCORE_URL, cookies=cookies, params=params)
        res.raise_for_status()

        root = ET.fromstring(res.content)
        ns = {"ns": "https://www.clubdam.com/app/damtomo/scoring/GetScoringAiListXML"}
        status = root.find(".//ns:status", ns)
        if status is None or status.text != "OK":
            break

        scorings = root.findall(".//ns:scoring", ns)
        if not scorings:
            break

        for data in scorings:
            song = data.attrib.get("contentsName", "")
            singer = data.attrib.get("artistName", "")
            date = data.attrib.get("scoringDateTime", "")
            try:
                score = float(data.text) / 1000
            except (ValueError, TypeError):
                continue
            all_scores.append([song, singer, username, score, date])

    df = pd.DataFrame(all_scores, columns=["曲名", "歌手名", "ユーザー", "スコア", "日付"])
    df["日付"] = pd.to_datetime(df["日付"], errors="coerce")
    return df

@app.route("/", methods=["GET"])
def home():
    return render_template("home.html")

@app.route("/ranking", methods=["GET"])
def ranking():
    song_query = request.args.get("song", "")
    singer_query = request.args.get("singer", "")
    sort_order = request.args.get("sort", "asc")

    csv_file = "scores.csv"
    if os.path.exists(csv_file):
        df_all = pd.read_csv(csv_file)
        df_all["スコア"] = pd.to_numeric(df_all["スコア"], errors="coerce")
        df_all["日付"] = pd.to_datetime(df_all["日付"], errors="coerce")
    else:
        df_all = pd.DataFrame(columns=["曲名", "歌手名", "ユーザー", "スコア", "日付"])

    if song_query:
        df_all = df_all[df_all["曲名"].str.contains(song_query, na=False)]
    if singer_query:
        df_all = df_all[df_all["歌手名"].str.contains(singer_query, na=False)]

    df_all = df_all.sort_values("曲名", ascending=(sort_order=="asc"))

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
    song_query = request.form.get("song", "")
    singer_query = request.form.get("singer", "")
    sort_order = request.form.get("sort", "asc")

    csv_file = "scores.csv"
    df_all = pd.DataFrame()

    for user, cookies in USER_COOKIES.items():
        df = fetch_damtomo_ai_scores(user, cookies)
        if not df.empty:
            df_all = pd.concat([df_all, df], ignore_index=True) if not df_all.empty else df

    if not df_all.empty:
        df_all["スコア"] = pd.to_numeric(df_all["スコア"], errors="coerce")
        df_all["日付"] = pd.to_datetime(df_all["日付"], errors="coerce")

        # CSVが存在する場合、既存データと結合
        if os.path.exists(csv_file):
            df_existing = pd.read_csv(csv_file)
            df_existing["日付"] = pd.to_datetime(df_existing["日付"], errors="coerce")
            df_all = pd.concat([df_existing, df_all], ignore_index=True)

        # 曲名・ユーザー・日付（秒まで完全一致）の重複を削除
        df_all = df_all.drop_duplicates(subset=["曲名", "ユーザー", "日付"], keep="first")
        df_all.to_csv(csv_file, index=False)

    # ランキング作成
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

if __name__ == "__main__":
    # Render が指定するポートを取得
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)