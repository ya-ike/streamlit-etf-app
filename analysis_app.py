
# -*- coding: utf-8 -*-
"""
analysis_app.py
Google Sheets の「売買記録台帳」「日次サマリー」を読み込んで集計する Streamlit アプリ
"""

from __future__ import annotations

from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
import numpy as np
import pandas as pd
import streamlit as st

JST = ZoneInfo("Asia/Tokyo")

st.set_page_config(page_title="日米時差ETF戦略 / 集計アプリ", page_icon="📊", layout="wide")


# -----------------------------
# Google Sheets helpers
# -----------------------------
@st.cache_resource(show_spinner=False)
def get_gspread_client():
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Secrets に [gcp_service_account] がありません。")
    return gspread.service_account_from_dict(dict(st.secrets["gcp_service_account"]))


@st.cache_resource(show_spinner=False)
def open_workbook():
    client = get_gspread_client()
    sheet_name = st.secrets.get("sheets", {}).get("spreadsheet_name", "ETF_運用台帳")
    return client.open(sheet_name)


def get_or_create_ws(book, title: str):
    try:
        return book.worksheet(title)
    except gspread.WorksheetNotFound:
        return book.add_worksheet(title=title, rows=200, cols=40)


def read_ws_df(title: str) -> pd.DataFrame:
    ws = get_or_create_ws(open_workbook(), title)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    headers = values[0]
    rows = values[1:]
    if not headers:
        return pd.DataFrame()

    # すべて空の行を除外
    clean_rows = []
    for r in rows:
        padded = r + [""] * max(0, len(headers) - len(r))
        if any(str(x).strip() != "" for x in padded):
            clean_rows.append(padded[:len(headers)])

    if not clean_rows:
        return pd.DataFrame(columns=headers)

    return pd.DataFrame(clean_rows, columns=headers)


# -----------------------------
# Preprocess
# -----------------------------


def normalize_date_like(val) -> str:
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if s == "" or s.lower() == "none":
        return ""
    s = s.replace("/", "-").replace(".", "-")
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return ""
    return pd.Timestamp(ts).strftime("%Y-%m-%d")

def normalize_exec_flag(val) -> str:
    s = str(val).strip()
    mapping = {
        "〇": "〇",
        "○": "〇",
        "◯": "〇",
        "o": "〇",
        "O": "〇",
        "×": "×",
        "✕": "×",
        "x": "×",
        "X": "×",
    }
    return mapping.get(s, s)


def clean_numeric_series(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype=float)
    s = series.astype(str)
    s = (
        s.str.replace("¥", "", regex=False)
         .str.replace(",", "", regex=False)
         .str.replace("%", "", regex=False)
         .str.strip()
    )
    s = s.replace({"": np.nan, "None": np.nan, "nan": np.nan, "NaN": np.nan})
    return pd.to_numeric(s, errors="coerce")


def preprocess_trade_log(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    # 列名の空白除去
    df.columns = [str(c).strip() for c in df.columns]

    # 必須列がない場合でも落ちにくくする
    required = ["売買日", "日本ETFコード", "日本ETF名", "実行有無", "買値", "売値", "口数"]
    for col in required:
        if col not in df.columns:
            df[col] = ""

    # 空白除去
    for col in ["売買日", "日本ETFコード", "日本ETF名", "実行有無", "入力チェック", "メモ"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df["実行有無"] = df["実行有無"].apply(normalize_exec_flag)

    # 数値列
    num_cols = [
        "予定順位", "予定スコア", "予定予算", "1口金額", "予定口数",
        "予定約定金額", "買値", "売値", "口数", "損益額", "損益率"
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = clean_numeric_series(df[col])

    # 日付
    df["売買日_key"] = df["売買日"].apply(normalize_date_like)
    df["売買日"] = pd.to_datetime(df["売買日_key"], errors="coerce")

    # 集計対象: 実行有無が〇 かつ 買値・売値・口数が揃っている
    df = df[df["実行有無"] == "〇"].copy()
    df = df.dropna(subset=["売買日", "買値", "売値", "口数"])

    if df.empty:
        return df

    # Python側で再計算
    df["損益額_再計算"] = (df["売値"] - df["買値"]) * df["口数"]
    df["損益率_再計算"] = np.where(df["買値"] != 0, (df["売値"] - df["買値"]) / df["買値"], np.nan)
    df["年月"] = df["売買日"].dt.strftime("%Y-%m")
    df["日付"] = df["売買日"].dt.strftime("%Y-%m-%d")

    return df.reset_index(drop=True)


def preprocess_daily_signal_log(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    df.columns = [str(c).strip() for c in df.columns]

    for col in ["シグナル日付", "見送り候補", "見送り理由"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    if "シグナル日付" not in df.columns:
        df["シグナル日付"] = ""

    df["シグナル日付_key"] = df["シグナル日付"].apply(normalize_date_like)
    df["シグナル日付"] = pd.to_datetime(df["シグナル日付_key"], errors="coerce")
    df = df.dropna(subset=["シグナル日付"]).copy()

    if df.empty:
        return df

    # 真偽値の正規化
    if "見送り候補" in df.columns:
        df["見送り候補"] = (
            df["見送り候補"].astype(str).str.strip().str.lower()
            .map({"true": True, "false": False, "1": True, "0": False})
            .fillna(False)
        )

    num_cols = ["PCA主成分数", "1位スコア", "1位-4位差", "フィルタ通過本数", "最終採用本数"]
    for col in num_cols:
        if col in df.columns:
            df[col] = clean_numeric_series(df[col])

    df["年月"] = df["シグナル日付"].dt.strftime("%Y-%m")
    df["日付"] = df["シグナル日付"].dt.strftime("%Y-%m-%d")

    return df.reset_index(drop=True)


# -----------------------------
# Aggregate
# -----------------------------
def make_overall_summary(trade_df: pd.DataFrame) -> pd.DataFrame:
    if trade_df.empty:
        return pd.DataFrame([{
            "対象件数": 0,
            "勝ち数": 0,
            "負け数": 0,
            "勝率": np.nan,
            "総損益": 0,
            "平均損益": np.nan,
            "平均損益率": np.nan,
        }])

    win_count = int((trade_df["損益額_再計算"] > 0).sum())
    lose_count = int((trade_df["損益額_再計算"] < 0).sum())
    total_count = int(len(trade_df))

    return pd.DataFrame([{
        "対象件数": total_count,
        "勝ち数": win_count,
        "負け数": lose_count,
        "勝率": win_count / total_count if total_count > 0 else np.nan,
        "総損益": trade_df["損益額_再計算"].sum(),
        "平均損益": trade_df["損益額_再計算"].mean(),
        "平均損益率": trade_df["損益率_再計算"].mean(),
    }])


def make_daily_summary(trade_df: pd.DataFrame, daily_signal_df: pd.DataFrame) -> pd.DataFrame:
    if trade_df.empty:
        exec_daily = pd.DataFrame(columns=[
            "日付", "実行本数", "日次総損益", "日次平均損益", "日次平均損益率", "勝ち数", "負け数", "日次勝率"
        ])
    else:
        exec_daily = (
            trade_df.groupby("日付", as_index=False)
            .agg(
                実行本数=("日本ETFコード", "count"),
                日次総損益=("損益額_再計算", "sum"),
                日次平均損益=("損益額_再計算", "mean"),
                日次平均損益率=("損益率_再計算", "mean"),
                勝ち数=("損益額_再計算", lambda x: int((x > 0).sum())),
                負け数=("損益額_再計算", lambda x: int((x < 0).sum())),
            )
            .sort_values("日付")
            .reset_index(drop=True)
        )
        exec_daily["日次勝率"] = np.where(exec_daily["実行本数"] > 0, exec_daily["勝ち数"] / exec_daily["実行本数"], np.nan)

    if daily_signal_df.empty:
        signal_daily = pd.DataFrame(columns=["日付", "見送り候補", "見送り理由", "フィルタ通過本数", "採用本数"])
    else:
        cols = ["日付", "見送り候補", "見送り理由", "フィルタ通過本数", "最終採用本数"]
        use_cols = [c for c in cols if c in daily_signal_df.columns]
        signal_daily = daily_signal_df[use_cols].copy()
        signal_daily = signal_daily.rename(columns={"最終採用本数": "採用本数"})
        signal_daily = signal_daily.dropna(subset=["日付"]).copy()

    daily = pd.merge(signal_daily, exec_daily, on="日付", how="outer")

    for col in ["採用本数", "フィルタ通過本数", "実行本数", "勝ち数", "負け数"]:
        if col in daily.columns:
            daily[col] = pd.to_numeric(daily[col], errors="coerce").fillna(0).astype(int)

    if "見送り候補" in daily.columns:
        daily["見送り候補"] = daily["見送り候補"].fillna(False)

    daily["見送り率"] = np.where(
        daily.get("採用本数", 0) > 0,
        1 - (daily.get("実行本数", 0) / daily.get("採用本数", 0)),
        np.nan
    )

    if "日次総損益" in daily.columns:
        daily["日次総損益"] = pd.to_numeric(daily["日次総損益"], errors="coerce").fillna(0)
        daily["累積損益"] = daily["日次総損益"].cumsum()

    daily = daily.sort_values("日付").reset_index(drop=True)

    cols = [
        "日付", "見送り候補", "見送り理由", "フィルタ通過本数", "採用本数",
        "実行本数", "見送り率", "勝ち数", "負け数", "日次勝率",
        "日次総損益", "日次平均損益", "日次平均損益率", "累積損益"
    ]
    return daily[[c for c in cols if c in daily.columns]]


def make_etf_summary(trade_df: pd.DataFrame) -> pd.DataFrame:
    if trade_df.empty:
        return pd.DataFrame(columns=[
            "日本ETFコード", "日本ETF名", "件数", "総損益", "平均損益", "平均損益率", "勝ち数", "負け数", "勝率"
        ])

    etf = (
        trade_df.groupby(["日本ETFコード", "日本ETF名"], as_index=False)
        .agg(
            件数=("売買日", "count"),
            総損益=("損益額_再計算", "sum"),
            平均損益=("損益額_再計算", "mean"),
            平均損益率=("損益率_再計算", "mean"),
            勝ち数=("損益額_再計算", lambda x: int((x > 0).sum())),
            負け数=("損益額_再計算", lambda x: int((x < 0).sum())),
        )
        .reset_index(drop=True)
    )
    etf["勝率"] = np.where(etf["件数"] > 0, etf["勝ち数"] / etf["件数"], np.nan)
    etf = etf.sort_values(["総損益", "勝率"], ascending=[False, False]).reset_index(drop=True)
    return etf


def make_monthly_summary(trade_df: pd.DataFrame, daily_signal_df: pd.DataFrame) -> pd.DataFrame:
    if trade_df.empty:
        exec_monthly = pd.DataFrame(columns=[
            "年月", "実行本数", "月次総損益", "月次平均損益", "月次平均損益率", "勝ち数", "負け数", "月次勝率"
        ])
    else:
        exec_monthly = (
            trade_df.groupby("年月", as_index=False)
            .agg(
                実行本数=("日本ETFコード", "count"),
                月次総損益=("損益額_再計算", "sum"),
                月次平均損益=("損益額_再計算", "mean"),
                月次平均損益率=("損益率_再計算", "mean"),
                勝ち数=("損益額_再計算", lambda x: int((x > 0).sum())),
                負け数=("損益額_再計算", lambda x: int((x < 0).sum())),
            )
            .sort_values("年月")
            .reset_index(drop=True)
        )
        exec_monthly["月次勝率"] = np.where(exec_monthly["実行本数"] > 0, exec_monthly["勝ち数"] / exec_monthly["実行本数"], np.nan)

    if daily_signal_df.empty:
        signal_monthly = pd.DataFrame(columns=["年月", "採用本数", "フィルタ通過本数"])
    else:
        signal_monthly = (
            daily_signal_df.groupby("年月", as_index=False)
            .agg(
                採用本数=("最終採用本数", "sum"),
                フィルタ通過本数=("フィルタ通過本数", "sum"),
            )
            .sort_values("年月")
            .reset_index(drop=True)
        )

    monthly = pd.merge(signal_monthly, exec_monthly, on="年月", how="outer")

    for col in ["採用本数", "フィルタ通過本数", "実行本数", "勝ち数", "負け数"]:
        if col in monthly.columns:
            monthly[col] = pd.to_numeric(monthly[col], errors="coerce").fillna(0).astype(int)

    monthly["見送り率"] = np.where(
        monthly.get("採用本数", 0) > 0,
        1 - (monthly.get("実行本数", 0) / monthly.get("採用本数", 0)),
        np.nan
    )

    if "月次総損益" in monthly.columns:
        monthly["月次総損益"] = pd.to_numeric(monthly["月次総損益"], errors="coerce").fillna(0)
        monthly["累積損益"] = monthly["月次総損益"].cumsum()

    monthly = monthly.sort_values("年月").reset_index(drop=True)

    cols = [
        "年月", "フィルタ通過本数", "採用本数", "実行本数", "見送り率",
        "勝ち数", "負け数", "月次勝率", "月次総損益",
        "月次平均損益", "月次平均損益率", "累積損益"
    ]
    return monthly[[c for c in cols if c in monthly.columns]]


# -----------------------------
# Formatting / export
# -----------------------------
def format_display_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    yen_cols = {"総損益", "平均損益", "日次総損益", "日次平均損益", "月次総損益", "月次平均損益", "累積損益"}
    pct_cols = {"勝率", "平均損益率", "日次平均損益率", "日次勝率", "月次平均損益率", "月次勝率", "見送り率"}

    for col in out.columns:
        if col in yen_cols:
            vals = pd.to_numeric(out[col], errors="coerce")
            out[col] = vals.apply(lambda x: "" if pd.isna(x) else f"¥{x:,.0f}")
        elif col in pct_cols:
            vals = pd.to_numeric(out[col], errors="coerce")
            out[col] = vals.apply(lambda x: "" if pd.isna(x) else f"{x*100:.3f}%")

    if "見送り候補" in out.columns:
        out["見送り候補"] = out["見送り候補"].apply(lambda x: "はい" if bool(x) else "いいえ")

    return out


def make_analysis_excel(overall, daily, etf, monthly, source_df) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        overall.to_excel(writer, sheet_name="全体集計", index=False)
        daily.to_excel(writer, sheet_name="日別集計", index=False)
        etf.to_excel(writer, sheet_name="ETF別集計", index=False)
        monthly.to_excel(writer, sheet_name="月次集計", index=False)
        source_df.to_excel(writer, sheet_name="集計対象データ", index=False)
    output.seek(0)
    return output.getvalue()


def make_csv_download(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


# -----------------------------
# UI
# -----------------------------
st.title("日米時差ETF戦略 / 集計アプリ")
st.caption("Google Sheets の「売買記録台帳」「日次サマリー」を読み込んで集計します")

reload_button = st.button("Google Sheets から再読込", type="primary", use_container_width=False)

for key, default in {
    "trade_df": pd.DataFrame(),
    "daily_signal_df": pd.DataFrame(),
    "overall_df": pd.DataFrame(),
    "daily_df": pd.DataFrame(),
    "etf_df": pd.DataFrame(),
    "monthly_df": pd.DataFrame(),
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

if reload_button or st.session_state["overall_df"].empty:
    try:
        raw_trade_df = read_ws_df("売買記録台帳")
        raw_daily_df = read_ws_df("日次サマリー")

        trade_df = preprocess_trade_log(raw_trade_df)
        daily_signal_df = preprocess_daily_signal_log(raw_daily_df)

        overall = make_overall_summary(trade_df)
        daily = make_daily_summary(trade_df, daily_signal_df)
        etf = make_etf_summary(trade_df)
        monthly = make_monthly_summary(trade_df, daily_signal_df)

        st.session_state["trade_df"] = trade_df
        st.session_state["daily_signal_df"] = daily_signal_df
        st.session_state["overall_df"] = overall
        st.session_state["daily_df"] = daily
        st.session_state["etf_df"] = etf
        st.session_state["monthly_df"] = monthly

        st.success("Google Sheets から読み込みました。")
    except Exception as e:
        st.error(f"読込エラー: {e}")

trade_df = st.session_state["trade_df"]
overall = st.session_state["overall_df"]
daily = st.session_state["daily_df"]
etf = st.session_state["etf_df"]
monthly = st.session_state["monthly_df"]

overall_row = overall.iloc[0] if not overall.empty else pd.Series(dtype=object)
latest_text = datetime.now(JST).strftime("%Y-%m-%d %H:%M")

c1, c2, c3, c4 = st.columns(4)
c1.metric("集計対象件数", int(pd.to_numeric(overall_row.get("対象件数", 0), errors="coerce") or 0))
c2.metric("総損益", f"¥{int(pd.to_numeric(overall_row.get('総損益', 0), errors='coerce') or 0):,}")
c3.metric("勝率", "" if pd.isna(pd.to_numeric(overall_row.get("勝率", np.nan), errors="coerce")) else f"{pd.to_numeric(overall_row.get('勝率', np.nan))*100:.1f}%")
c4.metric("最終更新", latest_text)

tabs = st.tabs(["全体集計", "日別集計", "ETF別集計", "月次集計", "ダウンロード"])

with tabs[0]:
    st.dataframe(format_display_df(overall), use_container_width=True, hide_index=True)

with tabs[1]:
    if daily.empty:
        st.info("日別集計の対象データがありません。")
    else:
        st.dataframe(format_display_df(daily), use_container_width=True, hide_index=True)

with tabs[2]:
    if etf.empty:
        st.info("ETF別集計の対象データがありません。売買記録台帳で『実行有無=〇』かつ『買値・売値・口数』が入った行を確認してください。")
    else:
        st.dataframe(format_display_df(etf), use_container_width=True, hide_index=True)

with tabs[3]:
    if monthly.empty:
        st.info("月次集計の対象データがありません。")
    else:
        st.dataframe(format_display_df(monthly), use_container_width=True, hide_index=True)

with tabs[4]:
    excel_bytes = make_analysis_excel(overall, daily, etf, monthly, trade_df)
    st.download_button(
        "集計Excelダウンロード",
        data=excel_bytes,
        file_name="analysis_result.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    st.download_button(
        "集計対象データCSV",
        data=make_csv_download(trade_df),
        file_name="analysis_source.csv",
        mime="text/csv",
        use_container_width=True,
    )
