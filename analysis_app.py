# -*- coding: utf-8 -*-
"""
Google Sheets 保存版用 集計アプリ
- ETF_運用台帳 の「売買記録台帳」「日次サマリー」を読み込む
- 全体集計 / 日別集計 / ETF別集計 / 月次集計 を表示
- CSV / Excel ダウンロード対応
"""
from __future__ import annotations

from io import BytesIO
from zoneinfo import ZoneInfo
from datetime import datetime

import gspread
import numpy as np
import pandas as pd
import streamlit as st

JST = ZoneInfo("Asia/Tokyo")

st.set_page_config(page_title="日米時差ETF戦略 / 集計", page_icon="📊", layout="wide")

TRADE_COLS = [
    "売買日", "日本ETFコード", "日本ETF名", "予定順位", "予定スコア", "予定予算", "1口金額",
    "予定口数", "予定約定金額", "注意フラグ", "実行有無", "買値", "売値", "口数",
    "損益額", "損益率", "入力チェック", "メモ",
]


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
    clean_rows = [r + [""] * max(0, len(headers) - len(r)) for r in rows]
    return pd.DataFrame(clean_rows, columns=headers)


def preprocess_trade_log(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    # 空行除去
    if "日本ETFコード" in df.columns:
        df = df[df["日本ETFコード"].astype(str).str.strip() != ""].copy()

    if "実行有無" in df.columns:
        df = df[df["実行有無"].astype(str) == "〇"].copy()

    if df.empty:
        return df

    df["売買日"] = pd.to_datetime(df["売買日"], errors="coerce")

    num_cols = [
        "予定順位", "予定スコア", "予定予算", "1口金額", "予定口数",
        "予定約定金額", "買値", "売値", "口数", "損益額", "損益率"
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["売買日", "買値", "売値", "口数"])
    if df.empty:
        return df

    df["損益額_再計算"] = (df["売値"] - df["買値"]) * df["口数"]
    df["損益率_再計算"] = np.where(
        df["買値"] != 0,
        (df["売値"] - df["買値"]) / df["買値"],
        np.nan,
    )
    df["年月"] = df["売買日"].dt.strftime("%Y-%m")
    return df


def preprocess_daily_signal_log(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    if "シグナル日付" in df.columns:
        df["シグナル日付"] = pd.to_datetime(df["シグナル日付"], errors="coerce")

    num_cols = ["PCA主成分数", "1位スコア", "1位-4位差", "フィルタ通過本数", "最終採用本数"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "シグナル日付" in df.columns:
        df["年月"] = df["シグナル日付"].dt.strftime("%Y-%m")
    return df


def make_overall_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame([{
            "対象件数": 0,
            "勝ち数": 0,
            "負け数": 0,
            "勝率": np.nan,
            "総損益": 0,
            "平均損益": np.nan,
            "平均損益率": np.nan,
        }])

    win_count = (df["損益額_再計算"] > 0).sum()
    lose_count = (df["損益額_再計算"] < 0).sum()
    total_count = len(df)
    return pd.DataFrame([{
        "対象件数": total_count,
        "勝ち数": int(win_count),
        "負け数": int(lose_count),
        "勝率": win_count / total_count if total_count > 0 else np.nan,
        "総損益": df["損益額_再計算"].sum(),
        "平均損益": df["損益額_再計算"].mean(),
        "平均損益率": df["損益率_再計算"].mean(),
    }])


def make_daily_summary(trade_df: pd.DataFrame, daily_signal_df: pd.DataFrame) -> pd.DataFrame:
    if trade_df.empty:
        exec_daily = pd.DataFrame(columns=[
            "売買日", "実行本数", "日次総損益", "日次平均損益", "日次平均損益率", "勝ち数", "負け数"
        ])
    else:
        exec_daily = (
            trade_df.groupby("売買日", as_index=False)
            .agg(
                実行本数=("日本ETFコード", "count"),
                日次総損益=("損益額_再計算", "sum"),
                日次平均損益=("損益額_再計算", "mean"),
                日次平均損益率=("損益率_再計算", "mean"),
                勝ち数=("損益額_再計算", lambda x: (x > 0).sum()),
                負け数=("損益額_再計算", lambda x: (x < 0).sum()),
            )
            .sort_values("売買日")
            .reset_index(drop=True)
        )
        exec_daily["日次勝率"] = np.where(
            exec_daily["実行本数"] > 0,
            exec_daily["勝ち数"] / exec_daily["実行本数"],
            np.nan,
        )

    if daily_signal_df.empty:
        signal_daily = pd.DataFrame(columns=["シグナル日付", "採用本数", "フィルタ通過本数", "見送り候補", "見送り理由"])
    else:
        use_cols = [c for c in ["シグナル日付", "最終採用本数", "フィルタ通過本数", "見送り候補", "見送り理由"] if c in daily_signal_df.columns]
        signal_daily = daily_signal_df[use_cols].copy()
        if "最終採用本数" in signal_daily.columns:
            signal_daily = signal_daily.rename(columns={"最終採用本数": "採用本数"})

    daily = pd.merge(signal_daily, exec_daily, left_on="シグナル日付", right_on="売買日", how="outer")
    daily["日付"] = daily.get("シグナル日付", pd.Series(dtype="datetime64[ns]")).combine_first(daily.get("売買日", pd.Series(dtype="datetime64[ns]")))
    daily = daily.drop(columns=["シグナル日付", "売買日"], errors="ignore")

    for col in ["採用本数", "フィルタ通過本数", "実行本数", "勝ち数", "負け数"]:
        if col in daily.columns:
            daily[col] = pd.to_numeric(daily[col], errors="coerce").fillna(0)

    if "採用本数" in daily.columns and "実行本数" in daily.columns:
        daily["見送り率"] = np.where(
            daily["採用本数"] > 0,
            1 - (daily["実行本数"] / daily["採用本数"]),
            np.nan,
        )

    if "日次総損益" in daily.columns:
        daily["日次総損益"] = pd.to_numeric(daily["日次総損益"], errors="coerce").fillna(0)
        daily["累積損益"] = daily["日次総損益"].cumsum()

    daily = daily.sort_values("日付").reset_index(drop=True)
    cols = [
        "日付", "見送り候補", "見送り理由", "フィルタ通過本数", "採用本数", "実行本数", "見送り率",
        "勝ち数", "負け数", "日次勝率", "日次総損益", "日次平均損益", "日次平均損益率", "累積損益",
    ]
    return daily[[c for c in cols if c in daily.columns]]


def make_etf_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    etf = (
        df.groupby(["日本ETFコード", "日本ETF名"], as_index=False)
        .agg(
            件数=("売買日", "count"),
            総損益=("損益額_再計算", "sum"),
            平均損益=("損益額_再計算", "mean"),
            平均損益率=("損益率_再計算", "mean"),
            勝ち数=("損益額_再計算", lambda x: (x > 0).sum()),
            負け数=("損益額_再計算", lambda x: (x < 0).sum()),
        )
        .reset_index(drop=True)
    )
    etf["勝率"] = np.where(etf["件数"] > 0, etf["勝ち数"] / etf["件数"], np.nan)
    return etf.sort_values(["総損益", "勝率"], ascending=[False, False]).reset_index(drop=True)


def make_monthly_summary(trade_df: pd.DataFrame, daily_signal_df: pd.DataFrame) -> pd.DataFrame:
    if trade_df.empty:
        exec_monthly = pd.DataFrame(columns=[
            "年月", "実行本数", "月次総損益", "月次平均損益", "月次平均損益率", "勝ち数", "負け数"
        ])
    else:
        exec_monthly = (
            trade_df.groupby("年月", as_index=False)
            .agg(
                実行本数=("日本ETFコード", "count"),
                月次総損益=("損益額_再計算", "sum"),
                月次平均損益=("損益額_再計算", "mean"),
                月次平均損益率=("損益率_再計算", "mean"),
                勝ち数=("損益額_再計算", lambda x: (x > 0).sum()),
                負け数=("損益額_再計算", lambda x: (x < 0).sum()),
            )
            .sort_values("年月")
            .reset_index(drop=True)
        )
        exec_monthly["月次勝率"] = np.where(
            exec_monthly["実行本数"] > 0,
            exec_monthly["勝ち数"] / exec_monthly["実行本数"],
            np.nan,
        )

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
            monthly[col] = pd.to_numeric(monthly[col], errors="coerce").fillna(0)

    if "採用本数" in monthly.columns and "実行本数" in monthly.columns:
        monthly["見送り率"] = np.where(
            monthly["採用本数"] > 0,
            1 - (monthly["実行本数"] / monthly["採用本数"]),
            np.nan,
        )
    if "月次総損益" in monthly.columns:
        monthly["月次総損益"] = pd.to_numeric(monthly["月次総損益"], errors="coerce").fillna(0)
        monthly["累積損益"] = monthly["月次総損益"].cumsum()

    monthly = monthly.sort_values("年月").reset_index(drop=True)
    cols = [
        "年月", "フィルタ通過本数", "採用本数", "実行本数", "見送り率", "勝ち数", "負け数",
        "月次勝率", "月次総損益", "月次平均損益", "月次平均損益率", "累積損益",
    ]
    return monthly[[c for c in cols if c in monthly.columns]]


def to_excel_bytes(overall: pd.DataFrame, daily: pd.DataFrame, etf: pd.DataFrame, monthly: pd.DataFrame, source_df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        overall.to_excel(writer, sheet_name="全体集計", index=False)
        daily.to_excel(writer, sheet_name="日別集計", index=False)
        etf.to_excel(writer, sheet_name="ETF別集計", index=False)
        monthly.to_excel(writer, sheet_name="月次集計", index=False)
        source_df.to_excel(writer, sheet_name="集計対象データ", index=False)
    output.seek(0)
    return output.getvalue()


def format_display(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    yen_cols = [c for c in ["総損益", "平均損益", "日次総損益", "日次平均損益", "月次総損益", "月次平均損益", "累積損益", "損益額_再計算"] if c in out.columns]
    pct_cols = [c for c in ["勝率", "平均損益率", "日次平均損益率", "日次勝率", "月次平均損益率", "月次勝率", "損益率_再計算", "見送り率"] if c in out.columns]
    for col in yen_cols:
        vals = pd.to_numeric(out[col], errors="coerce")
        out[col] = vals.apply(lambda x: "" if pd.isna(x) else f"¥{x:,.0f}")
    for col in pct_cols:
        vals = pd.to_numeric(out[col], errors="coerce")
        out[col] = vals.apply(lambda x: "" if pd.isna(x) else f"{x * 100:.3f}%")
    return out


st.title("日米時差ETF戦略 / 集計アプリ")
st.caption("Google Sheets の『売買記録台帳』『日次サマリー』を読み込んで集計します")

left, right = st.columns([1, 2])
with left:
    reload_button = st.button("Google Sheets から再読込", type="primary", use_container_width=True)
with right:
    st.write("")

if reload_button or "loaded" not in st.session_state:
    try:
        trade_raw = read_ws_df("売買記録台帳")
        daily_raw = read_ws_df("日次サマリー")
        st.session_state.trade_raw = trade_raw
        st.session_state.daily_raw = daily_raw
        st.session_state.loaded = True
        st.success("Google Sheets から読み込みました。")
    except Exception as e:
        st.error(f"読込エラー: {e}")

trade_raw = st.session_state.get("trade_raw", pd.DataFrame())
daily_raw = st.session_state.get("daily_raw", pd.DataFrame())

trade_df = preprocess_trade_log(trade_raw)
daily_signal_df = preprocess_daily_signal_log(daily_raw)

overall = make_overall_summary(trade_df)
daily = make_daily_summary(trade_df, daily_signal_df)
etf = make_etf_summary(trade_df)
monthly = make_monthly_summary(trade_df, daily_signal_df)

m1, m2, m3, m4 = st.columns(4)
m1.metric("集計対象件数", int(len(trade_df)))
m2.metric("総損益", "" if overall.empty else format_display(overall).iloc[0].get("総損益", ""))
m3.metric("勝率", "" if overall.empty else format_display(overall).iloc[0].get("勝率", ""))
m4.metric("最終更新", datetime.now(JST).strftime("%Y-%m-%d %H:%M JST"))

tab1, tab2, tab3, tab4, tab5 = st.tabs(["全体集計", "日別集計", "ETF別集計", "月次集計", "ダウンロード"])

with tab1:
    st.dataframe(format_display(overall), use_container_width=True, hide_index=True)

with tab2:
    st.dataframe(format_display(daily), use_container_width=True, hide_index=True)

with tab3:
    st.dataframe(format_display(etf), use_container_width=True, hide_index=True)

with tab4:
    st.dataframe(format_display(monthly), use_container_width=True, hide_index=True)

with tab5:
    excel_bytes = to_excel_bytes(overall, daily, etf, monthly, trade_df)
    st.download_button(
        "集計Excelダウンロード",
        data=excel_bytes,
        file_name=f"analysis_result_{datetime.now(JST).strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    st.download_button(
        "集計対象データCSV",
        data=trade_df.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"analysis_source_{datetime.now(JST).strftime('%Y%m%d')}.csv",
        mime="text/csv",
        use_container_width=True,
    )
