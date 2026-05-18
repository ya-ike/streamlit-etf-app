
# -*- coding: utf-8 -*-
"""
app.py
運用アプリ（UI / Google Sheets 保存）
計算は signal_engine.py を使用
"""

from __future__ import annotations

from datetime import datetime, timedelta, time as dtime
from io import BytesIO
from zoneinfo import ZoneInfo
import time

import gspread
import pandas as pd
import streamlit as st

from signal_engine_v3 import (
    now_jst, build_settings_sheet_df, load_excel_settings, jp_settings_to_internal,
    normalize_date_like_text, clean_numeric_series, build_settings_sheet_df,
    download_price_data, run_signal_for_date, apply_same_day_prices
)

JST = ZoneInfo("Asia/Tokyo")
APP_VERSION = "v2026-05-18-shared-02"

DEFAULT_SYSTEM = {
    "selected_mode": "実運用",
    "last_signal_date": "",
    "lock_until": "",
    "last_saved_at": "",
}

TRADE_COLS = [
    "売買日", "日本ETFコード", "日本ETF名", "予定順位", "予定スコア", "予定予算", "1口金額",
    "予定口数", "予定約定金額", "注意フラグ", "実行有無", "買値", "売値", "口数",
    "損益額", "損益率", "入力チェック", "メモ",
]
SHEET_TITLES = ["設定", "当日シグナル", "日次サマリー", "売買記録台帳", "システム"]

st.set_page_config(page_title="日米時差ETF戦略", page_icon="📈", layout="wide")


def parse_dt_or_none(text):
    if not text:
        return None
    t = str(text).strip().replace("/", "-")
    try:
        dt = datetime.fromisoformat(t)
        return dt.replace(tzinfo=JST) if dt.tzinfo is None else dt.astimezone(JST)
    except Exception:
        try:
            dt = pd.to_datetime(t, errors="coerce")
            if pd.isna(dt):
                return None
            dt = pd.Timestamp(dt).to_pydatetime()
            return dt.replace(tzinfo=JST) if dt.tzinfo is None else dt.astimezone(JST)
        except Exception:
            return None


def api_retry(func, *args, **kwargs):
    last_err = None
    for i in range(4):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            last_err = e
            if i == 3:
                break
            time.sleep(1.0 * (2 ** i))
    raise last_err


@st.cache_resource(show_spinner=False)
def get_gspread_client():
    return gspread.service_account_from_dict(dict(st.secrets["gcp_service_account"]))


@st.cache_resource(show_spinner=False)
def open_workbook():
    client = get_gspread_client()
    sheet_name = st.secrets.get("sheets", {}).get("spreadsheet_name", "ETF_運用台帳")
    return client.open(sheet_name)


@st.cache_resource(show_spinner=False)
def get_ws_map():
    book = open_workbook()
    ws_list = api_retry(book.worksheets)
    return {ws.title: ws for ws in ws_list}


def get_or_create_ws(title: str):
    ws_map = get_ws_map()
    if title in ws_map:
        return ws_map[title]
    book = open_workbook()
    ws = api_retry(book.add_worksheet, title=title, rows=300, cols=60)
    ws_map[title] = ws
    return ws


def read_values_df(values):
    if not values:
        return pd.DataFrame()
    headers = values[0]
    rows = values[1:]
    if not headers:
        return pd.DataFrame()
    clean_rows = []
    for r in rows:
        padded = r + [""] * max(0, len(headers) - len(r))
        if any(str(x).strip() != "" for x in padded):
            clean_rows.append(padded[:len(headers)])
    if not clean_rows:
        return pd.DataFrame(columns=headers)
    return pd.DataFrame(clean_rows, columns=headers)


def read_ws_df(title: str) -> pd.DataFrame:
    ws = get_or_create_ws(title)
    values = api_retry(ws.get_all_values)
    return read_values_df(values)


def write_ws_df(title: str, df: pd.DataFrame):
    ws = get_or_create_ws(title)
    api_retry(ws.clear)
    if df is None or df.empty:
        return
    clean = df.copy().fillna("")
    data = [clean.columns.tolist()] + clean.values.tolist()
    api_retry(ws.update, data)


def ensure_base():
    for title in SHEET_TITLES:
        get_or_create_ws(title)

    settings_df = read_ws_df("設定")
    if settings_df.empty or not {"項目", "実運用値", "論文寄り値"}.issubset(settings_df.columns):
        write_ws_df("設定", build_settings_sheet_df())

    system_df = read_ws_df("システム")
    if system_df.empty or not {"key", "value"}.issubset(system_df.columns):
        write_ws_df("システム", pd.DataFrame({"key": list(DEFAULT_SYSTEM.keys()), "value": list(DEFAULT_SYSTEM.values())}))


def load_system_map():
    df = read_ws_df("システム")
    if df.empty or "key" not in df.columns or "value" not in df.columns:
        return DEFAULT_SYSTEM.copy()
    out = {str(r["key"]).strip(): str(r["value"]).strip() for _, r in df.iterrows()}
    for k, v in DEFAULT_SYSTEM.items():
        out.setdefault(k, v)
    return out


def save_system_map(sys_map: dict):
    write_ws_df("システム", pd.DataFrame({"key": list(sys_map.keys()), "value": [sys_map[k] for k in sys_map.keys()]}))


def load_settings_table() -> pd.DataFrame:
    df = read_ws_df("設定")
    if df.empty or not {"項目", "実運用値", "論文寄り値"}.issubset(df.columns):
        out = build_settings_sheet_df()
        write_ws_df("設定", out)
        return out
    return df


def settings_table_to_map(settings_df: pd.DataFrame, mode: str) -> dict:
    mode_col = "論文寄り値" if mode == "論文寄り" else "実運用値"
    out = {}
    for _, r in settings_df.iterrows():
        item = str(r.get("項目", "")).strip()
        if item:
            out[item] = str(r.get(mode_col, "")).strip()
    out["モード"] = mode
    return out


def load_saved_state():
    sys_map = load_system_map()
    signal_date = normalize_date_like_text(sys_map.get("last_signal_date", ""))
    signal_df = read_ws_df("当日シグナル")
    daily_all = read_ws_df("日次サマリー")
    trade_all = read_ws_df("売買記録台帳")
    if signal_date and not daily_all.empty and "シグナル日付" in daily_all.columns:
        daily_df = daily_all[daily_all["シグナル日付"].astype(str).apply(normalize_date_like_text) == signal_date].copy()
        if daily_df.empty:
            daily_df = daily_all.tail(1).copy()
    else:
        daily_df = daily_all.tail(1).copy() if not daily_all.empty else pd.DataFrame()
    if signal_date and not trade_all.empty and "売買日" in trade_all.columns:
        trade_df = trade_all[trade_all["売買日"].astype(str).apply(normalize_date_like_text) == signal_date].copy()
    else:
        trade_df = pd.DataFrame(columns=TRADE_COLS)
    return signal_df, daily_df, trade_df, sys_map


def merge_trade_ledger(base_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if base_df.empty:
        return new_df.copy()
    base = base_df.copy()
    new = new_df.copy()
    base["_k"] = base["売買日"].astype(str).apply(normalize_date_like_text) + "|" + base["日本ETFコード"].astype(str).str.strip()
    new["_k"] = new["売買日"].astype(str).apply(normalize_date_like_text) + "|" + new["日本ETFコード"].astype(str).str.strip()
    merged = pd.concat([base, new], ignore_index=True).drop_duplicates(subset=["_k"], keep="last")
    merged = merged.drop(columns=["_k"], errors="ignore")
    merged["_sort"] = pd.to_datetime(merged["売買日"].astype(str).str.replace("/", "-", regex=False), errors="coerce")
    merged = merged.sort_values(["_sort", "予定順位", "日本ETFコード"], na_position="last").drop(columns=["_sort"], errors="ignore")
    return merged.reset_index(drop=True)


def save_signal_bundle(signal_df: pd.DataFrame, daily_df: pd.DataFrame, trade_df: pd.DataFrame, mode: str, signal_date: str):
    write_ws_df("当日シグナル", signal_df)
    write_ws_df("日次サマリー", daily_df)
    ledger_df = read_ws_df("売買記録台帳")
    merged = merge_trade_ledger(ledger_df, trade_df)
    write_ws_df("売買記録台帳", merged)

    lock_until = datetime.combine((now_jst() + timedelta(days=1)).date(), dtime(6, 0), tzinfo=JST)
    sys_map = load_system_map()
    sys_map["selected_mode"] = mode
    sys_map["last_signal_date"] = signal_date
    sys_map["lock_until"] = lock_until.isoformat(timespec="minutes")
    sys_map["last_saved_at"] = now_jst().strftime("%Y-%m-%d %H:%M:%S")
    save_system_map(sys_map)


def is_locked(sys_map: dict):
    last_signal_date = normalize_date_like_text(sys_map.get("last_signal_date", ""))
    lock_until = parse_dt_or_none(sys_map.get("lock_until", ""))
    if lock_until and now_jst() < lock_until:
        return True, f"前回確定日: {last_signal_date or '未設定'} / 再計算ロック: {lock_until.strftime('%Y-%m-%d %H:%M')} JST まで"
    return False, f"前回確定日: {last_signal_date or '未設定'} / 再計算ロックなし"


def format_display_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["スコア", "予定スコア", "1位スコア", "1位-4位差"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(4)
    for col in ["推奨予算", "推定価格", "1口金額", "推奨約定金額", "予定予算", "予定約定金額", "買値", "売値", "損益額"]:
        if col in out.columns:
            vals = pd.to_numeric(out[col], errors="coerce")
            out[col] = vals.apply(lambda x: "" if pd.isna(x) else f"¥{x:,.0f}")
    if "損益率" in out.columns:
        vals = pd.to_numeric(out["損益率"], errors="coerce")
        out["損益率"] = vals.apply(lambda x: "" if pd.isna(x) else f"{x * 100:.2f}%")
    return out


def make_excel_download(signal_df, daily_df, trade_df) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        signal_df.to_excel(writer, sheet_name="予測記録", index=False)
        daily_df.to_excel(writer, sheet_name="日次サマリー", index=False)
        trade_df.to_excel(writer, sheet_name="売買記録", index=False)
    output.seek(0)
    return output.getvalue()


st.title("日米時差ETF戦略 / Google Sheets 保存版")
st.caption(f"計算は signal_engine.py を使用。朝一計算、午後は当日の午前寄り値 / 午後引け値を記入します。 / {APP_VERSION}")

ensure_base()
system_map = load_system_map()
settings_df = load_settings_table()

current_mode = system_map.get("selected_mode", "実運用")
if current_mode not in ["実運用", "論文寄り"]:
    current_mode = "実運用"

with st.sidebar:
    selected_mode = st.selectbox("計算モード", ["実運用", "論文寄り"], index=0 if current_mode == "実運用" else 1)
    if st.button("モードを保存", use_container_width=True):
        system_map["selected_mode"] = selected_mode
        save_system_map(system_map)
        st.success(f"モードを {selected_mode} に保存しました。")
        st.rerun()
    reload_button = st.button("保存済みデータを再読込", use_container_width=True)
    run_button = st.button("朝の確定計算を実行", type="primary", use_container_width=True)

settings_map = settings_table_to_map(settings_df, selected_mode)
settings = jp_settings_to_internal(settings_map, {"投資額モード": "総額指定", "総投資額": settings_map.get("総投資額", "600000"), "1銘柄投資額": "200000"})

for key, default in {
    "signal_df": pd.DataFrame(),
    "daily_df": pd.DataFrame(),
    "trade_df": pd.DataFrame(columns=TRADE_COLS),
    "system_map": system_map,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

if reload_button or st.session_state["signal_df"].empty:
    signal_df, daily_df, trade_df, sys_map = load_saved_state()
    st.session_state["signal_df"] = signal_df
    st.session_state["daily_df"] = daily_df
    st.session_state["trade_df"] = trade_df
    st.session_state["system_map"] = sys_map

system_map = st.session_state["system_map"]
locked, lock_text = is_locked(system_map)
st.info(lock_text)

if run_button:
    if locked:
        st.warning("今回は再計算しません。保存済みデータをそのまま使ってください。")
    else:
        try:
            with st.spinner("朝の確定計算を実行中..."):
                close_df, open_df, volume_df = download_price_data(period="2y")
                signal_date = open_df.index.max()
                signal_df, daily_df, trade_df = run_signal_for_date(signal_date, close_df, open_df, volume_df, settings)
                signal_date_text = str(daily_df.iloc[0]["シグナル日付"])
                save_signal_bundle(signal_df, daily_df, trade_df, selected_mode, signal_date_text)
                signal_df, daily_df, trade_df, sys_map = load_saved_state()
            st.session_state["signal_df"] = signal_df
            st.session_state["daily_df"] = daily_df
            st.session_state["trade_df"] = trade_df
            st.session_state["system_map"] = sys_map
            st.success("朝の確定計算を保存しました。")
            st.rerun()
        except Exception as e:
            st.error(f"朝の確定計算エラー: {e}")

signal_df = st.session_state["signal_df"]
daily_df = st.session_state["daily_df"]
trade_df = st.session_state["trade_df"]

if not signal_df.empty:
    c1, c2 = st.columns(2)
    with c1:
        if st.button("当日の午前寄り値 / 午後引け値を反映", use_container_width=True):
            try:
                close_df, open_df, _ = download_price_data(period="2y")
                signal_date = pd.Timestamp(str(daily_df.iloc[0]["シグナル日付"]))
                priced = apply_same_day_prices(trade_df, signal_date, open_df, close_df)
                ledger = read_ws_df("売買記録台帳")
                merged = merge_trade_ledger(ledger, priced)
                write_ws_df("売買記録台帳", merged)
                st.session_state["trade_df"] = priced
                st.success("当日の午前寄り値 / 午後引け値を反映しました。")
                st.rerun()
            except Exception as e:
                st.error(f"価格反映エラー: {e}")
    with c2:
        st.write(f"現在モード: **{selected_mode}**")

    tabs = st.tabs(["日次サマリー", "候補一覧", "売買記録", "ダウンロード"])
    with tabs[0]:
        st.dataframe(format_display_df(daily_df), use_container_width=True, hide_index=True)
    with tabs[1]:
        st.dataframe(format_display_df(signal_df), use_container_width=True, hide_index=True)
    with tabs[2]:
        st.dataframe(format_display_df(trade_df), use_container_width=True, hide_index=True)
    with tabs[3]:
        st.download_button("当日Excelダウンロード", data=make_excel_download(signal_df, daily_df, trade_df), file_name="etf_signal.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
else:
    st.info("朝の確定計算を実行してください。")
