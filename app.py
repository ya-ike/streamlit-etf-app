# -*- coding: utf-8 -*-
"""
app.py
日米時差ETF戦略 / Google Sheets 保存版
設定シート・システムシート対応
"""

from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
from zoneinfo import ZoneInfo

import gspread
import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

JST = ZoneInfo("Asia/Tokyo")

st.set_page_config(page_title="日米時差ETF戦略", page_icon="📈", layout="wide")

US_ETFS = {
    "XLB": "Materials",
    "XLE": "Energy",
    "XLF": "Financials",
    "XLI": "Industrials",
    "XLK": "Technology",
    "XLP": "Consumer Staples",
    "XLU": "Utilities",
    "XLV": "Health Care",
    "XLY": "Consumer Discretionary",
    "XLC": "Communication Services",
    "XLRE": "Real Estate",
}

JP_ETFS = {
    "1617.T": "TOPIX-17 食品",
    "1618.T": "TOPIX-17 エネルギー資源",
    "1619.T": "TOPIX-17 建設・資材",
    "1620.T": "TOPIX-17 素材・化学",
    "1621.T": "TOPIX-17 医薬品",
    "1622.T": "TOPIX-17 自動車・輸送機",
    "1623.T": "TOPIX-17 鉄鋼・非鉄",
    "1624.T": "TOPIX-17 機械",
    "1625.T": "TOPIX-17 電機・精密",
    "1626.T": "TOPIX-17 情報通信・サービスその他",
    "1627.T": "TOPIX-17 電力・ガス",
    "1628.T": "TOPIX-17 運輸・物流",
    "1629.T": "TOPIX-17 商社・卸売",
    "1630.T": "TOPIX-17 小売",
    "1631.T": "TOPIX-17 銀行",
    "1632.T": "TOPIX-17 金融（除く銀行）",
    "1633.T": "TOPIX-17 不動産",
}
ALL_TICKERS = list(US_ETFS.keys()) + list(JP_ETFS.keys())

DEFAULT_SETTINGS_JP = {
    "モード": "実運用",
    "総投資額": "600000",
    "最大採用本数": "3",
    "ローリング窓": "60",
    "最低必要履歴数": "40",
    "PCA主成分数": "3",
    "リッジ係数": "0.000001",
    "MIN_SCORE_SPREAD": "0.0010",
    "1位スコア<=0で見送り候補": "TRUE",
    "スコア>0のみ採用": "TRUE",
    "出来高フィルタを使う": "FALSE",
    "最小推奨口数": "1",
    "数量計算安全係数": "1.02",
    "最低平均出来高": "1000",
    "低単価しきい値": "1000",
    "口数多めしきい値": "100",
}
DEFAULT_SYSTEM_JP = {
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


def now_jst() -> datetime:
    return datetime.now(JST)


def now_text() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")


def to_date_str(ts) -> str:
    if pd.isna(ts):
        return ""
    return pd.Timestamp(ts).strftime("%Y-%m-%d")


def parse_bool_jp(val, default=False) -> bool:
    s = str(val).strip().lower()
    if s in ["true", "1", "yes", "y", "on", "はい"]:
        return True
    if s in ["false", "0", "no", "n", "off", "いいえ"]:
        return False
    return default


def normalize_date_like_text(val) -> str:
    s = str(val).strip()
    if s == "" or s.lower() in ["none", "nan", "nat"]:
        return ""
    s = s.replace("/", "-")
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return s
        return pd.Timestamp(dt).strftime("%Y-%m-%d")
    except Exception:
        return s


def parse_dt_or_none(text: str | None):
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


def clean_numeric_series(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    s = (
        s.str.replace("¥", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    s = s.replace({"": np.nan, "None": np.nan, "nan": np.nan, "NaN": np.nan})
    return pd.to_numeric(s, errors="coerce")


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
        return book.add_worksheet(title=title, rows=300, cols=60)


def read_ws_df(title: str) -> pd.DataFrame:
    ws = get_or_create_ws(open_workbook(), title)
    values = ws.get_all_values()
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


def write_ws_df(title: str, df: pd.DataFrame):
    ws = get_or_create_ws(open_workbook(), title)
    ws.clear()
    if df is None or df.empty:
        return
    clean = df.copy().fillna("")
    data = [clean.columns.tolist()] + clean.values.tolist()
    ws.update(data)


def ensure_base_sheets_and_defaults():
    book = open_workbook()
    for title in SHEET_TITLES:
        get_or_create_ws(book, title)

    settings_df = read_ws_df("設定")
    if settings_df.empty or "項目" not in settings_df.columns or "値" not in settings_df.columns:
        df = pd.DataFrame({"項目": list(DEFAULT_SETTINGS_JP.keys()), "値": list(DEFAULT_SETTINGS_JP.values())})
        write_ws_df("設定", df)
    else:
        current = {str(r["項目"]).strip(): str(r["値"]).strip() for _, r in settings_df.iterrows()}
        changed = False
        for k, v in DEFAULT_SETTINGS_JP.items():
            if k not in current:
                current[k] = v
                changed = True
        if changed:
            df = pd.DataFrame({"項目": list(current.keys()), "値": list(current.values())})
            write_ws_df("設定", df)

    system_df = read_ws_df("システム")
    if system_df.empty or "key" not in system_df.columns or "value" not in system_df.columns:
        df = pd.DataFrame({"key": list(DEFAULT_SYSTEM_JP.keys()), "value": list(DEFAULT_SYSTEM_JP.values())})
        write_ws_df("システム", df)
    else:
        current = {str(r["key"]).strip(): str(r["value"]).strip() for _, r in system_df.iterrows()}
        changed = False
        for k, v in DEFAULT_SYSTEM_JP.items():
            if k not in current:
                current[k] = v
                changed = True
        if changed:
            df = pd.DataFrame({"key": list(current.keys()), "value": list(current.values())})
            write_ws_df("システム", df)


def load_settings_map() -> dict:
    ensure_base_sheets_and_defaults()
    df = read_ws_df("設定")
    if df.empty:
        return DEFAULT_SETTINGS_JP.copy()
    out = {str(r["項目"]).strip(): str(r["値"]).strip() for _, r in df.iterrows()}
    for k, v in DEFAULT_SETTINGS_JP.items():
        out.setdefault(k, v)
    return out


def save_settings_map(settings_map: dict):
    df = pd.DataFrame({"項目": list(settings_map.keys()), "値": [settings_map[k] for k in settings_map.keys()]})
    write_ws_df("設定", df)


def load_system_map() -> dict:
    ensure_base_sheets_and_defaults()
    df = read_ws_df("システム")
    if df.empty:
        return DEFAULT_SYSTEM_JP.copy()
    out = {str(r["key"]).strip(): str(r["value"]).strip() for _, r in df.iterrows()}
    for k, v in DEFAULT_SYSTEM_JP.items():
        out.setdefault(k, v)
    return out


def save_system_map(sys_map: dict):
    df = pd.DataFrame({"key": list(sys_map.keys()), "value": [sys_map[k] for k in sys_map.keys()]})
    write_ws_df("システム", df)


def jp_settings_to_internal(settings_map: dict) -> dict:
    mode = settings_map.get("モード", "実運用").strip() or "実運用"
    internal = {
        "mode": mode,
        "total_budget": int(float(settings_map.get("総投資額", DEFAULT_SETTINGS_JP["総投資額"]))),
        "top_n": int(float(settings_map.get("最大採用本数", DEFAULT_SETTINGS_JP["最大採用本数"]))),
        "rolling_window": int(float(settings_map.get("ローリング窓", DEFAULT_SETTINGS_JP["ローリング窓"]))),
        "min_history": int(float(settings_map.get("最低必要履歴数", DEFAULT_SETTINGS_JP["最低必要履歴数"]))),
        "pca_components": int(float(settings_map.get("PCA主成分数", DEFAULT_SETTINGS_JP["PCA主成分数"]))),
        "ridge_alpha": float(settings_map.get("リッジ係数", DEFAULT_SETTINGS_JP["リッジ係数"])),
        "min_score_spread": float(settings_map.get("MIN_SCORE_SPREAD", DEFAULT_SETTINGS_JP["MIN_SCORE_SPREAD"])),
        "skip_if_top1_leq_zero": parse_bool_jp(settings_map.get("1位スコア<=0で見送り候補", DEFAULT_SETTINGS_JP["1位スコア<=0で見送り候補"]), True),
        "require_positive_score": parse_bool_jp(settings_map.get("スコア>0のみ採用", DEFAULT_SETTINGS_JP["スコア>0のみ採用"]), True),
        "use_volume_filter": parse_bool_jp(settings_map.get("出来高フィルタを使う", DEFAULT_SETTINGS_JP["出来高フィルタを使う"]), False),
        "min_suggested_qty": int(float(settings_map.get("最小推奨口数", DEFAULT_SETTINGS_JP["最小推奨口数"]))),
        "min_price_buffer": float(settings_map.get("数量計算安全係数", DEFAULT_SETTINGS_JP["数量計算安全係数"])),
        "min_avg_volume": int(float(settings_map.get("最低平均出来高", DEFAULT_SETTINGS_JP["最低平均出来高"]))),
        "low_price_threshold": float(settings_map.get("低単価しきい値", DEFAULT_SETTINGS_JP["低単価しきい値"])),
        "high_qty_threshold": float(settings_map.get("口数多めしきい値", DEFAULT_SETTINGS_JP["口数多めしきい値"])),
    }
    if mode == "論文寄り":
        internal["use_volume_filter"] = False
        internal["require_positive_score"] = False
        internal["skip_if_top1_leq_zero"] = False
        internal["min_score_spread"] = 0.0
    return internal


@st.cache_data(ttl=3600, show_spinner=False)
def download_price_data(period: str = "2y"):
    data = yf.download(
        tickers=ALL_TICKERS,
        period=period,
        interval="1d",
        auto_adjust=False,
        group_by="ticker",
        progress=False,
        threads=True,
    )
    close_df = pd.DataFrame()
    open_df = pd.DataFrame()
    volume_df = pd.DataFrame()
    if isinstance(data.columns, pd.MultiIndex):
        for ticker in ALL_TICKERS:
            if ticker not in data.columns.get_level_values(0):
                continue
            sub = data[ticker].copy()
            if "Close" in sub.columns:
                close_df[ticker] = sub["Close"]
            if "Open" in sub.columns:
                open_df[ticker] = sub["Open"]
            if "Volume" in sub.columns:
                volume_df[ticker] = sub["Volume"]
    else:
        t0 = ALL_TICKERS[0]
        if "Close" in data.columns:
            close_df[t0] = data["Close"]
        if "Open" in data.columns:
            open_df[t0] = data["Open"]
        if "Volume" in data.columns:
            volume_df[t0] = data["Volume"]
    return close_df.sort_index(), open_df.sort_index(), volume_df.sort_index()


def calc_us_close_to_close_returns(close_df: pd.DataFrame) -> pd.DataFrame:
    return close_df[list(US_ETFS.keys())].copy().pct_change()


def calc_jp_open_to_close_returns(open_df: pd.DataFrame, close_df: pd.DataFrame) -> pd.DataFrame:
    jp_open = open_df[list(JP_ETFS.keys())].copy()
    jp_close = close_df[list(JP_ETFS.keys())].copy()
    return (jp_close / jp_open) - 1.0


def map_jp_date_to_prev_us_date(jp_dates, us_dates):
    us_dates = pd.DatetimeIndex(us_dates).sort_values()
    mapping = {}
    for jp_date in pd.DatetimeIndex(jp_dates).sort_values():
        prev_us = us_dates[us_dates < jp_date]
        mapping[jp_date] = prev_us[-1] if len(prev_us) > 0 else pd.NaT
    return pd.Series(mapping)


def align_us_to_jp(us_ret: pd.DataFrame, jp_ret: pd.DataFrame):
    mapping = map_jp_date_to_prev_us_date(jp_ret.index, us_ret.index)
    aligned_rows = []
    aligned_index = []
    for jp_date, us_date in mapping.items():
        if pd.isna(us_date) or us_date not in us_ret.index:
            continue
        aligned_rows.append(us_ret.loc[us_date].values)
        aligned_index.append(jp_date)
    aligned_us = pd.DataFrame(aligned_rows, index=pd.DatetimeIndex(aligned_index), columns=us_ret.columns)
    common_index = aligned_us.index.intersection(jp_ret.index)
    return aligned_us.loc[common_index].sort_index(), jp_ret.loc[common_index].sort_index()


def get_latest_mapping_info(aligned_us: pd.DataFrame, aligned_jp: pd.DataFrame):
    if aligned_us.empty or aligned_jp.empty:
        return "", ""
    return pd.Timestamp(aligned_us.index[-1]).strftime("%Y-%m-%d"), pd.Timestamp(aligned_jp.index[-1]).strftime("%Y-%m-%d")


def compute_scores(aligned_us: pd.DataFrame, aligned_jp: pd.DataFrame, settings: dict) -> pd.DataFrame:
    if len(aligned_us) < settings["min_history"] or len(aligned_jp) < settings["min_history"]:
        raise ValueError(f"履歴不足です。aligned_us={len(aligned_us)}, aligned_jp={len(aligned_jp)}")
    use_us = aligned_us.iloc[-settings["rolling_window"]:].copy()
    use_jp = aligned_jp.iloc[-settings["rolling_window"]:].copy()
    us_mean = use_us.mean(axis=0)
    us_std = use_us.std(axis=0, ddof=0).replace(0, np.nan)
    us_z = ((use_us - us_mean) / us_std).fillna(0.0)
    x_full = us_z.copy()
    t_full, n_assets = x_full.shape
    k = min(settings["pca_components"], n_assets, t_full)
    cov = np.cov(x_full.values, rowvar=False)
    eigvals, eigvecs = np.linalg.eigh(cov)
    order = np.argsort(eigvals)[::-1]
    eigvecs = eigvecs[:, order]
    v = eigvecs[:, :k]
    f_full = x_full.values @ v
    latest_factor = x_full.iloc[-1].values.reshape(1, -1) @ v
    scores = {}
    valid_counts = {}
    for jp_code in use_jp.columns:
        y_full = use_jp[jp_code].copy()
        valid_mask = y_full.notna().values
        xreg_base = f_full[valid_mask]
        y = y_full[valid_mask].values.reshape(-1, 1)
        valid_counts[jp_code] = int(len(y))
        if len(y) < max(10, k + 2):
            scores[jp_code] = 0.0
            continue
        xreg = np.column_stack([np.ones(len(y)), xreg_base])
        try:
            xtx = xreg.T @ xreg
            reg = settings["ridge_alpha"] * np.eye(xtx.shape[0])
            reg[0, 0] = 0.0
            beta = np.linalg.solve(xtx + reg, xreg.T @ y)
            latest_x = np.column_stack([np.ones(1), latest_factor])
            pred = float((latest_x @ beta).ravel()[0])
            scores[jp_code] = 0.0 if np.isnan(pred) or np.isinf(pred) else pred
        except Exception:
            scores[jp_code] = 0.0
    score_df = pd.DataFrame({"jp_code": list(scores.keys()), "score": list(scores.values())})
    score_df["valid_train_count"] = score_df["jp_code"].map(valid_counts)
    score_df["jp_name"] = score_df["jp_code"].map(JP_ETFS)
    score_df = score_df.sort_values("score", ascending=False).reset_index(drop=True)
    score_df["rank"] = np.arange(1, len(score_df) + 1)
    score_df["selected"] = score_df["rank"] <= settings["top_n"]
    return score_df


def add_skip_flags(score_df: pd.DataFrame, settings: dict) -> pd.DataFrame:
    out = score_df.copy()
    if out.empty:
        out["skip_candidate"] = False
        out["skip_reason"] = ""
        out["top1_score"] = np.nan
        out["spread_1_4"] = np.nan
        return out
    top1_score = float(out.iloc[0]["score"])
    top4_score = float(out.iloc[3]["score"]) if len(out) >= 4 else float(out.iloc[-1]["score"])
    spread_1_4 = top1_score - top4_score
    reasons = []
    if settings["skip_if_top1_leq_zero"] and top1_score <= 0:
        reasons.append("1位スコア<=0")
    if spread_1_4 < settings["min_score_spread"]:
        reasons.append("1位-4位差が小さい")
    out["skip_candidate"] = len(reasons) > 0
    out["skip_reason"] = "|".join(reasons)
    out["top1_score"] = top1_score
    out["spread_1_4"] = spread_1_4
    return out


def apply_quality_filters(score_df: pd.DataFrame, settings: dict) -> pd.DataFrame:
    out = score_df.copy()
    pass_flags, reasons = [], []
    for _, row in out.iterrows():
        reason_list = []
        score = row.get("score", np.nan)
        qty = row.get("suggested_qty", 0)
        note = str(row.get("note", "") or "")
        volume_flag = str(row.get("volume_flag", "") or "")
        if settings["require_positive_score"] and (pd.isna(score) or score <= 0):
            reason_list.append("スコア<=0")
        if pd.isna(qty) or qty < settings["min_suggested_qty"]:
            reason_list.append("口数不足")
        if "価格取得不可" in note:
            reason_list.append("価格取得不可")
        if settings["use_volume_filter"] and volume_flag == "低出来高":
            reason_list.append("低出来高")
        pass_flags.append(len(reason_list) == 0)
        reasons.append("|".join(reason_list))
    out["フィルタ通過"] = pass_flags
    out["除外理由"] = reasons
    out["selected"] = False
    passed_idx = out[out["フィルタ通過"]].sort_values("score", ascending=False).head(settings["top_n"]).index
    out.loc[passed_idx, "selected"] = True
    out["final_rank"] = np.nan
    selected = out[out["selected"]].sort_values("score", ascending=False)
    for i, idx in enumerate(selected.index, start=1):
        out.loc[idx, "final_rank"] = i
    return out


def calculate_suggested_quantity(score_df: pd.DataFrame, close_df: pd.DataFrame, volume_df: pd.DataFrame, settings: dict):
    jp_close = close_df[list(JP_ETFS.keys())].copy()
    jp_volume = volume_df[list(JP_ETFS.keys())].copy()
    latest_close = jp_close.ffill().iloc[-1]
    latest_volume = jp_volume.ffill().iloc[-1]
    budget_per_name = settings["total_budget"] / settings["top_n"]
    est_prices, unit_prices, est_qtys, est_amounts = [], [], [], []
    prev_volumes, volume_flags, alert_flags, notes = [], [], [], []
    for _, row in score_df.iterrows():
        code = row["jp_code"]
        price = latest_close.get(code, np.nan)
        volume = latest_volume.get(code, np.nan)
        qty, amount, note, vol_flag = 0, 0.0, "", ""
        alerts = []
        if pd.isna(price) or price <= 0:
            note = "価格取得不可"
        else:
            qty = max(int(budget_per_name // (price * settings["min_price_buffer"])), 0)
            amount = float(price * qty)
            if price < settings["low_price_threshold"]:
                alerts.append("低単価")
            if qty > settings["high_qty_threshold"]:
                alerts.append("口数多め")
        if settings["use_volume_filter"] and (pd.isna(volume) or volume < settings["min_avg_volume"]):
            vol_flag = "低出来高"
            alerts.append("出来高注意")
        est_prices.append(float(price) if pd.notna(price) else np.nan)
        unit_prices.append(float(price) if pd.notna(price) else np.nan)
        est_qtys.append(int(qty))
        est_amounts.append(float(amount))
        prev_volumes.append(float(volume) if pd.notna(volume) else np.nan)
        volume_flags.append(vol_flag)
        alert_flags.append("|".join(alerts))
        notes.append(note)
    out = score_df.copy()
    out["suggested_budget"] = budget_per_name
    out["estimated_price"] = est_prices
    out["unit_price"] = unit_prices
    out["suggested_qty"] = est_qtys
    out["suggested_amount"] = est_amounts
    out["prev_jp_volume"] = prev_volumes
    out["volume_flag"] = volume_flags
    out["alert_flag"] = alert_flags
    out["note"] = notes
    out = add_skip_flags(out, settings)
    out = apply_quality_filters(out, settings)
    return out


def build_signal_log_df(score_df: pd.DataFrame, aligned_jp_index, aligned_us_index, settings: dict) -> pd.DataFrame:
    signal_date = pd.Timestamp(aligned_jp_index[-1])
    latest_us_date, latest_jp_date = get_latest_mapping_info(pd.DataFrame(index=aligned_us_index), pd.DataFrame(index=aligned_jp_index))
    out = score_df.copy().rename(columns={
        "jp_code": "日本ETFコード",
        "jp_name": "日本ETF名",
        "score": "スコア",
        "rank": "順位",
        "final_rank": "最終順位",
        "selected": "採用",
        "suggested_budget": "推奨予算",
        "estimated_price": "推定価格",
        "unit_price": "1口金額",
        "suggested_qty": "推奨口数",
        "suggested_amount": "推奨約定金額",
        "prev_jp_volume": "前日出来高",
        "volume_flag": "出来高フラグ",
        "alert_flag": "注意フラグ",
        "note": "備考",
        "valid_train_count": "有効学習件数",
        "skip_candidate": "見送り候補",
        "skip_reason": "見送り理由",
        "top1_score": "1位スコア",
        "spread_1_4": "1位-4位差",
    })
    out.insert(0, "実行時刻", now_text())
    out.insert(1, "シグナル日付", to_date_str(signal_date))
    out.insert(2, "使用米国日付", latest_us_date)
    out.insert(3, "使用日本日付", latest_jp_date)
    out.insert(4, "計算方式", "pca_regression")
    out.insert(5, "PCA主成分数", settings["pca_components"])
    cols = [
        "実行時刻", "シグナル日付", "使用米国日付", "使用日本日付", "計算方式", "PCA主成分数",
        "有効学習件数", "見送り候補", "見送り理由", "1位スコア", "1位-4位差",
        "フィルタ通過", "除外理由", "日本ETFコード", "日本ETF名", "スコア", "順位", "最終順位",
        "採用", "推奨予算", "推定価格", "1口金額", "推奨口数", "推奨約定金額",
        "前日出来高", "出来高フラグ", "注意フラグ", "備考",
    ]
    return out[cols]


def build_daily_summary_df(signal_df: pd.DataFrame) -> pd.DataFrame:
    if signal_df.empty:
        return pd.DataFrame()
    first_row = signal_df.iloc[0]
    selected_df = signal_df[signal_df["採用"] == True].copy()
    passed_df = signal_df[signal_df["フィルタ通過"] == True].copy()
    code_list = selected_df["日本ETFコード"].tolist()
    name_list = selected_df["日本ETF名"].tolist()
    while len(code_list) < 3:
        code_list.append("")
    while len(name_list) < 3:
        name_list.append("")
    return pd.DataFrame([{
        "実行時刻": first_row["実行時刻"],
        "シグナル日付": first_row["シグナル日付"],
        "使用米国日付": first_row["使用米国日付"],
        "使用日本日付": first_row["使用日本日付"],
        "計算方式": first_row["計算方式"],
        "PCA主成分数": first_row["PCA主成分数"],
        "見送り候補": first_row["見送り候補"],
        "見送り理由": first_row["見送り理由"],
        "1位スコア": first_row["1位スコア"],
        "1位-4位差": first_row["1位-4位差"],
        "フィルタ通過本数": len(passed_df),
        "最終採用本数": len(selected_df),
        "採用1位コード": code_list[0],
        "採用2位コード": code_list[1],
        "採用3位コード": code_list[2],
        "採用1位名": name_list[0],
        "採用2位名": name_list[1],
        "採用3位名": name_list[2],
        "コメント": "",
    }])


def ensure_trade_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    num_cols = ["予定順位", "予定スコア", "予定予算", "1口金額", "予定口数", "予定約定金額", "買値", "売値", "口数", "損益額", "損益率"]
    for col in TRADE_COLS:
        if col not in out.columns:
            out[col] = np.nan if col in num_cols else ""
    return out[TRADE_COLS]


def recalc_trade_input_df(df: pd.DataFrame) -> pd.DataFrame:
    out = ensure_trade_columns(df)
    if out.empty:
        return out
    for col in ["買値", "売値", "口数"]:
        out[col] = clean_numeric_series(out[col])
    if "売買日" in out.columns:
        out["売買日"] = out["売買日"].apply(normalize_date_like_text)
    pnl, pnl_pct, checks = [], [], []
    for _, row in out.iterrows():
        exec_flag = str(row.get("実行有無", "") or "").strip()
        buy, sell, qty = row.get("買値", np.nan), row.get("売値", np.nan), row.get("口数", np.nan)
        if exec_flag == "×":
            pnl.append(np.nan); pnl_pct.append(np.nan); checks.append("不要")
        elif pd.isna(buy) or pd.isna(sell) or pd.isna(qty):
            pnl.append(np.nan); pnl_pct.append(np.nan); checks.append("未入力")
        else:
            trade_pnl = (sell - buy) * qty
            trade_pct = np.nan if buy == 0 else (sell - buy) / buy
            pnl.append(trade_pnl); pnl_pct.append(trade_pct); checks.append("OK")
    out["損益額"] = pnl
    out["損益率"] = pnl_pct
    out["入力チェック"] = checks
    return ensure_trade_columns(out)


def build_trade_input_df(signal_df: pd.DataFrame) -> pd.DataFrame:
    selected_df = signal_df[signal_df["採用"] == True].copy()
    if selected_df.empty:
        return pd.DataFrame(columns=TRADE_COLS)
    trade_df = pd.DataFrame({
        "売買日": selected_df["シグナル日付"],
        "日本ETFコード": selected_df["日本ETFコード"],
        "日本ETF名": selected_df["日本ETF名"],
        "予定順位": selected_df["最終順位"],
        "予定スコア": selected_df["スコア"],
        "予定予算": selected_df["推奨予算"],
        "1口金額": selected_df["1口金額"],
        "予定口数": selected_df["推奨口数"],
        "予定約定金額": selected_df["推奨約定金額"],
        "注意フラグ": selected_df["注意フラグ"],
        "実行有無": "",
        "買値": np.nan,
        "売値": np.nan,
        "口数": np.nan,
        "損益額": np.nan,
        "損益率": np.nan,
        "入力チェック": "未入力",
        "メモ": "",
    })
    return recalc_trade_input_df(trade_df)


def trade_key_series(df: pd.DataFrame) -> pd.Series:
    work = ensure_trade_columns(df)
    dates = work["売買日"].apply(normalize_date_like_text).fillna("")
    codes = work["日本ETFコード"].fillna("").astype(str).str.strip()
    return dates + "|" + codes


def merge_trade_ledger(base_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    base = recalc_trade_input_df(base_df)
    new = recalc_trade_input_df(new_df)
    if base.empty:
        merged = new.copy()
    else:
        base["_k"] = trade_key_series(base)
        new["_k"] = trade_key_series(new)
        merged = pd.concat([base, new], ignore_index=True).drop_duplicates(subset=["_k"], keep="last")
        merged = merged.drop(columns=["_k"], errors="ignore")
    merged["_sort"] = pd.to_datetime(merged["売買日"].astype(str).str.replace("/", "-", regex=False), errors="coerce")
    merged = merged.sort_values(["_sort", "予定順位", "日本ETFコード"], na_position="last").drop(columns=["_sort"], errors="ignore")
    return recalc_trade_input_df(merged).reset_index(drop=True)


def append_or_replace_rows(title: str, new_df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    base = read_ws_df(title)
    if base.empty:
        merged = new_df.copy()
    else:
        base = base.copy()
        new = new_df.copy()
        for col in key_cols:
            if "日付" in col or "売買日" in col:
                base[col] = base[col].apply(normalize_date_like_text)
                new[col] = new[col].apply(normalize_date_like_text)
            else:
                base[col] = base[col].fillna("").astype(str).str.strip()
                new[col] = new[col].fillna("").astype(str).str.strip()
        base["_k"] = base[key_cols].fillna("").astype(str).agg("|".join, axis=1)
        new["_k"] = new[key_cols].fillna("").astype(str).agg("|".join, axis=1)
        merged = pd.concat([base, new], ignore_index=True).drop_duplicates(subset=["_k"], keep="last")
        merged = merged.drop(columns=["_k"], errors="ignore")
    write_ws_df(title, merged)
    return merged


def load_saved_state_from_sheets():
    sys_map = load_system_map()
    signal_df = read_ws_df("当日シグナル")
    daily_all = read_ws_df("日次サマリー")
    ledger_df = read_ws_df("売買記録台帳")
    signal_date = normalize_date_like_text(sys_map.get("last_signal_date", ""))
    if signal_date and not daily_all.empty and "シグナル日付" in daily_all.columns:
        normalized_dates = daily_all["シグナル日付"].apply(normalize_date_like_text)
        daily_df = daily_all[normalized_dates == signal_date].copy()
        if daily_df.empty:
            daily_df = daily_all.tail(1).copy()
    else:
        daily_df = daily_all.tail(1).copy() if not daily_all.empty else pd.DataFrame()
    if signal_date and not ledger_df.empty and "売買日" in ledger_df.columns:
        normalized_trade_dates = ledger_df["売買日"].apply(normalize_date_like_text)
        trade_df = ledger_df[normalized_trade_dates == signal_date].copy()
    else:
        trade_df = pd.DataFrame(columns=TRADE_COLS)
    return signal_df, daily_df, recalc_trade_input_df(trade_df), sys_map


def save_signal_bundle(signal_df: pd.DataFrame, daily_df: pd.DataFrame, trade_df: pd.DataFrame, settings_map: dict, signal_date: str):
    write_ws_df("当日シグナル", signal_df)
    append_or_replace_rows("日次サマリー", daily_df, ["シグナル日付"])
    ledger_df = read_ws_df("売買記録台帳")
    merged_ledger = merge_trade_ledger(ledger_df, trade_df)
    write_ws_df("売買記録台帳", merged_ledger)
    save_settings_map(settings_map)
    lock_until = datetime.combine((now_jst() + timedelta(days=1)).date(), datetime.min.time(), tzinfo=JST).replace(hour=6)
    sys_map = load_system_map()
    sys_map["last_signal_date"] = normalize_date_like_text(signal_date)
    sys_map["lock_until"] = lock_until.isoformat(timespec="minutes")
    sys_map["last_saved_at"] = now_text()
    save_system_map(sys_map)


def is_locked(sys_map: dict):
    last_signal_date = normalize_date_like_text(sys_map.get("last_signal_date", ""))
    lock_until_text = sys_map.get("lock_until", "")
    lock_dt = parse_dt_or_none(lock_until_text)
    if lock_dt and now_jst() < lock_dt:
        return True, f"前回確定日: {last_signal_date or '未設定'} / 再計算ロック: {lock_dt.strftime('%Y-%m-%d %H:%M')} JST まで"
    return False, f"前回確定日: {last_signal_date or '未設定'} / 再計算ロックなし"


def format_display_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["スコア", "予定スコア", "1位スコア", "1位-4位差"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(4)
    yen_cols = ["推奨予算", "推定価格", "1口金額", "推奨約定金額", "予定予算", "予定約定金額", "買値", "売値", "損益額"]
    for col in yen_cols:
        if col in out.columns:
            vals = pd.to_numeric(out[col], errors="coerce")
            out[col] = vals.apply(lambda x: "" if pd.isna(x) else f"¥{x:,.0f}")
    if "損益率" in out.columns:
        vals = pd.to_numeric(out["損益率"], errors="coerce")
        out["損益率"] = vals.apply(lambda x: "" if pd.isna(x) else f"{x * 100:.2f}%")
    return out


def make_excel_download(signal_df: pd.DataFrame, daily_df: pd.DataFrame, trade_df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        signal_df.to_excel(writer, sheet_name="予測記録", index=False)
        daily_df.to_excel(writer, sheet_name="日次サマリー", index=False)
        trade_df.to_excel(writer, sheet_name="売買記録", index=False)
    output.seek(0)
    return output.getvalue()


def make_csv_download(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


st.title("日米時差ETF戦略 / Google Sheets 保存版")
st.caption("設定シート・システムシート対応。朝に1回だけ確定計算し、翌日06:00(JST)まで再計算しません。")

try:
    ensure_base_sheets_and_defaults()
except Exception as e:
    st.error(f"Google Sheets 初期化エラー: {e}")
    st.stop()

settings_map = load_settings_map()
settings = jp_settings_to_internal(settings_map)
system_map = load_system_map()

with st.sidebar:
    st.subheader("現在の設定（設定シートを使用）")
    st.write(f"モード: **{settings['mode']}**")
    st.write(f"総投資額: **{settings['total_budget']:,}**")
    st.write(f"最大採用本数: **{settings['top_n']}**")
    st.write(f"ローリング窓: **{settings['rolling_window']}**")
    st.write(f"PCA主成分数: **{settings['pca_components']}**")
    st.write(f"MIN_SCORE_SPREAD: **{settings['min_score_spread']:.4f}**")
    st.write(f"スコア>0のみ採用: **{'ON' if settings['require_positive_score'] else 'OFF'}**")
    st.write(f"出来高フィルタ: **{'ON' if settings['use_volume_filter'] else 'OFF'}**")
    reload_button = st.button("保存済みデータを再読込", use_container_width=True)
    run_button = st.button("朝の確定計算を実行", type="primary", use_container_width=True)

for key, default in {
    "signal_df": pd.DataFrame(),
    "daily_df": pd.DataFrame(),
    "trade_df": pd.DataFrame(columns=TRADE_COLS),
    "system_map": system_map,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

if reload_button or st.session_state["signal_df"].empty:
    try:
        signal_df, daily_df, trade_df, sys_map = load_saved_state_from_sheets()
        st.session_state["signal_df"] = signal_df
        st.session_state["daily_df"] = daily_df
        st.session_state["trade_df"] = trade_df
        st.session_state["system_map"] = sys_map
        if reload_button:
            st.success("Google Sheets の保存済みデータを再読込しました。")
    except Exception as e:
        st.error(f"Google Sheets 読込エラー: {e}")

system_map = st.session_state["system_map"]
locked, lock_text = is_locked(system_map)
st.info(lock_text)

if run_button:
    if locked:
        st.warning("今回は再計算しません。保存済みデータをそのまま使ってください。テスト時は Google Sheets の『システム』シートで lock_until を過去日時へ変更するか、last_signal_date を空欄にしてください。")
    else:
        try:
            with st.spinner("朝の確定計算を実行して Google Sheets へ保存中..."):
                close_df, open_df, volume_df = download_price_data(period="2y")
                if close_df.empty or open_df.empty:
                    raise RuntimeError("価格データ取得に失敗しました。")
                us_ret = calc_us_close_to_close_returns(close_df)
                jp_ret = calc_jp_open_to_close_returns(open_df, close_df)
                aligned_us, aligned_jp = align_us_to_jp(us_ret, jp_ret)
                if aligned_us.empty or aligned_jp.empty:
                    raise RuntimeError("日米営業日の対応付けに失敗しました。")
                score_df = compute_scores(aligned_us, aligned_jp, settings)
                score_df = calculate_suggested_quantity(score_df, close_df, volume_df, settings)
                signal_df = build_signal_log_df(score_df, aligned_jp.index, aligned_us.index, settings)
                daily_df = build_daily_summary_df(signal_df)
                trade_df = build_trade_input_df(signal_df)
                signal_date = str(daily_df.iloc[0]["シグナル日付"])
                save_signal_bundle(signal_df, daily_df, trade_df, settings_map, signal_date)
                signal_df, daily_df, trade_df, sys_map = load_saved_state_from_sheets()
            st.session_state["signal_df"] = signal_df
            st.session_state["daily_df"] = daily_df
            st.session_state["trade_df"] = trade_df
            st.session_state["system_map"] = sys_map
            st.success("朝の確定計算を保存しました。翌日06:00(JST)まで再計算しません。")
            st.rerun()
        except Exception as e:
            st.error(f"朝の確定計算エラー: {e}")

signal_df = st.session_state["signal_df"]
daily_df = st.session_state["daily_df"]
trade_df_state = recalc_trade_input_df(st.session_state["trade_df"])

if not signal_df.empty:
    summary_row = daily_df.iloc[0] if not daily_df.empty else pd.Series(dtype=object)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("シグナル日付", str(summary_row.get("シグナル日付", "")))
    c2.metric("フィルタ通過本数", int(pd.to_numeric(summary_row.get("フィルタ通過本数", 0), errors="coerce") or 0))
    c3.metric("最終採用本数", int(pd.to_numeric(summary_row.get("最終採用本数", 0), errors="coerce") or 0))
    c4.metric("見送り候補", "はい" if str(summary_row.get("見送り候補", "False")).lower() in ["true", "1"] else "いいえ")

    tabs = st.tabs(["日次サマリー", "候補一覧", "売買記録入力", "ダウンロード"])

    with tabs[0]:
        st.dataframe(format_display_df(daily_df), use_container_width=True, hide_index=True)

    with tabs[1]:
        view_cols = [
            "見送り候補", "見送り理由", "フィルタ通過", "除外理由", "日本ETFコード", "日本ETF名",
            "スコア", "順位", "最終順位", "採用", "推奨予算", "1口金額", "推奨口数", "推奨約定金額",
            "注意フラグ", "備考",
        ]
        st.dataframe(format_display_df(signal_df[view_cols].copy()), use_container_width=True, hide_index=True)

    with tabs[2]:
        st.write("夕方〜夜はここで買値・売値・口数を入力し、Google Sheets の売買記録台帳へ保存します。")
        base_trade_df = recalc_trade_input_df(trade_df_state.copy())
        edited_df = st.data_editor(
            base_trade_df,
            use_container_width=True,
            hide_index=True,
            disabled=[
                "売買日", "日本ETFコード", "日本ETF名", "予定順位", "予定スコア", "予定予算",
                "1口金額", "予定口数", "予定約定金額", "注意フラグ", "損益額", "損益率", "入力チェック",
            ],
            column_config={
                "実行有無": st.column_config.SelectboxColumn("実行有無", options=["", "〇", "×"], required=False),
                "買値": st.column_config.NumberColumn("買値", min_value=0.0, step=1.0, format="¥%.0f"),
                "売値": st.column_config.NumberColumn("売値", min_value=0.0, step=1.0, format="¥%.0f"),
                "口数": st.column_config.NumberColumn("口数", min_value=0.0, step=1.0, format="%.0f"),
                "損益額": st.column_config.NumberColumn("損益額", format="¥%.0f"),
                "損益率": st.column_config.NumberColumn("損益率", format="%.4f"),
            },
            key="trade_editor",
        )
        if st.button("入力内容を反映して Google Sheets へ保存", use_container_width=True):
            try:
                new_trade_df = recalc_trade_input_df(edited_df)
                ledger_df = read_ws_df("売買記録台帳")
                merged = merge_trade_ledger(ledger_df, new_trade_df)
                write_ws_df("売買記録台帳", merged)
                st.session_state["trade_df"] = new_trade_df
                st.success("売買記録台帳へ保存しました。")
                st.rerun()
            except Exception as e:
                st.error(f"売買記録保存エラー: {e}")

    with tabs[3]:
        excel_bytes = make_excel_download(signal_df, daily_df, trade_df_state)
        signal_date = str(daily_df.iloc[0]["シグナル日付"]) if not daily_df.empty else now_jst().strftime("%Y-%m-%d")
        st.download_button(
            "当日Excelダウンロード",
            data=excel_bytes,
            file_name=f"etf_signal_{signal_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        st.download_button(
            "当日売買記録CSV",
            data=make_csv_download(trade_df_state),
            file_name=f"trade_input_{signal_date}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with st.expander("空のスプレッドシートに必要な項目"):
        st.markdown(
            """
### 設定シート
列は **項目 / 値** の2列です。初期項目は次です。

- モード
- 総投資額
- 最大採用本数
- ローリング窓
- 最低必要履歴数
- PCA主成分数
- リッジ係数
- MIN_SCORE_SPREAD
- 1位スコア<=0で見送り候補
- スコア>0のみ採用
- 出来高フィルタを使う
- 最小推奨口数
- 数量計算安全係数
- 最低平均出来高
- 低単価しきい値
- 口数多めしきい値

### システムシート
列は **key / value** の2列です。初期項目は次です。

- last_signal_date
- lock_until
- last_saved_at
            """
        )

    with st.expander("テスト時の再計算解除方法"):
        st.markdown(
            """
- Google Sheets の **システム** シートで `lock_until` を過去日時へ変更します。
- もしくは `last_signal_date` を空欄にします。
- その後、アプリで **保存済みデータを再読込** を押してから **朝の確定計算を実行** してください。
            """
        )
else:
    st.info("左のサイドバーから **保存済みデータを再読込**、または **朝の確定計算を実行** を押してください。")
