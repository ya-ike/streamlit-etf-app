
# -*- coding: utf-8 -*-
"""
signal_engine.py
運用アプリ / バックテスト共通の計算エンジン
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import yfinance as yf
import pandas_market_calendars as mcal

JST = ZoneInfo("Asia/Tokyo")

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

DEFAULT_PRACTICAL_SETTINGS_JP = {
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

DEFAULT_PAPER_SETTINGS_JP = {
    "総投資額": "600000",
    "最大採用本数": "3",
    "ローリング窓": "60",
    "最低必要履歴数": "40",
    "PCA主成分数": "3",
    "リッジ係数": "0.000001",
    "MIN_SCORE_SPREAD": "0.0000",
    "1位スコア<=0で見送り候補": "FALSE",
    "スコア>0のみ採用": "FALSE",
    "出来高フィルタを使う": "FALSE",
    "最小推奨口数": "1",
    "数量計算安全係数": "1.02",
    "最低平均出来高": "1000",
    "低単価しきい値": "1000",
    "口数多めしきい値": "100",
}

SETTING_ITEMS_JP = list(DEFAULT_PRACTICAL_SETTINGS_JP.keys())


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


def build_settings_sheet_df(practical_map: dict | None = None, paper_map: dict | None = None) -> pd.DataFrame:
    practical_map = practical_map or DEFAULT_PRACTICAL_SETTINGS_JP.copy()
    paper_map = paper_map or DEFAULT_PAPER_SETTINGS_JP.copy()
    rows = []
    for item in SETTING_ITEMS_JP:
        p_val = str(practical_map.get(item, DEFAULT_PRACTICAL_SETTINGS_JP[item])).strip() or DEFAULT_PRACTICAL_SETTINGS_JP[item]
        r_val = str(paper_map.get(item, DEFAULT_PAPER_SETTINGS_JP[item])).strip() or DEFAULT_PAPER_SETTINGS_JP[item]
        rows.append({"項目": item, "実運用値": p_val, "論文寄り値": r_val})
    return pd.DataFrame(rows)


def load_excel_settings(config_path: str, mode_jp: str) -> tuple[dict, dict]:
    xl = pd.ExcelFile(config_path)
    global_df = pd.read_excel(xl, "全体設定")
    settings_df = pd.read_excel(xl, "設定")
    modes_df = pd.read_excel(xl, "実行モード")

    global_map = {str(r["項目"]).strip(): str(r["値"]).strip() for _, r in global_df.iterrows()}
    mode_col = "論文寄り値" if mode_jp == "論文寄り" else "実運用値"
    settings_map = {}
    for _, r in settings_df.iterrows():
        item = str(r["項目"]).strip()
        if item:
            settings_map[item] = str(r.get(mode_col, "")).strip()

    settings_map["モード"] = mode_jp
    mode_row = modes_df[modes_df["表示名"].astype(str).str.strip() == mode_jp]
    if not mode_row.empty:
        settings_map["英語モード"] = str(mode_row.iloc[0]["内部名"]).strip()

    return global_map, settings_map


def jp_settings_to_internal(settings_map: dict, global_map: dict | None = None) -> dict:
    defaults = DEFAULT_PAPER_SETTINGS_JP if settings_map.get("モード") == "論文寄り" else DEFAULT_PRACTICAL_SETTINGS_JP
    g = global_map or {}
    budget_mode = str(g.get("投資額モード", "総額指定")).strip() or "総額指定"
    internal = {
        "mode": settings_map.get("モード", "実運用"),
        "mode_en": settings_map.get("英語モード", "practical"),
        "budget_mode": budget_mode,
        "total_budget": int(float(g.get("総投資額", settings_map.get("総投資額", defaults["総投資額"])))),
        "per_name_budget": int(float(g.get("1銘柄投資額", 200000))),
        "top_n": int(float(settings_map.get("最大採用本数", defaults["最大採用本数"]))),
        "rolling_window": int(float(settings_map.get("ローリング窓", defaults["ローリング窓"]))),
        "min_history": int(float(settings_map.get("最低必要履歴数", defaults["最低必要履歴数"]))),
        "pca_components": int(float(settings_map.get("PCA主成分数", defaults["PCA主成分数"]))),
        "ridge_alpha": float(settings_map.get("リッジ係数", defaults["リッジ係数"])),
        "min_score_spread": float(settings_map.get("MIN_SCORE_SPREAD", defaults["MIN_SCORE_SPREAD"])),
        "skip_if_top1_leq_zero": parse_bool_jp(settings_map.get("1位スコア<=0で見送り候補", defaults["1位スコア<=0で見送り候補"]), True),
        "require_positive_score": parse_bool_jp(settings_map.get("スコア>0のみ採用", defaults["スコア>0のみ採用"]), True),
        "use_volume_filter": parse_bool_jp(settings_map.get("出来高フィルタを使う", defaults["出来高フィルタを使う"]), False),
        "min_suggested_qty": int(float(settings_map.get("最小推奨口数", defaults["最小推奨口数"]))),
        "min_price_buffer": float(settings_map.get("数量計算安全係数", defaults["数量計算安全係数"])),
        "min_avg_volume": int(float(settings_map.get("最低平均出来高", defaults["最低平均出来高"]))),
        "low_price_threshold": float(settings_map.get("低単価しきい値", defaults["低単価しきい値"])),
        "high_qty_threshold": float(settings_map.get("口数多めしきい値", defaults["口数多めしきい値"])),
    }
    return internal


def get_market_dates(start_date: str, end_date: str) -> Tuple[pd.DatetimeIndex, pd.DatetimeIndex]:
    nyse = mcal.get_calendar("NYSE")
    xtks = mcal.get_calendar("XTKS")
    nyse_sched = nyse.schedule(start_date=start_date, end_date=end_date)
    xtks_sched = xtks.schedule(start_date=start_date, end_date=end_date)
    us_dates = pd.DatetimeIndex(nyse_sched.index.tz_localize(None).normalize().unique()).sort_values()
    jp_dates = pd.DatetimeIndex(xtks_sched.index.tz_localize(None).normalize().unique()).sort_values()
    return us_dates, jp_dates


def should_skip_for_holiday(signal_date: pd.Timestamp, prev_us_map: pd.Series, prev_jp_map: pd.Series) -> bool:
    prev_us = prev_us_map.get(signal_date, pd.NaT)
    prev_jp = prev_jp_map.get(signal_date, pd.NaT)
    if pd.isna(prev_us):
        return True
    if pd.isna(prev_jp):
        return False
    prev_prev_us = prev_us_map.get(prev_jp, pd.NaT)
    if pd.isna(prev_prev_us):
        return False
    return pd.Timestamp(prev_us) == pd.Timestamp(prev_prev_us)


def build_prev_us_map(jp_dates: pd.DatetimeIndex, us_dates: pd.DatetimeIndex) -> pd.Series:
    mapping = {}
    us_dates = us_dates.sort_values()
    for jp_date in pd.DatetimeIndex(jp_dates).sort_values():
        prev_us = us_dates[us_dates < jp_date]
        mapping[jp_date] = prev_us[-1] if len(prev_us) > 0 else pd.NaT
    return pd.Series(mapping)


def build_prev_jp_map(jp_dates: pd.DatetimeIndex) -> pd.Series:
    jp_dates = pd.DatetimeIndex(jp_dates).sort_values()
    mapping = {}
    prev = pd.NaT
    for d in jp_dates:
        mapping[d] = prev
        prev = d
    return pd.Series(mapping)

def build_next_jp_map(jp_dates: pd.DatetimeIndex) -> pd.Series:
    jp_dates = pd.DatetimeIndex(jp_dates).sort_values()
    mapping = {}
    for i, d in enumerate(jp_dates):
        mapping[d] = jp_dates[i + 1] if i + 1 < len(jp_dates) else pd.NaT
    return pd.Series(mapping)


def get_execution_jp_date(signal_jp_date: pd.Timestamp, jp_dates: pd.DatetimeIndex) -> pd.Timestamp:
    next_map = build_next_jp_map(jp_dates)
    return next_map.get(pd.Timestamp(signal_jp_date), pd.NaT)


def download_price_data(start_date: str | None = None, end_date: str | None = None, period: str = "2y"):
    if start_date and end_date:
        fetch_start = (pd.Timestamp(start_date) - pd.Timedelta(days=500)).strftime("%Y-%m-%d")
        fetch_end = (pd.Timestamp(end_date) + pd.Timedelta(days=5)).strftime("%Y-%m-%d")
        data = yf.download(
            tickers=ALL_TICKERS, start=fetch_start, end=fetch_end, interval="1d",
            auto_adjust=False, group_by="ticker", progress=False, threads=True,
        )
    else:
        data = yf.download(
            tickers=ALL_TICKERS, period=period, interval="1d",
            auto_adjust=False, group_by="ticker", progress=False, threads=True,
        )

    close_df, open_df, volume_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if isinstance(data.columns, pd.MultiIndex):
        for ticker in ALL_TICKERS:
            if ticker not in data.columns.get_level_values(0):
                continue
            sub = data[ticker].copy()
            if "Close" in sub.columns:
                close_df[ticker] = pd.to_numeric(sub["Close"], errors="coerce")
            if "Open" in sub.columns:
                open_df[ticker] = pd.to_numeric(sub["Open"], errors="coerce")
            if "Volume" in sub.columns:
                volume_df[ticker] = pd.to_numeric(sub["Volume"], errors="coerce")
    else:
        t0 = ALL_TICKERS[0]
        if "Close" in data.columns:
            close_df[t0] = pd.to_numeric(data["Close"], errors="coerce")
        if "Open" in data.columns:
            open_df[t0] = pd.to_numeric(data["Open"], errors="coerce")
        if "Volume" in data.columns:
            volume_df[t0] = pd.to_numeric(data["Volume"], errors="coerce")

    close_df.index = pd.to_datetime(close_df.index).normalize()
    open_df.index = pd.to_datetime(open_df.index).normalize()
    volume_df.index = pd.to_datetime(volume_df.index).normalize()
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
    if settings["budget_mode"] == "1銘柄指定":
        budget_per_name = settings["per_name_budget"]
    else:
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


def get_latest_mapping_info(aligned_us: pd.DataFrame, aligned_jp: pd.DataFrame):
    if aligned_us.empty or aligned_jp.empty:
        return "", ""
    return pd.Timestamp(aligned_us.index[-1]).strftime("%Y-%m-%d"), pd.Timestamp(aligned_jp.index[-1]).strftime("%Y-%m-%d")


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
    out.insert(1, "使用米国日付", latest_us_date)
    out.insert(2, "使用日本日付", to_date_str(signal_date))
    out.insert(3, "計算方式", "pca_regression")
    out.insert(4, "PCA主成分数", settings["pca_components"])
    cols = [
        "実行時刻", "使用米国日付", "使用日本日付", "計算方式", "PCA主成分数",
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


def build_trade_input_df(signal_df: pd.DataFrame) -> pd.DataFrame:
    selected_df = signal_df[signal_df["採用"] == True].copy()
    if selected_df.empty:
        return pd.DataFrame(columns=[
            "売買日", "日本ETFコード", "日本ETF名", "予定順位", "予定スコア", "予定予算", "1口金額",
            "予定口数", "予定約定金額", "注意フラグ", "実行有無", "買値", "売値", "口数",
            "損益額", "損益率", "入力チェック", "メモ",
        ])
    trade_df = pd.DataFrame({
        "売買日": selected_df["使用日本日付"],
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
    return trade_df


def run_signal_for_date(signal_date: pd.Timestamp, close_df: pd.DataFrame, open_df: pd.DataFrame, volume_df: pd.DataFrame, settings: dict):
    us_ret = calc_us_close_to_close_returns(close_df)
    jp_ret = calc_jp_open_to_close_returns(open_df, close_df)
    aligned_us, aligned_jp = align_us_to_jp(us_ret, jp_ret)
    aligned_us = aligned_us[aligned_us.index <= signal_date]
    aligned_jp = aligned_jp[aligned_jp.index <= signal_date]
    if aligned_us.empty or aligned_jp.empty or signal_date not in aligned_jp.index:
        raise RuntimeError("日米営業日の対応付けに失敗しました。")
    score_df = compute_scores(aligned_us, aligned_jp, settings)
    score_df = calculate_suggested_quantity(score_df, close_df.loc[:signal_date], volume_df.loc[:signal_date], settings)
    signal_df = build_signal_log_df(score_df, aligned_jp.index, aligned_us.index, settings)
    daily_df = build_daily_summary_df(signal_df)
    trade_df = build_trade_input_df(signal_df)
    return signal_df, daily_df, trade_df


def apply_same_day_prices(trade_df: pd.DataFrame, signal_date: pd.Timestamp, open_df: pd.DataFrame, close_df: pd.DataFrame) -> pd.DataFrame:
    if trade_df.empty:
        return trade_df
    out = trade_df.copy()
    out["売買日"] = pd.Timestamp(signal_date).strftime("%Y-%m-%d")
    buys, sells, qtys, pnls, pcts, checks = [], [], [], [], [], []
    for _, row in out.iterrows():
        code = row["日本ETFコード"]
        buy = open_df.loc[signal_date, code] if signal_date in open_df.index and code in open_df.columns else np.nan
        sell = close_df.loc[signal_date, code] if signal_date in close_df.index and code in close_df.columns else np.nan
        qty = pd.to_numeric(row["予定口数"], errors="coerce")
        buys.append(buy)
        sells.append(sell)
        qtys.append(qty)
        if pd.isna(buy) or pd.isna(sell) or pd.isna(qty):
            pnls.append(np.nan); pcts.append(np.nan); checks.append("未入力")
        else:
            pnl = (sell - buy) * qty
            pct = np.nan if buy == 0 else (sell - buy) / buy
            pnls.append(pnl); pcts.append(pct); checks.append("OK")
    out["実行有無"] = "〇"
    out["買値"] = buys
    out["売値"] = sells
    out["口数"] = qtys
    out["損益額"] = pnls
    out["損益率"] = pcts
    out["入力チェック"] = checks
    return out


def build_trade_input_df_for_execution_date(signal_df: pd.DataFrame, execution_date: pd.Timestamp) -> pd.DataFrame:
    trade_df = build_trade_input_df(signal_df)
    if trade_df.empty:
        return trade_df
    trade_df["売買日"] = pd.Timestamp(execution_date).strftime("%Y-%m-%d")
    return trade_df
