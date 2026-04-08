# -*- coding: utf-8 -*-
"""
Streamlit Community Cloud 版
日米時差ETF戦略 試験運用アプリ

- シグナル計算
- 日次サマリー表示
- 候補一覧表示
- 売買記録入力
- CSV / Excel ダウンロード
- 台帳CSV / Excel をアップロードして追記・再ダウンロード
"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf


st.set_page_config(
    page_title="日米時差ETF戦略",
    page_icon="📈",
    layout="wide",
)


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

DEFAULTS = {
    "total_budget": 600000,
    "top_n": 3,
    "rolling_window": 60,
    "min_history": 40,
    "min_price_buffer": 1.02,
    "min_avg_volume": 1000,
    "use_volume_filter": False,
    "low_price_threshold": 1000,
    "high_qty_threshold": 100,
    "pca_components": 3,
    "ridge_alpha": 1e-6,
    "skip_if_top1_leq_zero": True,
    "min_score_spread": 0.0010,
    "require_positive_score": True,
    "min_suggested_qty": 1,
}

TRADE_COLS = [
    "売買日", "日本ETFコード", "日本ETF名", "予定順位", "予定スコア", "予定予算", "1口金額",
    "予定口数", "予定約定金額", "注意フラグ", "実行有無", "買値", "売値", "口数",
    "損益額", "損益率", "入力チェック", "メモ",
]


# -----------------------------
# 計算ロジック
# -----------------------------
def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_date_str(ts) -> str:
    if pd.isna(ts):
        return ""
    return pd.Timestamp(ts).strftime("%Y-%m-%d")


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
        if len(ALL_TICKERS) >= 1:
            t0 = ALL_TICKERS[0]
            if "Close" in data.columns:
                close_df[t0] = data["Close"]
            if "Open" in data.columns:
                open_df[t0] = data["Open"]
            if "Volume" in data.columns:
                volume_df[t0] = data["Volume"]

    return close_df.sort_index(), open_df.sort_index(), volume_df.sort_index()


def calc_us_close_to_close_returns(close_df: pd.DataFrame) -> pd.DataFrame:
    us_close = close_df[list(US_ETFS.keys())].copy()
    return us_close.pct_change()


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
        if pd.isna(us_date):
            continue
        if us_date not in us_ret.index:
            continue
        aligned_rows.append(us_ret.loc[us_date].values)
        aligned_index.append(jp_date)

    aligned_us = pd.DataFrame(
        aligned_rows,
        index=pd.DatetimeIndex(aligned_index),
        columns=us_ret.columns,
    )

    common_index = aligned_us.index.intersection(jp_ret.index)
    aligned_us = aligned_us.loc[common_index].sort_index()
    aligned_jp = jp_ret.loc[common_index].sort_index()
    return aligned_us, aligned_jp


def get_latest_mapping_info(aligned_us: pd.DataFrame, aligned_jp: pd.DataFrame):
    if aligned_us.empty or aligned_jp.empty:
        return "", ""
    latest_us_date = pd.Timestamp(aligned_us.index[-1]).strftime("%Y-%m-%d")
    latest_jp_date = pd.Timestamp(aligned_jp.index[-1]).strftime("%Y-%m-%d")
    return latest_us_date, latest_jp_date


def compute_scores(aligned_us: pd.DataFrame, aligned_jp: pd.DataFrame, settings: dict) -> pd.DataFrame:
    min_history = settings["min_history"]
    rolling_window = settings["rolling_window"]
    pca_components = settings["pca_components"]
    ridge_alpha = settings["ridge_alpha"]
    top_n = settings["top_n"]

    if len(aligned_us) < min_history or len(aligned_jp) < min_history:
        raise ValueError(f"履歴不足です。aligned_us={len(aligned_us)}, aligned_jp={len(aligned_jp)}")

    use_us = aligned_us.iloc[-rolling_window:].copy()
    use_jp = aligned_jp.iloc[-rolling_window:].copy()

    us_mean = use_us.mean(axis=0)
    us_std = use_us.std(axis=0, ddof=0).replace(0, np.nan)
    us_z = ((use_us - us_mean) / us_std).fillna(0.0)

    x_full = us_z.copy()
    t_full, n_assets = x_full.shape
    k = min(pca_components, n_assets, t_full)

    cov = np.cov(x_full.values, rowvar=False)
    eigvals, eigvecs = np.linalg.eigh(cov)
    order = np.argsort(eigvals)[::-1]
    eigvecs = eigvecs[:, order]

    v = eigvecs[:, :k]
    f_full = x_full.values @ v

    latest_us = x_full.iloc[-1].values.reshape(1, -1)
    latest_factor = latest_us @ v

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
            reg = ridge_alpha * np.eye(xtx.shape[0])
            reg[0, 0] = 0.0
            beta = np.linalg.solve(xtx + reg, xreg.T @ y)
            latest_x = np.column_stack([np.ones(1), latest_factor])
            pred = float((latest_x @ beta).ravel()[0])
            if np.isnan(pred) or np.isinf(pred):
                pred = 0.0
            scores[jp_code] = pred
        except Exception:
            scores[jp_code] = 0.0

    score_df = pd.DataFrame({"jp_code": list(scores.keys()), "score": list(scores.values())})
    score_df["valid_train_count"] = score_df["jp_code"].map(valid_counts)
    score_df["score"] = pd.to_numeric(score_df["score"], errors="coerce").fillna(0.0)
    score_df["jp_name"] = score_df["jp_code"].map(JP_ETFS)
    score_df = score_df.sort_values("score", ascending=False).reset_index(drop=True)
    score_df["rank"] = np.arange(1, len(score_df) + 1)
    score_df["selected"] = score_df["rank"] <= top_n
    return score_df


def add_skip_flags(score_df: pd.DataFrame, settings: dict) -> pd.DataFrame:
    out = score_df.copy()

    if out.empty:
        out["skip_candidate"] = False
        out["skip_reason"] = ""
        out["top1_score"] = np.nan
        out["spread_1_4"] = np.nan
        return out

    top1_score = float(out.iloc[0]["score"]) if len(out) >= 1 else 0.0
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
    pass_flags = []
    reasons = []

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

    est_prices = []
    unit_prices = []
    est_qtys = []
    est_amounts = []
    prev_volumes = []
    volume_flags = []
    alert_flags = []
    notes = []

    for _, row in score_df.iterrows():
        code = row["jp_code"]
        price = latest_close.get(code, np.nan)
        volume = latest_volume.get(code, np.nan)

        qty = 0
        amount = 0.0
        note = ""
        vol_flag = ""
        alerts = []

        if pd.isna(price) or price <= 0:
            note = "価格取得不可"
        else:
            qty = int(budget_per_name // (price * settings["min_price_buffer"]))
            qty = max(qty, 0)
            amount = float(price * qty)

            if price < settings["low_price_threshold"]:
                alerts.append("低単価")
            if qty > settings["high_qty_threshold"]:
                alerts.append("口数多め")

        if settings["use_volume_filter"]:
            if pd.isna(volume) or volume < settings["min_avg_volume"]:
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
    latest_us_date, latest_jp_date = get_latest_mapping_info(
        pd.DataFrame(index=aligned_us_index),
        pd.DataFrame(index=aligned_jp_index),
    )

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
        "有効学習件数", "見送り候補", "見送り理由", "1位スコア", "1位-4位差", "フィルタ通過", "除外理由",
        "日本ETFコード", "日本ETF名", "スコア", "順位", "最終順位", "採用", "推奨予算", "推定価格",
        "1口金額", "推奨口数", "推奨約定金額", "前日出来高", "出来高フラグ", "注意フラグ", "備考",
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


def recalc_trade_input_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return ensure_trade_columns(out)

    for col in ["買値", "売値", "口数"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    pnl = []
    pnl_pct = []
    checks = []

    for _, row in out.iterrows():
        exec_flag = str(row.get("実行有無", "") or "")
        buy = row.get("買値", np.nan)
        sell = row.get("売値", np.nan)
        qty = row.get("口数", np.nan)

        if exec_flag == "×":
            pnl.append(np.nan)
            pnl_pct.append(np.nan)
            checks.append("不要")
        elif pd.isna(buy) or pd.isna(sell) or pd.isna(qty):
            pnl.append(np.nan)
            pnl_pct.append(np.nan)
            checks.append("未入力")
        else:
            trade_pnl = (sell - buy) * qty
            trade_pct = np.nan if buy == 0 else (sell - buy) / buy
            pnl.append(trade_pnl)
            pnl_pct.append(trade_pct)
            checks.append("OK")

    out["損益額"] = pnl
    out["損益率"] = pnl_pct
    out["入力チェック"] = checks
    return ensure_trade_columns(out)


def ensure_trade_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in TRADE_COLS:
        if col not in out.columns:
            out[col] = np.nan if col in ["予定順位", "予定スコア", "予定予算", "1口金額", "予定口数", "予定約定金額", "買値", "売値", "口数", "損益額", "損益率"] else ""
    return out[TRADE_COLS]


def format_display_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    num4_cols = ["スコア", "予定スコア", "1位スコア", "1位-4位差"]
    int_cols = ["順位", "最終順位", "推奨口数", "前日出来高", "予定順位", "予定口数", "口数", "有効学習件数", "フィルタ通過本数", "最終採用本数"]

    for col in num4_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(4)
    for col in int_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    yen_cols = ["推奨予算", "推定価格", "1口金額", "推奨約定金額", "予定予算", "予定約定金額", "買値", "売値", "損益額"]
    for col in yen_cols:
        if col in out.columns:
            vals = pd.to_numeric(out[col], errors="coerce")
            out[col] = vals.apply(lambda x: "" if pd.isna(x) else f"¥{x:,.0f}")

    if "損益率" in out.columns:
        vals = pd.to_numeric(out["損益率"], errors="coerce")
        out["損益率"] = vals.apply(lambda x: "" if pd.isna(x) else f"{x * 100:.3f}%")

    return out


def ledger_key_columns(df: pd.DataFrame) -> pd.Series:
    work = df.copy()
    sell_date = pd.to_datetime(work.get("売買日"), errors="coerce").dt.strftime("%Y-%m-%d")
    code = work.get("日本ETFコード", "").fillna("").astype(str)
    return sell_date.fillna("") + "|" + code


def merge_trade_ledger(base_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    base = recalc_trade_input_df(ensure_trade_columns(base_df))
    new = recalc_trade_input_df(ensure_trade_columns(new_df))

    if base.empty:
        merged = new.copy()
    else:
        base["_ledger_key"] = ledger_key_columns(base)
        new["_ledger_key"] = ledger_key_columns(new)
        merged = pd.concat([base, new], ignore_index=True)
        merged = merged.drop_duplicates(subset=["_ledger_key"], keep="last")
        merged = merged.drop(columns=["_ledger_key"], errors="ignore")

    if "売買日" in merged.columns:
        merged["_sort_date"] = pd.to_datetime(merged["売買日"], errors="coerce")
        merged = merged.sort_values(["_sort_date", "予定順位", "日本ETFコード"], ascending=[True, True, True], na_position="last")
        merged = merged.drop(columns=["_sort_date"], errors="ignore")

    return recalc_trade_input_df(ensure_trade_columns(merged)).reset_index(drop=True)


def load_trade_ledger(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    elif name.endswith(".xlsx") or name.endswith(".xls"):
        xls = pd.ExcelFile(uploaded_file)
        if "売買記録" in xls.sheet_names:
            df = pd.read_excel(uploaded_file, sheet_name="売買記録")
        else:
            df = pd.read_excel(uploaded_file)
    else:
        raise ValueError("CSV または Excel(.xlsx) をアップロードしてください。")

    return recalc_trade_input_df(ensure_trade_columns(df))


def make_excel_download(signal_df: pd.DataFrame, daily_df: pd.DataFrame, trade_df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        signal_df.to_excel(writer, sheet_name="予測記録", index=False)
        daily_df.to_excel(writer, sheet_name="日次サマリー", index=False)
        trade_df.to_excel(writer, sheet_name="売買記録", index=False)
    output.seek(0)
    return output.getvalue()


def make_ledger_excel_download(ledger_df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        ledger_df.to_excel(writer, sheet_name="売買記録", index=False)
    output.seek(0)
    return output.getvalue()


def make_csv_download(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


# -----------------------------
# UI
# -----------------------------
st.title("日米時差ETF戦略 / Streamlit試験版")
st.caption("PCA回帰ベース・Androidタブレットのブラウザ利用を想定")

with st.sidebar:
    st.subheader("設定")
    total_budget = st.number_input("総投資額", min_value=100000, value=DEFAULTS["total_budget"], step=100000)
    top_n = st.number_input("最大採用本数", min_value=1, max_value=10, value=DEFAULTS["top_n"], step=1)
    rolling_window = st.number_input("ローリング窓", min_value=20, max_value=240, value=DEFAULTS["rolling_window"], step=5)
    min_history = st.number_input("最低必要履歴数", min_value=20, max_value=240, value=DEFAULTS["min_history"], step=5)
    pca_components = st.number_input("PCA主成分数", min_value=1, max_value=10, value=DEFAULTS["pca_components"], step=1)
    min_score_spread = st.number_input("MIN_SCORE_SPREAD", min_value=0.0, value=float(DEFAULTS["min_score_spread"]), step=0.0001, format="%.4f")
    use_volume_filter = st.checkbox("出来高フィルタを使う", value=DEFAULTS["use_volume_filter"])
    require_positive_score = st.checkbox("スコア>0のみ採用", value=DEFAULTS["require_positive_score"])
    min_suggested_qty = st.number_input("最小推奨口数", min_value=1, max_value=1000, value=DEFAULTS["min_suggested_qty"], step=1)
    run_button = st.button("シグナル計算を実行", type="primary", use_container_width=True)

settings = {
    **DEFAULTS,
    "total_budget": int(total_budget),
    "top_n": int(top_n),
    "rolling_window": int(rolling_window),
    "min_history": int(min_history),
    "pca_components": int(pca_components),
    "min_score_spread": float(min_score_spread),
    "use_volume_filter": bool(use_volume_filter),
    "require_positive_score": bool(require_positive_score),
    "min_suggested_qty": int(min_suggested_qty),
}

if "signal_df" not in st.session_state:
    st.session_state.signal_df = pd.DataFrame()
if "daily_df" not in st.session_state:
    st.session_state.daily_df = pd.DataFrame()
if "trade_df" not in st.session_state:
    st.session_state.trade_df = pd.DataFrame()
if "ledger_df" not in st.session_state:
    st.session_state.ledger_df = pd.DataFrame(columns=TRADE_COLS)

if run_button:
    try:
        with st.spinner("価格データ取得・計算中..."):
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

        st.session_state.signal_df = signal_df
        st.session_state.daily_df = daily_df
        st.session_state.trade_df = trade_df
        st.success("計算が完了しました。")
    except Exception as e:
        st.error(f"エラー: {e}")

signal_df = st.session_state.signal_df
daily_df = st.session_state.daily_df
trade_df_state = st.session_state.trade_df
ledger_df_state = st.session_state.ledger_df

if not signal_df.empty:
    summary_row = daily_df.iloc[0]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("シグナル日付", str(summary_row["シグナル日付"]))
    col2.metric("フィルタ通過本数", int(summary_row["フィルタ通過本数"]))
    col3.metric("最終採用本数", int(summary_row["最終採用本数"]))
    col4.metric("見送り候補", "はい" if bool(summary_row["見送り候補"]) else "いいえ")

    if bool(summary_row["見送り候補"]):
        st.warning(f"見送り候補です。理由: {summary_row['見送り理由']}")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["日次サマリー", "候補一覧", "売買記録入力", "台帳へ蓄積", "ダウンロード"])

    with tab1:
        st.dataframe(format_display_df(daily_df), use_container_width=True, hide_index=True)

    with tab2:
        view_cols = [
            "見送り候補", "見送り理由", "フィルタ通過", "除外理由", "日本ETFコード", "日本ETF名",
            "スコア", "順位", "最終順位", "採用", "推奨予算", "1口金額", "推奨口数", "推奨約定金額",
            "注意フラグ", "備考",
        ]
        candidate_view = signal_df[view_cols].copy()
        st.dataframe(format_display_df(candidate_view), use_container_width=True, hide_index=True)

    with tab3:
        st.write("採用銘柄だけ編集できます。実行有無は 〇 / × / 空欄 を想定しています。")

        edited_df = st.data_editor(
            trade_df_state.copy(),
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

        if st.button("入力内容を反映", key="apply_trade_input", use_container_width=True):
            st.session_state.trade_df = recalc_trade_input_df(edited_df)
            st.success("売買記録へ反映しました。")

        st.dataframe(format_display_df(st.session_state.trade_df), use_container_width=True, hide_index=True)

    with tab4:
        st.write("既存の台帳CSV / Excel を読み込み、今回の売買記録を追記して再ダウンロードできます。")
        uploaded_ledger = st.file_uploader(
            "台帳ファイルをアップロード",
            type=["csv", "xlsx", "xls"],
            help="CSV は売買記録CSV、Excel は売買記録シートを含むファイルを想定しています。",
            key="ledger_uploader",
        )

        c1, c2 = st.columns(2)
        with c1:
            if st.button("アップロードした台帳を読み込む", use_container_width=True):
                try:
                    if uploaded_ledger is None:
                        st.warning("先に台帳ファイルを選択してください。")
                    else:
                        st.session_state.ledger_df = load_trade_ledger(uploaded_ledger)
                        st.success("台帳を読み込みました。")
                except Exception as e:
                    st.error(f"台帳読込エラー: {e}")
        with c2:
            if st.button("現在の入力を台帳へ追記 / 上書き反映", use_container_width=True):
                st.session_state.ledger_df = merge_trade_ledger(st.session_state.ledger_df, st.session_state.trade_df)
                st.success("台帳へ現在分を反映しました。同じ売買日+ETFコードは最新内容で上書きしています。")

        ledger_df_state = st.session_state.ledger_df

        if not ledger_df_state.empty:
            ledger_exec = int((ledger_df_state["実行有無"] == "〇").sum()) if "実行有無" in ledger_df_state.columns else 0
            ledger_total = int(len(ledger_df_state))
            ledger_dates = pd.to_datetime(ledger_df_state["売買日"], errors="coerce") if "売買日" in ledger_df_state.columns else pd.Series(dtype="datetime64[ns]")
            d1, d2, d3 = st.columns(3)
            d1.metric("台帳件数", ledger_total)
            d2.metric("実行〇件数", ledger_exec)
            d3.metric("台帳最終日", "" if ledger_dates.dropna().empty else ledger_dates.max().strftime("%Y-%m-%d"))

            st.dataframe(format_display_df(ledger_df_state), use_container_width=True, hide_index=True)

            merged_csv = make_csv_download(ledger_df_state)
            merged_xlsx = make_ledger_excel_download(ledger_df_state)
            today_text = datetime.now().strftime("%Y%m%d")
            b1, b2 = st.columns(2)
            with b1:
                st.download_button(
                    "蓄積台帳CSVをダウンロード",
                    data=merged_csv,
                    file_name=f"trade_ledger_{today_text}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            with b2:
                st.download_button(
                    "蓄積台帳Excelをダウンロード",
                    data=merged_xlsx,
                    file_name=f"trade_ledger_{today_text}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
        else:
            st.info("まだ台帳は空です。まず現在の入力を反映するか、過去の台帳をアップロードしてください。")

    with tab5:
        excel_bytes = make_excel_download(signal_df, daily_df, st.session_state.trade_df)
        signal_csv = make_csv_download(signal_df)
        daily_csv = make_csv_download(daily_df)
        trade_csv = make_csv_download(st.session_state.trade_df)
        signal_date = str(daily_df.iloc[0]["シグナル日付"])

        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "Excelダウンロード",
                data=excel_bytes,
                file_name=f"etf_signal_{signal_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
            st.download_button(
                "予測記録CSV",
                data=signal_csv,
                file_name=f"signal_{signal_date}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c2:
            st.download_button(
                "日次サマリーCSV",
                data=daily_csv,
                file_name=f"daily_summary_{signal_date}.csv",
                mime="text/csv",
                use_container_width=True,
            )
            st.download_button(
                "売買記録CSV",
                data=trade_csv,
                file_name=f"trade_input_{signal_date}.csv",
                mime="text/csv",
                use_container_width=True,
            )
else:
    st.info("左のサイドバーから『シグナル計算を実行』を押してください。")

with st.expander("この試験版の位置づけ"):
    st.markdown(
        """
- この版は Community Cloud 上へ自動永続保存する方式ではありません。
- その代わり、**既存台帳をアップロード → 今回分を追記 → 蓄積台帳を再ダウンロード** できます。
- 同じ **売買日 + 日本ETFコード** は、台帳へ再反映したとき最新内容で上書きします。
- まずは CSV 台帳で試すのが簡単です。必要なら Excel 台帳でも運用できます。
        """
    )
