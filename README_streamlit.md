# 日米時差ETF戦略 Streamlit Community Cloud 試験版

## 目的
Android タブレットのブラウザから、以下を実行できる試験版です。

- シグナル計算
- 日次サマリー表示
- 候補一覧表示
- 売買記録入力
- Excel / CSV ダウンロード

## この版の制約
- Community Cloud 上への永続保存はしていません
- 毎回その場で yfinance からデータ取得します
- 入力した売買記録は画面上のセッション中のみ保持されます
- 必要に応じて Excel / CSV をダウンロードして保存してください

## ファイル構成
- `app.py` : Streamlit 本体
- `requirements.txt` : Community Cloud 用依存関係
- `README_streamlit.md` : この説明

## ローカルで試す手順
1. Python 3.11 前後を用意
2. 依存関係をインストール
   ```bash
   pip install -r requirements.txt
   ```
3. 起動
   ```bash
   streamlit run app.py
   ```

## Community Cloud へ上げる手順
1. GitHub に新しいリポジトリを作成
2. この3ファイルをアップロード
3. Streamlit Community Cloud にログイン
4. `New app` を選ぶ
5. GitHub リポジトリを指定
6. Main file path を `app.py` にする
7. Deploy を押す

## 今後の拡張候補
- ローカル Excel 台帳フォーマットに近い Excel 出力
- 既存 `data_utils.py` のロジックを `strategy_core.py` として共通化
- GitHub Secrets を使った限定的設定管理
- 永続保存先として Google Drive / Notion / Supabase / GitHub Releases などの検討
