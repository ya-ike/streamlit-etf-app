# GitHub 未経験向け: Streamlit Community Cloud 公開手順

## 1. GitHub アカウント作成
- GitHub 公式サイトでアカウントを作成します。

## 2. 新しいリポジトリを作成
- `New repository` を押します。
- Repository name 例: `nikbei-etf-streamlit`
- Public を選びます。
- `Create repository` を押します。

## 3. ファイルをアップロード
ブラウザでそのまま可能です。

- `Add file` → `Upload files`
- 次のファイルをアップロード
  - `app.py`
  - `requirements.txt`
  - `README_streamlit.md`
- `Commit changes` を押します。

## 4. Streamlit Community Cloud にログイン
- Streamlit Community Cloud に GitHub アカウントでログインします。

## 5. アプリを新規作成
- `New app` を押します。
- GitHub のリポジトリを選びます。
- Branch は通常 `main`
- Main file path は `app.py`
- `Deploy` を押します。

## 6. 初回起動確認
- 数分で URL が発行されます。
- Android タブレットの Chrome などで URL を開きます。
- `シグナル計算を実行` を押して表示確認します。

## 7. 更新方法
- GitHub 側で `app.py` を編集して保存します。
- Community Cloud 側は通常自動で再デプロイされます。

## 8. 問題が出たときの確認点
- `requirements.txt` の書式ミス
- Main file path が `app.py` になっているか
- yfinance 側の一時的取得失敗
- 日本ETFの一部データ欠損

## 9. この初版の運用方法
- 朝にアプリを開く
- 計算実行
- 候補確認
- 売買後に買値・売値・口数を入力
- Excel または CSV をダウンロード
- 必要ならローカル台帳へ転記
