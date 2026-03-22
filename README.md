# Lifestudio ERP アナライザ

Lifestudio 豊川店向けの顧客データ収集・分析ツールです。
ERP（staff.lifeerp.net）から予約・顧客情報を収集し、ダッシュボードで可視化します。
また Instagram Graph API を使った投稿データの収集・分析にも対応しています。

---

## 主な機能

### ERP データ収集
- staff.lifeerp.net からブラウザCookieを使って予約・顧客情報を自動収集
- 子供の誕生日・住所・母親情報を取得
- SSE（Server-Sent Events）によるリアルタイム進捗表示

### ダッシュボード分析（12項目）
| カテゴリ | 分析内容 |
|---|---|
| 月別推移 | 来店件数合計 / 撮影種類×月別 |
| 年齢・子供 | 撮影時年齢分布 / 兄弟順×撮影傾向 / 年齢×種類クロス / 兄弟人数×来店回数 |
| 母親分析 | 母親の年齢分布（撮影時・5歳刻み） |
| 地域分析 | 市別居住地ランキング / 地域×撮影種類 |
| 来店行動 | 初来店→次回撮影連鎖 / 0歳来店→次回連鎖 / 初来店月別件数 |

### Instagram 分析
- **Meta Graph API**（公式）を使用した投稿データ収集
- アクセストークンをDBに安全に保存（UIにはマスク表示）
- 月別投稿件数 / 時間帯別 / 曜日別 / 投稿種別 / 月別平均いいね数
- ハッシュタグ TOP30 / エンゲージメント上位10投稿

---

## 技術スタック

| レイヤー | 技術 |
|---|---|
| バックエンド | Python 3.11 / FastAPI / uvicorn |
| DB | SQLite（aiosqlite） |
| スクレイピング | requests / BeautifulSoup4 |
| フロントエンド | Bootstrap 5 / Chart.js 4 |
| リアルタイム通信 | Server-Sent Events（SSE） |
| Instagram API | Meta Graph API v18.0 |

---

## セットアップ

### 必要環境
- Python 3.10 以上
- pip

### インストール

```bash
cd lifeerp_app
pip install -r requirements.txt
```

### 起動

```bash
python app.py
```

ブラウザで `http://localhost:8000` を開いてください。

---

## ディレクトリ構成

```
lifeerp_app/
├── app.py                  # FastAPI メインアプリ
├── crawler.py              # ERP データ収集クローラー
├── db.py                   # SQLite データベース操作
├── instagram_graph.py      # Instagram Graph API 収集
├── instagram_crawler.py    # Instagram 旧実装（参考用）
├── excel_export.py         # Excel エクスポート
├── requirements.txt        # Python 依存パッケージ
├── static/
│   ├── index.html          # フロントエンド HTML
│   └── app.js              # フロントエンド JavaScript
└── data/                   # 収集データ置き場（gitignore済み）
```

---

## Instagram Graph API の設定方法

1. [Meta for Developers](https://developers.facebook.com/apps/) でアプリを作成
2. 「Instagram グラフ API」を製品に追加
3. Instagram Business アカウントと連携
4. アクセストークンを発行
5. アプリの「Instagram」タブ → トークン入力欄に貼り付けて「検証して保存」

> トークンはローカルの SQLite DB に保存されます。外部サーバーへの送信は行いません。

---

## 注意事項

- 本ツールは **Lifestudio 豊川店の内部利用専用**です
- 収集した顧客情報（DB ファイル）は `.gitignore` により Git 管理対象外です
- ERP のCookie は定期的に失効するため、再取得が必要な場合があります
