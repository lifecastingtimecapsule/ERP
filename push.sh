#!/bin/bash
# =====================================================
# Lifestudio ERP アナライザ - GitHub 強制プッシュスクリプト
# 使い方: bash push.sh "コミットメッセージ"
# =====================================================

BRANCH="main"
MSG="${1:-update: $(date '+%Y-%m-%d %H:%M')}"

# スクリプトのあるディレクトリに移動
cd "$(dirname "$0")"

# トークン読み込み（~/.erp_github_token から）
TOKEN_FILE="$HOME/.erp_github_token"
if [ ! -f "$TOKEN_FILE" ]; then
  echo "❌ トークンファイルが見つかりません: $TOKEN_FILE"
  echo "   以下のコマンドで作成してください："
  echo "   echo 'ghp_xxxx...' > ~/.erp_github_token"
  echo "   chmod 600 ~/.erp_github_token"
  exit 1
fi
TOKEN=$(cat "$TOKEN_FILE" | tr -d '[:space:]')
REPO="https://${TOKEN}@github.com/lifecastingtimecapsule/ERP.git"

echo "📦 変更をステージング..."
git add app.py crawler.py db.py instagram_graph.py instagram_crawler.py \
        excel_export.py lifeerp_crawler.py requirements.txt \
        static/index.html static/app.js .gitignore README.md push.sh 2>/dev/null

if git diff --cached --quiet; then
  echo "✅ 変更なし。プッシュ不要。"
  exit 0
fi

echo "📝 コミット: $MSG"
git commit -m "$MSG"

echo "🚀 GitHub へ強制プッシュ中..."
git push --force "$REPO" "$BRANCH"

if [ $? -eq 0 ]; then
  echo "✅ 完了！ https://github.com/lifecastingtimecapsule/ERP"
else
  echo "❌ プッシュ失敗。トークンを確認してください。"
fi
