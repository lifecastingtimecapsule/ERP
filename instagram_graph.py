"""
Instagram Graph API 収集モジュール（公式）
Meta Developerで発行したアクセストークンを使用。
内部APIとは異なり、Meta公式の承認された方法。
"""
import re
import time
import threading
from datetime import datetime, timezone

import requests

GRAPH_BASE = "https://graph.facebook.com/v18.0"

MEDIA_FIELDS = ",".join([
    "id", "caption", "media_type", "timestamp",
    "like_count", "comments_count", "permalink", "shortcode",
])


# ─── トークン検証・アカウント取得 ──────────────────────

def validate_token(token: str) -> dict:
    """アクセストークンの有効性を確認し、ユーザー名を返す"""
    try:
        r = requests.get(
            f"{GRAPH_BASE}/me",
            params={"fields": "id,name", "access_token": token},
            timeout=10,
        )
        data = r.json()
        if "error" in data:
            return {"ok": False, "error": data["error"].get("message", "不明なエラー")}
        return {"ok": True, "fb_user_id": data.get("id"), "name": data.get("name")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_ig_account(token: str) -> dict:
    """
    FacebookページからInstagramビジネスアカウント情報を取得する。
    返り値: {"ok": True, "ig_user_id": ..., "username": ..., "followers": ..., "media_count": ...}
    """
    try:
        # FacebookページリストからInstagram Business Accountを探す
        r = requests.get(
            f"{GRAPH_BASE}/me/accounts",
            params={"fields": "id,name,access_token", "access_token": token},
            timeout=10,
        )
        pages = r.json().get("data", [])
        if not pages:
            return {"ok": False, "error": "Facebookページが見つかりません。ビジネスアカウントが必要です。"}

        for page in pages:
            page_id = page["id"]
            page_token = page.get("access_token", token)
            r2 = requests.get(
                f"{GRAPH_BASE}/{page_id}",
                params={
                    "fields": "instagram_business_account{id,username,name,followers_count,media_count}",
                    "access_token": page_token,
                },
                timeout=10,
            )
            ig = r2.json().get("instagram_business_account")
            if ig:
                return {
                    "ok": True,
                    "ig_user_id": ig["id"],
                    "username": ig.get("username", ""),
                    "name": ig.get("name", ""),
                    "followers": ig.get("followers_count", 0),
                    "media_count": ig.get("media_count", 0),
                    "page_token": page_token,
                }

        return {"ok": False, "error": "Instagramビジネスアカウントが見つかりません。Instagram BusinessアカウントをFacebookページに連携してください。"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ─── 投稿取得 ──────────────────────────────────────────

def fetch_media_page(token: str, ig_user_id: str, after: str = "") -> dict:
    """1ページ分（最大100件）の投稿を取得"""
    params = {"fields": MEDIA_FIELDS, "limit": 100, "access_token": token}
    if after:
        params["after"] = after
    try:
        r = requests.get(
            f"{GRAPH_BASE}/{ig_user_id}/media",
            params=params,
            timeout=20,
        )
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def _extract_shortcode(item: dict) -> str:
    """permalinkからshortcodeを抽出"""
    sc = item.get("shortcode", "")
    if sc:
        return sc
    permalink = item.get("permalink", "")
    m = re.search(r"/(p|reel|tv)/([^/]+)/", permalink)
    return m.group(2) if m else ""


def _parse_timestamp(ts_str: str) -> int:
    """ISO 8601 → Unixタイムスタンプ"""
    if not ts_str:
        return 0
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return 0


def parse_post_graph(item: dict, username: str) -> dict:
    """Graph APIレスポンスをDBに保存する形式に変換"""
    type_map = {"IMAGE": 1, "VIDEO": 2, "CAROUSEL_ALBUM": 8, "REELS": 2}
    media_type = type_map.get(item.get("media_type", ""), 0)
    return {
        "pk": item.get("id"),
        "username": username,
        "taken_at": _parse_timestamp(item.get("timestamp", "")),
        "media_type": media_type,
        "product_type": item.get("media_type", "").lower(),
        "like_count": item.get("like_count", 0),
        "comment_count": item.get("comments_count", 0),
        "play_count": 0,
        "caption": (item.get("caption") or "")[:2000],
        "shortcode": _extract_shortcode(item),
        "source": "graph_api",
    }


# ─── コレクションジョブ ──────────────────────────────

class IGGraphCollectionJob:
    def __init__(self, db, token: str, ig_user_id: str, username: str):
        self.db = db
        self.token = token
        self.ig_user_id = ig_user_id
        self.username = username
        self._stop = False
        self._running = False
        self._thread = None
        self.progress_log = []

    def start(self):
        self._stop = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop = True

    def is_running(self):
        return self._running

    def _log(self, msg_type: str, message: str, current=0, total=0):
        pct = round(current / total * 100) if total > 0 else 0
        self.progress_log.append({
            "type": msg_type, "message": message,
            "current": current, "total": total, "percent": pct,
        })

    def _run(self):
        self._running = True
        self._log("phase", f"📸 @{self.username} の投稿を収集中（公式Graph API）...")

        after = ""
        total_saved = 0
        page = 0

        while not self._stop:
            page += 1
            data = fetch_media_page(self.token, self.ig_user_id, after)

            if "error" in data:
                self._log("error", f"❌ APIエラー: {data['error']}")
                break

            items = data.get("data", [])
            if not items:
                break

            posts = [parse_post_graph(item, self.username) for item in items]
            self.db.save_instagram_posts(posts)
            total_saved += len(posts)

            paging = data.get("paging", {})
            cursors = paging.get("cursors", {})
            after = cursors.get("after", "")
            has_next = bool(paging.get("next"))

            self._log(
                "progress",
                f"@{self.username}: {total_saved}件収集中... (ページ{page})",
                current=total_saved,
                total=total_saved + (100 if has_next else 0),
            )

            if not has_next:
                break
            time.sleep(0.4)

        if self._stop:
            self._log("stopped", f"⏹ 中断（保存済み: {total_saved}件）")
        else:
            self._log("done", f"🎉 収集完了！ @{self.username} 合計{total_saved}件")

        self._running = False
