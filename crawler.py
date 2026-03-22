"""
Lifestudio ERP クローラー（Webアプリ用）
"""
import re
import time
import threading

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://staff.lifeerp.net/"
SLEEP_SEC = 0.4
B_SEQ_DEFAULT = 49  # 豊川店

# 都道府県・市抽出用（市レベルのみ）
PREF_RE = re.compile(r"([^\s\u3000]{2,4}[都道府県])")
CITY_RE = re.compile(r"([^\s\u3000]{2,8}市)")


def make_session(cookie_str):
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Referer": BASE_URL,
        }
    )
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            k, v = item.split("=", 1)
            session.cookies.set(k.strip(), v.strip(), domain="staff.lifeerp.net")
    return session


def test_session(session):
    try:
        r = session.get(BASE_URL + "?run_id=my_home&top_menu=1", timeout=15)
        return r.status_code == 200 and "staff.lifeerp.net" in r.url
    except Exception:
        return False


def get_eseq_map_from_page(session, page_no, b_seq=B_SEQ_DEFAULT):
    """estimate_list の1ページから {顧客No: e_seq} を取得"""
    params = (
        f"top_menu=1&lm_id=1&s_seq=1&b_seq={b_seq}"
        f"&run_id=estimate_list&display_count=200&page_no={page_no}"
    )
    try:
        r = session.get(BASE_URL + "?" + params, timeout=30)
        r.raise_for_status()
    except Exception:
        return {}

    soup = BeautifulSoup(r.text, "lxml")
    mapping = {}

    for tr in soup.find_all("tr"):
        onclick = tr.get("onclick", "")
        eseq_m = re.search(r"e_seq=(\d+)", onclick)
        if not eseq_m:
            continue
        e_seq = eseq_m.group(1)
        for cell in tr.find_all("td"):
            txt = cell.get_text(strip=True)
            if re.match(r"^\d{6}$", txt):
                mapping[txt] = e_seq
                break

    if not mapping:
        for tr in soup.find_all("tr"):
            e_seq = tr.get("data-e_seq") or tr.get("data-eseq") or tr.get("data-id")
            if e_seq:
                for cell in tr.find_all("td"):
                    txt = cell.get_text(strip=True)
                    if re.match(r"^\d{6}$", txt):
                        mapping[txt] = e_seq
                        break

    return mapping


def parse_children_from_html(html):
    """estimate_view HTML から子供の誕生日・性別を抽出"""
    text = BeautifulSoup(html, "lxml").get_text(" ")
    children = []
    bday_re = re.compile(r"誕生日\s*(\d{4}-\d{2}-\d{2})\s*\(\d+\)")
    gender_re = re.compile(r"性別\s*([男女])")

    blocks = re.split(r"子供(\d+)", text)
    child_num = 0
    for block in blocks:
        stripped = block.strip()
        if re.match(r"^\d+$", stripped):
            child_num = int(stripped)
            continue
        if 0 < child_num <= 10:
            bday_m = bday_re.search(block)
            gender_m = gender_re.search(block)
            if bday_m:
                children.append(
                    {
                        "子供番号": child_num,
                        "性別": gender_m.group(1) if gender_m else "不明",
                        "誕生日": bday_m.group(1),
                    }
                )
            child_num = 0

    if not children:
        for i, m in enumerate(bday_re.finditer(text), 1):
            children.append({"子供番号": i, "性別": "不明", "誕生日": m.group(1)})

    return children


def parse_address_from_html(html):
    """
    estimate_view HTML から住所を抽出し、都道府県・市区町村に分解する。
    複数の戦略を試みる。
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ")
    address_raw = ""

    # 戦略1: 「住所」ラベルの隣/次のtd
    for label_tag in soup.find_all(string=re.compile(r"住所")):
        parent = label_tag.parent
        if parent is None:
            continue
        # 同じ行のtdを探す
        tr = parent.find_parent("tr")
        if tr:
            tds = tr.find_all("td")
            for i, td in enumerate(tds):
                if "住所" in td.get_text():
                    if i + 1 < len(tds):
                        val = tds[i + 1].get_text(strip=True)
                        if val and len(val) > 3:
                            address_raw = val
                            break
        if address_raw:
            break

    # 戦略2: 郵便番号の後に続く住所
    if not address_raw:
        postal_m = re.search(r"[〒]?\d{3}[-ー]\d{4}\s*([^\s]{5,30}[市区町村][^\s]*)", text)
        if postal_m:
            address_raw = postal_m.group(1)

    # 戦略3: テキスト内の都道府県パターン
    if not address_raw:
        pref_m = PREF_RE.search(text)
        if pref_m:
            start = pref_m.start()
            # 都道府県から20文字以内に市区町村があるか
            chunk = text[start : start + 30]
            city_m = CITY_RE.search(chunk)
            if city_m:
                address_raw = chunk[: city_m.end()].strip()

    # 都道府県・市区町村を抽出
    prefecture, city = _normalize_address(address_raw)

    return {
        "address_raw": address_raw,
        "prefecture": prefecture,
        "city": city,
    }


def _normalize_address(address_raw):
    if not address_raw:
        return "", ""
    pref_m = PREF_RE.search(address_raw)
    prefecture = pref_m.group(1) if pref_m else ""

    # 都道府県以降の文字列から市区町村を探す
    remaining = address_raw[pref_m.end():] if pref_m else address_raw
    city_m = CITY_RE.search(remaining)
    city = city_m.group(1) if city_m else ""

    # 都道府県が取れなかった場合、全体から市区町村を探す
    if not city and not pref_m:
        city_m2 = CITY_RE.search(address_raw)
        city = city_m2.group(1) if city_m2 else ""

    return prefecture, city


def parse_mother_from_html(html):
    """
    estimate_view HTML から母親（保護者）の誕生日を抽出する。
    複数のパターンを試みる。
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ")
    mother_birthday = ""

    BDAY_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
    BDAY_JP_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
    MOTHER_KW = re.compile(r"(母|保護者|ママ|お母様|母親)")

    # 戦略1: 「母」「保護者」等のラベルを持つtdの近くにある誕生日
    for label_tag in soup.find_all(string=MOTHER_KW):
        parent = label_tag.parent
        if parent is None:
            continue
        tr = parent.find_parent("tr")
        if tr:
            for td in tr.find_all("td"):
                td_text = td.get_text(strip=True)
                m = BDAY_RE.search(td_text)
                if m:
                    mother_birthday = m.group(1)
                    break
                m2 = BDAY_JP_RE.search(td_text)
                if m2:
                    mother_birthday = f"{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"
                    break
        if mother_birthday:
            break

    # 戦略2: テキスト内の「母」「保護者」に続く日付
    if not mother_birthday:
        m = re.search(r"(?:母|保護者|ママ|母親)[^\d]{0,10}(\d{4}-\d{2}-\d{2})", text)
        if m:
            mother_birthday = m.group(1)

    if not mother_birthday:
        m = re.search(r"(?:母|保護者|ママ|母親)[^\d]{0,10}(\d{4})年(\d{1,2})月(\d{1,2})日", text)
        if m:
            mother_birthday = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    return mother_birthday


def get_customer_data(session, e_seq, b_seq=B_SEQ_DEFAULT):
    """estimate_view から子供データ + 住所 + 母親誕生日を取得"""
    url = (
        BASE_URL
        + f"?top_menu=1&lm_id=1&s_seq=1&b_seq={b_seq}&run_id=estimate_view&e_seq={e_seq}"
    )
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        return {"error": str(e), "children": [], "address": {}, "mother_birthday": ""}

    children = parse_children_from_html(r.text)
    address = parse_address_from_html(r.text)
    mother_birthday = parse_mother_from_html(r.text)

    return {"e_seq": e_seq, "children": children, "address": address, "mother_birthday": mother_birthday}


# ─── バックグラウンド収集ジョブ ──────────────────────────

class CollectionJob:
    def __init__(self, db, cookie_str, b_seq=B_SEQ_DEFAULT):
        self.db = db
        self.cookie_str = cookie_str
        self.b_seq = b_seq
        self.stop_event = threading.Event()
        self.thread = None
        self.progress_log = []
        self._lock = threading.Lock()

    def _log(self, msg):
        with self._lock:
            self.progress_log.append(msg)

    def start(self):
        if self.thread and self.thread.is_alive():
            return False
        self.stop_event.clear()
        with self._lock:
            self.progress_log = []
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        return True

    def stop(self):
        self.stop_event.set()

    def is_running(self):
        return self.thread is not None and self.thread.is_alive()

    def _run(self):
        try:
            session = make_session(self.cookie_str)
            self._log({"type": "info", "message": "接続確認中..."})
            if not test_session(session):
                self._log({"type": "error", "message": "Cookieが無効です。再度コピーしてください。"})
                return
            self._log({"type": "info", "message": "接続OK。データ収集を開始します。"})

            self._phase_eseq_map(session)
            if self.stop_event.is_set():
                self._log({"type": "stopped", "message": "中断しました。"})
                return

            self._phase_children(session)

            if self.stop_event.is_set():
                self._log({"type": "stopped", "message": "中断しました。"})
            else:
                self._log({"type": "done", "message": "収集完了！"})
        except Exception as e:
            self._log({"type": "error", "message": f"予期せぬエラー: {e}"})

    def _phase_eseq_map(self, session):
        target = self.db.get_unique_kokyaku(self.b_seq)
        eseq_map = self.db.get_eseq_map(self.b_seq)
        covered = target.intersection(set(eseq_map.keys()))

        if covered == target and eseq_map:
            self._log({"type": "info", "message": f"e_seqマップ キャッシュ使用 ({len(eseq_map)}件)"})
            return

        self._log({"type": "phase", "phase": "eseq_map", "message": "フェーズ1: 顧客マッピング構築中..."})
        page = 1
        while not self.stop_event.is_set():
            self._log({
                "type": "progress", "phase": "eseq_map",
                "message": f"estimate_list ページ {page} 取得中...", "page": page,
            })
            page_map = get_eseq_map_from_page(session, page, self.b_seq)
            if not page_map:
                self._log({"type": "info", "message": f"ページ {page}: データなし → フェーズ1完了"})
                break
            self.db.save_eseq_batch(page_map, self.b_seq)
            eseq_map.update(page_map)
            covered = target.intersection(set(eseq_map.keys()))
            self._log({
                "type": "progress", "phase": "eseq_map",
                "message": f"ページ{page}完了 ({len(page_map)}件 / カバー: {len(covered)}/{len(target)})",
                "page": page, "covered": len(covered), "total_customers": len(target),
            })
            if covered == target:
                self._log({"type": "info", "message": "全顧客のe_seq取得完了"})
                break
            page += 1
            if page > 50:
                break
            time.sleep(SLEEP_SEC)

    def _phase_children(self, session):
        target = self.db.get_unique_kokyaku(self.b_seq)
        eseq_map = self.db.get_eseq_map(self.b_seq)
        fetched = self.db.get_fetched_kokyaku(self.b_seq)

        targets = [
            (kno, eseq_map[kno])
            for kno in sorted(target)
            if kno in eseq_map and kno not in fetched
        ]
        total = len(targets)
        already_done = len(target) - total
        total_all = len(target)

        self._log({
            "type": "phase", "phase": "children",
            "message": f"フェーズ2: 子供データ・住所収集中... (残り{total}件)",
            "current": already_done, "total": total_all,
        })

        for i, (kno, e_seq) in enumerate(targets):
            if self.stop_event.is_set():
                break
            try:
                data = get_customer_data(session, e_seq, self.b_seq)
                addr = data.get("address", {})
                self.db.save_customer_data(
                    kno,
                    data.get("children", []),
                    address_raw=addr.get("address_raw", ""),
                    prefecture=addr.get("prefecture", ""),
                    city=addr.get("city", ""),
                    mother_birthday=data.get("mother_birthday", ""),
                    branch_id=self.b_seq,
                )
            except Exception:
                # エラーはスキップして続行
                self.db.save_customer_data(kno, [], branch_id=self.b_seq)

            done = already_done + i + 1
            if (i + 1) % 10 == 0 or (i + 1) == total:
                pct = round(done / total_all * 100, 1) if total_all else 100
                self._log({
                    "type": "progress", "phase": "children",
                    "message": f"収集中 {done}/{total_all}件...",
                    "current": done, "total": total_all, "percent": pct,
                })

            time.sleep(SLEEP_SEC)
