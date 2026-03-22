#!/usr/bin/env python3
"""
Lifestudio ERP 豊川店 - 顧客・子供データ一括収集スクリプト
============================================================
使い方:
  python3 lifeerp_crawler.py --cookie "PHPSESSID=xxxx; ..."

Cookieの取得方法:
  1. Chrome で staff.lifeerp.net を開く
  2. F12 → Application タブ → Cookies → https://staff.lifeerp.net
  3. 全Cookieをコピー、または Network タブ → リクエスト選択 → Headers → Cookie をコピー
"""

import argparse
import json
import os
import re
import time
import sys
from datetime import date, datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ─── 設定 ───────────────────────────────────────────────
BASE_URL   = "https://staff.lifeerp.net/"
B_SEQ      = 49        # 豊川店
DATA_DIR   = Path("/sessions/tender-stoic-gauss/erp_data")
WORK_DIR   = Path("/sessions/tender-stoic-gauss")
OUTPUT_DIR = Path("/sessions/tender-stoic-gauss/mnt/outputs")
PROGRESS_FILE = WORK_DIR / "progress_customers.json"
SLEEP_SEC  = 0.4       # リクエスト間隔（秒）
# ────────────────────────────────────────────────────────


def make_session(cookie_str: str) -> requests.Session:
    """Cookieを設定したセッションを作成"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36",
        "Referer": BASE_URL,
    })
    # Cookie文字列をパース
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            k, v = item.split("=", 1)
            session.cookies.set(k.strip(), v.strip(), domain="staff.lifeerp.net")
    return session


def test_session(session: requests.Session) -> bool:
    """ログイン済みか確認"""
    try:
        r = session.get(BASE_URL + "?run_id=my_home&top_menu=1", timeout=15)
        return r.status_code == 200 and "lifeerp" in r.url.lower()
    except Exception as e:
        print(f"[ERROR] 接続失敗: {e}")
        return False


# ─── Phase 1: estimate_list から 顧客No. → e_seq マッピング ───

def get_eseq_map_from_page(session: requests.Session, page_no: int) -> dict:
    """
    estimate_list の1ページから {顧客No.: e_seq} を取得
    HTMLの <tr onclick="...e_seq=XXXXX..."> からe_seqを抽出
    """
    params = (
        f"top_menu=1&lm_id=1&s_seq=1&b_seq={B_SEQ}"
        f"&run_id=estimate_list&display_count=200&page_no={page_no}"
    )
    url = BASE_URL + "?" + params
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"  [ERROR] page {page_no}: {e}")
        return {}

    soup = BeautifulSoup(r.text, "lxml")
    mapping = {}

    # パターン1: <tr onclick="...e_seq=271448..."> の行
    for tr in soup.find_all("tr"):
        onclick = tr.get("onclick", "")
        eseq_m = re.search(r"e_seq=(\d+)", onclick)
        if not eseq_m:
            continue
        e_seq = eseq_m.group(1)

        # 同じ行から顧客No.（6桁数字）を探す
        cells = tr.find_all("td")
        for cell in cells:
            txt = cell.get_text(strip=True)
            if re.match(r"^\d{6}$", txt):
                mapping[txt] = e_seq
                break

    # パターン2: data属性 data-e_seq or data-eseq
    if not mapping:
        for tr in soup.find_all("tr"):
            e_seq = tr.get("data-e_seq") or tr.get("data-eseq") or tr.get("data-id")
            if e_seq:
                cells = tr.find_all("td")
                for cell in cells:
                    txt = cell.get_text(strip=True)
                    if re.match(r"^\d{6}$", txt):
                        mapping[txt] = e_seq
                        break

    return mapping


def build_eseq_map(session: requests.Session, target_kokyaku_set: set) -> dict:
    """
    全estimate_listページを巡回して 顧客No. → e_seq マッピングを構築
    target_kokyaku_set に含まれる顧客だけ対象
    """
    eseq_map_file = WORK_DIR / "eseq_map.json"

    # キャッシュがあれば読み込む
    if eseq_map_file.exists():
        with open(eseq_map_file) as f:
            cached = json.load(f)
        print(f"[INFO] e_seqマップ キャッシュ読み込み: {len(cached)}件")
        # 対象が全部揃っていれば終了
        if target_kokyaku_set.issubset(set(cached.keys())):
            return cached
    else:
        cached = {}

    print("[INFO] estimate_list を巡回して e_seqマップを構築中...")
    page = 1
    found_pages_with_data = 0

    while True:
        print(f"  estimate_list page {page}...", end=" ", flush=True)
        page_map = get_eseq_map_from_page(session, page)
        print(f"{len(page_map)}件")

        if not page_map:
            print(f"  [INFO] ページ {page}: データなし → 終了")
            break

        cached.update(page_map)
        found_pages_with_data += 1

        # 対象顧客が全部見つかったら終了
        covered = target_kokyaku_set.intersection(set(cached.keys()))
        print(f"  対象顧客カバー率: {len(covered)}/{len(target_kokyaku_set)}")
        if covered == target_kokyaku_set:
            print("  [INFO] 全対象顧客のe_seq取得完了")
            break

        page += 1
        if page > 50:  # 安全リミット
            break
        time.sleep(SLEEP_SEC)

    # 保存
    with open(eseq_map_file, "w") as f:
        json.dump(cached, f, ensure_ascii=False)
    print(f"[INFO] e_seqマップ保存: {len(cached)}件 → {eseq_map_file}")

    return cached


# ─── Phase 2: estimate_view から子供の誕生日取得 ───

def parse_children_from_html(html: str) -> list:
    """
    estimate_view の HTMLから子供の誕生日情報を抽出
    フォーマット例: 子供1ひな性別女子供1[英]Hina誕生日2014-08-31(11)
    """
    text = BeautifulSoup(html, "lxml").get_text(" ")
    children = []

    # パターン: 誕生日YYYY-MM-DD(年齢) の繰り返し
    bday_pattern = re.compile(r"誕生日\s*(\d{4}-\d{2}-\d{2})\s*\(\d+\)")
    gender_pattern = re.compile(r"性別\s*([男女])")

    # 子供ブロックを探す: 子供N から始まる
    # テキスト全体から子供ブロックを分割
    blocks = re.split(r"子供(\d+)", text)

    child_num = 0
    for i, block in enumerate(blocks):
        if re.match(r"^\d+$", block.strip()):
            child_num = int(block.strip())
            continue
        if child_num > 0 and child_num <= 10:
            bday_m = bday_pattern.search(block)
            gender_m = gender_pattern.search(block)
            if bday_m:
                children.append({
                    "子供番号": child_num,
                    "性別": gender_m.group(1) if gender_m else "不明",
                    "誕生日": bday_m.group(1),
                })
            child_num = 0

    # フォールバック: 単純に全誕生日を抽出（上記で取れなかった場合）
    if not children:
        for m in bday_pattern.finditer(text):
            children.append({
                "子供番号": len(children) + 1,
                "性別": "不明",
                "誕生日": m.group(1),
            })

    return children


def get_customer_data(session: requests.Session, e_seq: str) -> dict:
    """estimate_view から顧客情報・子供誕生日を取得"""
    url = BASE_URL + f"?top_menu=1&lm_id=1&s_seq=1&b_seq={B_SEQ}&run_id=estimate_view&e_seq={e_seq}"
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        return {"error": str(e), "children": []}

    html = r.text
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ")

    # 顧客No.確認
    kno_m = re.search(r"顧客No[.\s]*(\d{6})", text)
    kokyaku_no = kno_m.group(1) if kno_m else ""

    # 子供データ
    children = parse_children_from_html(html)

    return {
        "kokyaku_no": kokyaku_no,
        "e_seq": e_seq,
        "children": children,
    }


def collect_all_customer_data(session: requests.Session, eseq_map: dict,
                               target_kokyaku_set: set) -> dict:
    """
    対象顧客全員の estimate_view を巡回して子供データを収集
    進捗をファイルに保存（途中再開可能）
    """
    # 進捗読み込み
    progress = {}
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
        print(f"[INFO] 進捗読み込み: {len(progress)}件完了済み")

    targets = [(kno, eseq_map[kno]) for kno in sorted(target_kokyaku_set)
               if kno in eseq_map and kno not in progress]

    print(f"[INFO] 残り取得対象: {len(targets)}件")
    total = len(targets)

    for i, (kno, e_seq) in enumerate(targets):
        data = get_customer_data(session, e_seq)
        progress[kno] = data

        # 進捗表示
        if (i + 1) % 50 == 0 or (i + 1) == total:
            pct = (i + 1) / total * 100 if total > 0 else 100
            print(f"  [{i+1}/{total}] {pct:.1f}% - 顧客No.{kno}: "
                  f"子供{len(data.get('children', []))}人")
            # 50件ごとに進捗保存
            with open(PROGRESS_FILE, "w") as f:
                json.dump(progress, f, ensure_ascii=False)

        time.sleep(SLEEP_SEC)

    # 最終保存
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, ensure_ascii=False)
    print(f"[INFO] 顧客データ収集完了: {len(progress)}件 → {PROGRESS_FILE}")

    return progress


# ─── Phase 3: 年齢計算 & Excel 出力 ───

def calc_age(birthday_str: str, reference_date_str: str) -> int:
    """誕生日と基準日から年齢を計算"""
    try:
        bday = datetime.strptime(birthday_str, "%Y-%m-%d").date()
        ref  = datetime.strptime(reference_date_str, "%Y-%m-%d").date()
        age = ref.year - bday.year
        if (ref.month, ref.day) < (bday.month, bday.day):
            age -= 1
        return max(age, 0)
    except Exception:
        return -1


def build_excel(reservations: list, customer_data: dict, output_path: Path):
    """
    予約データ＋顧客データを結合してExcelに出力
    シート1: 予約×子供（1行1子供）
    シート2: 顧客サマリー（撮影回数・子供人数）
    シート3: 年齢分布集計
    """
    print("[INFO] Excelデータ構築中...")

    # 予約回数カウント
    visit_count = {}
    for r in reservations:
        kno = r["e_seq"]  # ※parsed_records.json では e_seq フィールドが実際は顧客No.
        visit_count[kno] = visit_count.get(kno, 0) + 1

    # ─ シート1: 予約×子供の詳細行 ─
    rows_detail = []
    for r in reservations:
        kno       = r["e_seq"]
        shoot_date = r["yoyaku_date"]
        shoot_type = r.get("shoot_type", "")

        cust = customer_data.get(kno, {})
        children = cust.get("children", [])

        if not children:
            rows_detail.append({
                "顧客No":   kno,
                "撮影日":   shoot_date,
                "撮影種類": shoot_type,
                "来店回数": visit_count.get(kno, 1),
                "子供番号": "",
                "性別":     "",
                "誕生日":   "",
                "撮影時年齢": "",
            })
        else:
            for child in children:
                age_at_shoot = calc_age(child["誕生日"], shoot_date)
                rows_detail.append({
                    "顧客No":    kno,
                    "撮影日":    shoot_date,
                    "撮影種類":  shoot_type,
                    "来店回数":  visit_count.get(kno, 1),
                    "子供番号":  child["子供番号"],
                    "性別":      child["性別"],
                    "誕生日":    child["誕生日"],
                    "撮影時年齢": age_at_shoot if age_at_shoot >= 0 else "",
                })

    df_detail = pd.DataFrame(rows_detail)

    # ─ シート2: 顧客サマリー ─
    rows_summary = []
    for kno, cust in customer_data.items():
        children = cust.get("children", [])
        rows_summary.append({
            "顧客No":     kno,
            "来店回数":   visit_count.get(kno, 0),
            "子供人数":   len(children),
            "誕生日リスト": ", ".join(c["誕生日"] for c in children),
        })
    df_summary = pd.DataFrame(rows_summary).sort_values("来店回数", ascending=False)

    # ─ シート3: 年齢分布 ─
    ages = [r["撮影時年齢"] for r in rows_detail if r["撮影時年齢"] != ""]
    age_dist = {}
    for a in ages:
        age_dist[a] = age_dist.get(a, 0) + 1

    rows_age = [{"年齢": k, "人数": v}
                for k, v in sorted(age_dist.items())]
    df_age = pd.DataFrame(rows_age)

    # ─ Excel書き出し ─
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_detail.to_excel(writer, sheet_name="予約×子供詳細", index=False)
        df_summary.to_excel(writer, sheet_name="顧客サマリー", index=False)
        df_age.to_excel(writer, sheet_name="年齢分布", index=False)

        # 書式設定
        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            for col in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in col)
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 30)

    print(f"[INFO] Excel出力完了: {output_path}")
    print(f"  - 予約×子供詳細: {len(df_detail)}行")
    print(f"  - 顧客サマリー:   {len(df_summary)}行")
    print(f"  - 年齢分布:       {len(df_age)}行")


# ─── メイン ────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Lifestudio ERP データ収集")
    parser.add_argument("--cookie", required=True,
                        help='Cookieヘッダー文字列 (例: "PHPSESSID=xxx; other=yyy")')
    parser.add_argument("--skip-eseq",  action="store_true",
                        help="e_seqマップ構築をスキップ（キャッシュ使用）")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="estimate_view巡回をスキップ（進捗ファイル使用）")
    args = parser.parse_args()

    # セッション作成
    print("[STEP 0] セッション作成・確認...")
    session = make_session(args.cookie)

    print("[STEP 0] 接続テスト...")
    if not test_session(session):
        print("[ERROR] ログインに失敗しました。Cookieを確認してください。")
        sys.exit(1)
    print("  ✓ ログイン確認OK")

    # 予約データ読み込み
    print("\n[STEP 1] 予約データ読み込み...")
    records_path = WORK_DIR / "parsed_records.json"
    if not records_path.exists():
        print(f"[ERROR] {records_path} が見つかりません")
        sys.exit(1)
    with open(records_path) as f:
        reservations = json.load(f)
    print(f"  予約件数: {len(reservations)}件")

    target_kokyaku = set(r["e_seq"] for r in reservations)
    print(f"  ユニーク顧客: {len(target_kokyaku)}名")

    # e_seqマップ構築
    if not args.skip_eseq:
        print("\n[STEP 2] e_seqマップ構築...")
        eseq_map = build_eseq_map(session, target_kokyaku)
    else:
        eseq_map_path = WORK_DIR / "eseq_map.json"
        with open(eseq_map_path) as f:
            eseq_map = json.load(f)
        print(f"[STEP 2] e_seqマップ スキップ（{len(eseq_map)}件キャッシュ）")

    covered = target_kokyaku.intersection(set(eseq_map.keys()))
    print(f"  e_seq マッピング完了: {len(covered)}/{len(target_kokyaku)}名")

    # 顧客データ収集
    if not args.skip_fetch:
        print("\n[STEP 3] estimate_view 巡回（子供誕生日取得）...")
        customer_data = collect_all_customer_data(session, eseq_map, target_kokyaku)
    else:
        with open(PROGRESS_FILE) as f:
            customer_data = json.load(f)
        print(f"[STEP 3] 収集スキップ（{len(customer_data)}件キャッシュ）")

    # Excel出力
    print("\n[STEP 4] Excel出力...")
    output_path = OUTPUT_DIR / "豊川店_顧客年齢分析.xlsx"
    build_excel(reservations, customer_data, output_path)

    print("\n✅ 完了!")
    print(f"  出力ファイル: {output_path}")


if __name__ == "__main__":
    main()
