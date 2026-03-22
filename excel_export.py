"""
Excel エクスポートモジュール（7シート）
"""
from pathlib import Path
from datetime import datetime

import pandas as pd


def calc_age(birthday_str, reference_date_str):
    try:
        by, bm, bd = map(int, birthday_str.split("-"))
        ry, rm, rd = map(int, reference_date_str.split("-"))
        age = ry - by
        if (rm, rd) < (bm, bd):
            age -= 1
        return max(age, 0)
    except Exception:
        return -1


def _age_bucket(age):
    if age <= 1:
        return "0-1歳"
    elif age <= 3:
        return "2-3歳"
    elif age <= 5:
        return "4-5歳"
    elif age <= 7:
        return "6-7歳"
    else:
        return "8歳以上"


def build_excel(reservations, customer_data, output_path):
    """
    予約データ＋顧客データを結合してExcelに出力（7シート）
    """
    output_path = Path(output_path)

    # 来店集計
    visit_count = {}
    first_visit = {}
    last_visit = {}
    for r in reservations:
        kno = r.get("kokyaku_no") or r.get("e_seq", "")
        date = r.get("yoyaku_date", "")
        visit_count[kno] = visit_count.get(kno, 0) + 1
        if kno not in first_visit or date < first_visit.get(kno, "9999"):
            first_visit[kno] = date
        if kno not in last_visit or date > last_visit.get(kno, ""):
            last_visit[kno] = date

    # ─ シート1: 全予約リスト ─
    rows_detail = []
    for r in reservations:
        kno = r.get("kokyaku_no") or r.get("e_seq", "")
        shoot_date = r.get("yoyaku_date", "")
        shoot_type = r.get("shoot_type", "")
        cust = customer_data.get(kno, {})
        children = cust.get("children", [])
        addr = cust.get("address", {})
        city = addr.get("city", "")

        if not children:
            rows_detail.append({
                "顧客No": kno, "撮影日": shoot_date, "撮影時刻": r.get("time", ""),
                "撮影種類": shoot_type, "来店回数": visit_count.get(kno, 1),
                "居住市区町村": city, "子供番号": "", "性別": "", "誕生日": "", "撮影時年齢": "",
            })
        else:
            for child in children:
                age_at_shoot = calc_age(child["誕生日"], shoot_date)
                rows_detail.append({
                    "顧客No": kno, "撮影日": shoot_date, "撮影時刻": r.get("time", ""),
                    "撮影種類": shoot_type, "来店回数": visit_count.get(kno, 1),
                    "居住市区町村": city, "子供番号": child.get("子供番号", ""),
                    "性別": child.get("性別", ""), "誕生日": child["誕生日"],
                    "撮影時年齢": age_at_shoot if age_at_shoot >= 0 else "",
                })
    df_detail = pd.DataFrame(rows_detail)

    # ─ シート2: 顧客別まとめ ─
    rows_summary = []
    all_kno = set(visit_count.keys()) | set(customer_data.keys())
    for kno in sorted(all_kno):
        cust = customer_data.get(kno, {})
        children = cust.get("children", [])
        addr = cust.get("address", {})
        rows_summary.append({
            "顧客No": kno, "来店回数": visit_count.get(kno, 0),
            "居住都道府県": addr.get("prefecture", ""),
            "居住市区町村": addr.get("city", ""),
            "住所（生データ）": addr.get("raw", ""),
            "子供人数": len(children),
            "誕生日リスト": ", ".join(c["誕生日"] for c in children),
            "初回撮影日": first_visit.get(kno, ""),
            "最終撮影日": last_visit.get(kno, ""),
        })
    df_summary = pd.DataFrame(rows_summary).sort_values("来店回数", ascending=False)

    # ─ シート3: 年齢分布集計 ─
    age_dist = {}
    for r in rows_detail:
        age = r.get("撮影時年齢")
        if isinstance(age, int) and 0 <= age <= 15:
            age_dist[age] = age_dist.get(age, 0) + 1
    df_age = pd.DataFrame(
        [{"年齢": k, "人数": v} for k, v in sorted(age_dist.items())]
    ) if age_dist else pd.DataFrame(columns=["年齢", "人数"])

    # ─ シート4: 撮影種類×月別集計 ─
    monthly = {}
    for r in reservations:
        date = r.get("yoyaku_date", "")
        stype = r.get("shoot_type", "不明")
        if date and len(date) >= 7:
            month = date[:7]
            monthly.setdefault(month, {})[stype] = monthly.get(month, {}).get(stype, 0) + 1
    all_types_monthly = sorted({st for m in monthly.values() for st in m})
    rows_monthly = [
        {"年月": month, **{st: monthly[month].get(st, 0) for st in all_types_monthly}}
        for month in sorted(monthly.keys())
    ]
    df_monthly = pd.DataFrame(rows_monthly) if rows_monthly else pd.DataFrame()

    # ─ シート5: 兄弟順×撮影種類クロス集計 ─
    # 子供番号(1=第1子, 2=第2子, 3=第3子)ごとの撮影種類カウント
    sibling_cross = {}
    for r in reservations:
        kno = r.get("kokyaku_no") or r.get("e_seq", "")
        stype = r.get("shoot_type", "不明")
        cust = customer_data.get(kno, {})
        for child in cust.get("children", []):
            cn = child.get("子供番号", 0)
            if cn > 0:
                label = f"第{cn}子" if cn <= 3 else "第4子以上"
                sibling_cross.setdefault(label, {})[stype] = \
                    sibling_cross.get(label, {}).get(stype, 0) + 1
    all_types_sib = sorted({st for d in sibling_cross.values() for st in d})
    sibling_order_labels = ["第1子", "第2子", "第3子", "第4子以上"]
    rows_sibling = [
        {"兄弟順": label, **{st: sibling_cross.get(label, {}).get(st, 0) for st in all_types_sib}}
        for label in sibling_order_labels if label in sibling_cross
    ]
    df_sibling = pd.DataFrame(rows_sibling) if rows_sibling else pd.DataFrame()

    # ─ シート6: 地域別来店数ランキング ─
    area_customers = {}
    area_visits = {}
    for kno, cust in customer_data.items():
        city = cust.get("address", {}).get("city", "")
        pref = cust.get("address", {}).get("prefecture", "")
        if not city:
            continue
        area_customers.setdefault(city, {"prefecture": pref, "customers": set(), "visits": 0})
        area_customers[city]["customers"].add(kno)
        area_customers[city]["visits"] += visit_count.get(kno, 0)
    rows_area = sorted(
        [
            {
                "都道府県": v["prefecture"],
                "市区町村": city,
                "顧客数": len(v["customers"]),
                "来店回数合計": v["visits"],
            }
            for city, v in area_customers.items()
        ],
        key=lambda x: x["顧客数"],
        reverse=True,
    )
    df_area = pd.DataFrame(rows_area) if rows_area else pd.DataFrame()

    # ─ シート7: 撮影連鎖分析（初回→次回） ─
    # 顧客ごとに来店順に並べて遷移を集計
    from collections import defaultdict
    cust_visits = defaultdict(list)
    for r in reservations:
        kno = r.get("kokyaku_no") or r.get("e_seq", "")
        cust_visits[kno].append((r.get("yoyaku_date", ""), r.get("shoot_type", "")))
    chain = defaultdict(lambda: defaultdict(int))
    for kno, visits in cust_visits.items():
        sorted_visits = sorted(visits, key=lambda v: v[0])
        for i in range(len(sorted_visits) - 1):
            from_t = sorted_visits[i][1]
            to_t = sorted_visits[i + 1][1]
            chain[from_t][to_t] += 1

    all_types_chain = sorted(set(list(chain.keys()) + [t for d in chain.values() for t in d]))
    rows_chain = [
        {"初回撮影種類": from_t, **{to_t: chain[from_t].get(to_t, 0) for to_t in all_types_chain}}
        for from_t in all_types_chain if from_t in chain
    ]
    df_chain = pd.DataFrame(rows_chain) if rows_chain else pd.DataFrame()

    # ─ Excel書き出し ─
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_detail.to_excel(writer, sheet_name="全予約リスト", index=False)
        df_summary.to_excel(writer, sheet_name="顧客別まとめ", index=False)
        df_age.to_excel(writer, sheet_name="年齢分布集計", index=False)
        df_monthly.to_excel(writer, sheet_name="撮影種類×月別", index=False)
        df_sibling.to_excel(writer, sheet_name="兄弟順×撮影種類", index=False)
        df_area.to_excel(writer, sheet_name="地域別来店ランキング", index=False)
        df_chain.to_excel(writer, sheet_name="撮影連鎖分析", index=False)

        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            for col in ws.columns:
                max_len = max(
                    (len(str(cell.value or "")) for cell in col), default=0
                )
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 35)

    return output_path
