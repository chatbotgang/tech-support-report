"""
generate_report.py
每週一執行：查詢 Supabase → 計算三個產品數據 → 輸出 JSON 至 docs/data/
"""

import os
import json
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta
from pathlib import Path

# ─── 1. 日期範圍計算 ────────────────────────────────────────────────────────────
tw = pytz.timezone('Asia/Taipei')
utc = pytz.utc
now = datetime.now(tw)

week_end_tw   = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
week_start_tw = (week_end_tw - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
prev_week_start_tw = week_start_tw - timedelta(days=7)
prev_week_end_tw   = week_end_tw   - timedelta(days=7)

def to_utc_str(dt):
    return dt.astimezone(utc).strftime('%Y-%m-%d %H:%M:%S')

week_start_utc      = to_utc_str(week_start_tw)
week_end_utc        = to_utc_str(week_end_tw)
prev_week_start_utc = to_utc_str(prev_week_start_tw)
prev_week_end_utc   = to_utc_str(prev_week_end_tw)

report_date_label = week_end_tw.strftime('%Y-%m-%d')
week_start_label  = week_start_tw.strftime('%Y-%m-%d')
week_end_label    = week_end_tw.strftime('%Y-%m-%d')

print(f"報告週期: {week_start_label} ～ {week_end_label}")

# ─── 2. Supabase 查詢 ──────────────────────────────────────────────────────────
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
}

SQL_TEMPLATE = """
SELECT
  id, name, ticket_created_at, ticket_completed_at, ai_summary, assignee_name,
  custom_fields->>'Product'      AS product,
  custom_fields->>'Priority'     AS priority,
  custom_fields->>'Feature'      AS feature,
  custom_fields->>'Resolve Type' AS resolve_type,
  custom_fields->>'New Feature'  AS new_feature
FROM public.task_state
WHERE name ILIKE '%Issue Ticket%'
  AND (
    (ticket_created_at  >= '{start}' AND ticket_created_at  <= '{end}')
    OR (ticket_completed_at >= '{start}' AND ticket_completed_at <= '{end}')
    OR (ticket_created_at < '{start}' AND (ticket_completed_at IS NULL OR ticket_completed_at > '{end}'))
  )
ORDER BY ticket_created_at;
"""

def run_query(start_utc, end_utc):
    sql = SQL_TEMPLATE.format(start=start_utc, end=end_utc)
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/execute_sql",
        headers=HEADERS,
        json={"query": sql}
    )
    # Supabase REST direct SQL via pg endpoint
    resp2 = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/query",
        headers=HEADERS,
        json={"sql": sql}
    )
    # fallback: use PostgREST with raw SQL via pg_dump endpoint
    # Try the standard Supabase SQL API
    resp3 = requests.post(
        f"{SUPABASE_URL}/rest/v1/",
        headers={**HEADERS, 'Prefer': 'return=representation'},
        params={
            'select': '*',
            'name': 'ilike.*Issue Ticket*'
        }
    )
    return query_via_rest(start_utc, end_utc)

def query_via_rest(start_utc, end_utc):
    """Use Supabase REST API to fetch tickets"""
    # Fetch all Issue Tickets then filter in Python
    url = f"{SUPABASE_URL}/rest/v1/task_state"
    params = {
        'select': 'id,name,ticket_created_at,ticket_completed_at,ai_summary,assignee_name,custom_fields',
        'name': 'ilike.*Issue Ticket*',
        'limit': 10000
    }
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    rows = resp.json()

    # Flatten custom_fields
    result = []
    for r in rows:
        cf = r.get('custom_fields') or {}
        result.append({
            'id': r['id'],
            'name': r['name'],
            'ticket_created_at': r.get('ticket_created_at'),
            'ticket_completed_at': r.get('ticket_completed_at'),
            'ai_summary': r.get('ai_summary'),
            'assignee_name': r.get('assignee_name'),
            'product': cf.get('Product'),
            'priority': cf.get('Priority'),
            'feature': cf.get('Feature'),
            'resolve_type': cf.get('Resolve Type'),
            'new_feature': cf.get('New Feature'),
        })
    return result

def fetch_tickets(start_utc, end_utc):
    all_rows = query_via_rest(start_utc, end_utc)
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    df['ticket_created_at']   = pd.to_datetime(df['ticket_created_at'],   utc=True, errors='coerce')
    df['ticket_completed_at'] = pd.to_datetime(df['ticket_completed_at'], utc=True, errors='coerce')

    start = pd.Timestamp(start_utc, tz='UTC')
    end   = pd.Timestamp(end_utc,   tz='UTC')

    # 篩選：本週創建 OR 本週完成 OR 跨週積壓
    mask = (
        ((df['ticket_created_at'] >= start) & (df['ticket_created_at'] <= end)) |
        ((df['ticket_completed_at'] >= start) & (df['ticket_completed_at'] <= end)) |
        ((df['ticket_created_at'] < start) & (df['ticket_completed_at'].isna() | (df['ticket_completed_at'] > end)))
    )
    return df[mask].copy()

print("查詢本週資料...")
df_cur  = fetch_tickets(week_start_utc, week_end_utc)
print(f"本週共 {len(df_cur)} 筆")
print("查詢上週資料...")
df_prev = fetch_tickets(prev_week_start_utc, prev_week_end_utc)
print(f"上週共 {len(df_prev)} 筆")

# ─── 3. 工具函式 ──────────────────────────────────────────────────────────────
def normalize_priority(p):
    if not p: return '(Empty)'
    if p.startswith('P0'): return 'P0'
    if p.startswith('P1'): return 'P1'
    if any(x in p for x in ['P2','P3','P4']): return 'P2~P4'
    return p.strip()

def wow_delta(cur, prev):
    delta = cur - prev
    if prev == 0:
        pct = None
    else:
        pct = round((delta / prev) * 100, 1)
    return {'cur': cur, 'prev': prev, 'delta': delta, 'pct': pct}

def top_features(series, n=3):
    counts = series.value_counts().head(n)
    return [{'feature': k, 'count': int(v)} for k, v in counts.items()]

def resolve_type_breakdown(df_done, df_done_prev, df_backlog, df_backlog_prev):
    """計算 Resolve Type 分析"""
    # 2.1 已完成
    done_total = len(df_done)
    done_prev_total = len(df_done_prev)

    rt_done = df_done['resolve_type'].fillna('(Empty)').value_counts()
    rt_done_prev = df_done_prev['resolve_type'].fillna('(Empty)').value_counts()

    done_breakdown = []
    for rt in sorted(set(list(rt_done.index) + list(rt_done_prev.index))):
        cur_n = int(rt_done.get(rt, 0))
        prev_n = int(rt_done_prev.get(rt, 0))
        pct = round(cur_n / done_total * 100, 1) if done_total > 0 else 0
        feat_cross = df_done[df_done['resolve_type'].fillna('(Empty)') == rt]['feature'].value_counts().head(5)
        done_breakdown.append({
            'resolve_type': rt,
            'count': cur_n,
            'pct': pct,
            'wow': wow_delta(cur_n, prev_n),
            'flag_dominant': pct > 30,
            'features': [{'name': k, 'count': int(v)} for k, v in feat_cross.items()]
        })

    # 2.2 積壓
    bl_total = len(df_backlog)
    bl_prev_total = len(df_backlog_prev)

    rt_bl = df_backlog['resolve_type'].fillna('(Empty)').value_counts()
    rt_bl_prev = df_backlog_prev['resolve_type'].fillna('(Empty)').value_counts()

    backlog_breakdown = []
    for rt in sorted(set(list(rt_bl.index) + list(rt_bl_prev.index))):
        cur_n = int(rt_bl.get(rt, 0))
        prev_n = int(rt_bl_prev.get(rt, 0))
        pct = round(cur_n / bl_total * 100, 1) if bl_total > 0 else 0
        feat_cross = df_backlog[df_backlog['resolve_type'].fillna('(Empty)') == rt]['feature'].value_counts().head(5)
        backlog_breakdown.append({
            'resolve_type': rt,
            'count': cur_n,
            'pct': pct,
            'wow': wow_delta(cur_n, prev_n),
            'flag_empty_risk': rt == '(Empty)' and pct > 20,
            'features': [{'name': k, 'count': int(v)} for k, v in feat_cross.items()]
        })

    top3_backlog_features = top_features(df_backlog['feature'].fillna('(Empty)'))

    return {
        'done': {'total': done_total, 'breakdown': done_breakdown},
        'backlog': {'total': bl_total, 'breakdown': backlog_breakdown, 'top3_features': top3_backlog_features}
    }

# ─── 4. 每個產品分析 ──────────────────────────────────────────────────────────
PRODUCTS = ['MAAC', 'CAAC', 'DAAC']
products_data = {}

for product in PRODUCTS:
    print(f"\n分析 {product}...")

    # 本週 / 上週 各自的 product filter
    cur  = df_cur[df_cur['product'] == product].copy()  if not df_cur.empty  else pd.DataFrame()
    prev = df_prev[df_prev['product'] == product].copy() if not df_prev.empty else pd.DataFrame()

    if cur.empty:
        products_data[product] = {'empty': True, 'product': product}
        continue

    week_start = pd.Timestamp(week_start_utc, tz='UTC')
    week_end   = pd.Timestamp(week_end_utc,   tz='UTC')
    prev_start = pd.Timestamp(prev_week_start_utc, tz='UTC')
    prev_end   = pd.Timestamp(prev_week_end_utc,   tz='UTC')

    # 分類：新增、完成、積壓
    cur_created  = cur[cur['ticket_created_at'].between(week_start, week_end)]
    cur_completed = cur[cur['ticket_completed_at'].between(week_start, week_end)]
    cur_backlog   = cur[cur['ticket_completed_at'].isna() | (cur['ticket_completed_at'] > week_end)]

    prev_created   = prev[prev['ticket_created_at'].between(prev_start, prev_end)]   if not prev.empty else pd.DataFrame()
    prev_completed = prev[prev['ticket_completed_at'].between(prev_start, prev_end)] if not prev.empty else pd.DataFrame()
    prev_backlog   = prev[prev['ticket_completed_at'].isna() | (prev['ticket_completed_at'] > prev_end)] if not prev.empty else pd.DataFrame()

    # 1. 每週概覽
    overview = {
        'created':   wow_delta(len(cur_created),   len(prev_created)),
        'completed': wow_delta(len(cur_completed), len(prev_completed)),
        'backlog':   wow_delta(len(cur_backlog),   len(prev_backlog)),
    }

    # 2. Resolve Type 分析
    resolve_analysis = resolve_type_breakdown(cur_completed, prev_completed, cur_backlog, prev_backlog)

    # 3. 優先級分佈（本週新增）
    cur_created_p = cur_created.copy()
    cur_created_p['priority_norm'] = cur_created_p['priority'].apply(normalize_priority)
    prev_created_p = prev_created.copy()
    if not prev_created_p.empty:
        prev_created_p['priority_norm'] = prev_created_p['priority'].apply(normalize_priority)

    priority_labels = ['P0', 'P1', 'P2~P4', '(Empty)']
    priority_dist = []
    total_created = len(cur_created_p)

    p1_backlog_cur  = len(cur_backlog[cur_backlog['priority'].apply(normalize_priority) == 'P1'])
    p1_backlog_prev = len(prev_backlog[prev_backlog['priority'].apply(normalize_priority) == 'P1']) if not prev_backlog.empty else 0

    for p in priority_labels:
        cur_n  = int((cur_created_p['priority_norm'] == p).sum())
        prev_n = int((prev_created_p['priority_norm'] == p).sum()) if not prev_created_p.empty else 0
        pct    = round(cur_n / total_created * 100, 1) if total_created > 0 else 0
        high_p_features = []
        if p in ['P0', 'P1']:
            feat_cross = cur_created_p[cur_created_p['priority_norm'] == p]['feature'].value_counts().head(5)
            high_p_features = [{'name': k, 'count': int(v)} for k, v in feat_cross.items()]
        priority_dist.append({
            'priority': p,
            'count': cur_n,
            'pct': pct,
            'wow': wow_delta(cur_n, prev_n),
            'features': high_p_features,
            'flag_p1_backlog_increase': p == 'P1' and p1_backlog_cur > p1_backlog_prev
        })

    # 4. 新功能影響（本週已完成）
    nf_map = cur_completed['new_feature'].fillna('(Empty)')
    nf_prev_map = prev_completed['new_feature'].fillna('(Empty)') if not prev_completed.empty else pd.Series([], dtype=str)
    nf_total = len(cur_completed)

    nf_dist = []
    for val in ['Yes', 'No', '(Empty)']:
        cur_n  = int((nf_map == val).sum())
        prev_n = int((nf_prev_map == val).sum()) if not nf_prev_map.empty else 0
        pct    = round(cur_n / nf_total * 100, 1) if nf_total > 0 else 0
        detail = []
        if val == 'Yes' and cur_n > 0:
            yes_df = cur_completed[cur_completed['new_feature'] == 'Yes'].copy()
            yes_df['priority_norm'] = yes_df['priority'].apply(normalize_priority)
            by_p = yes_df['priority_norm'].value_counts()
            by_f = yes_df['feature'].value_counts().head(5)
            detail = {
                'by_priority': [{'priority': k, 'count': int(v)} for k, v in by_p.items()],
                'by_feature':  [{'feature': k, 'count': int(v)} for k, v in by_f.items()],
                'release_risk': any(yes_df['priority_norm'].isin(['P0', 'P1']))
            }
        nf_dist.append({'value': val, 'count': cur_n, 'pct': pct, 'wow': wow_delta(cur_n, prev_n), 'detail': detail})

    # 5. Feature 熱點
    feat_created  = cur_created['feature'].value_counts()
    feat_completed = cur_completed['feature'].value_counts()
    feat_backlog  = cur_backlog['feature'].value_counts()
    feat_prev_created = prev_created['feature'].value_counts() if not prev_created.empty else pd.Series([], dtype=int)

    hot_feature_created = []
    for f, cnt in feat_created.head(5).items():
        prev_cnt = int(feat_prev_created.get(f, 0))
        done_cnt = int(feat_completed.get(f, 0))
        bl_cnt   = int(feat_backlog.get(f, 0))
        hot_feature_created.append({
            'feature': f, 'count': int(cnt),
            'wow': wow_delta(int(cnt), prev_cnt),
            'gap': int(cnt) - done_cnt,
            'backlog': bl_cnt
        })

    hot_feature_completed = [{'feature': f, 'count': int(c)} for f, c in feat_completed.head(5).items()]

    bl_total_cnt = len(cur_backlog)
    backlog_concentration = []
    for f, cnt in feat_backlog.head(3).items():
        pct = round(cnt / bl_total_cnt * 100, 1) if bl_total_cnt > 0 else 0
        backlog_concentration.append({'feature': f, 'count': int(cnt), 'pct': pct})

    # 多重風險 Feature 標記
    high_p_features_set = set(cur_created_p[cur_created_p['priority_norm'].isin(['P0','P1'])]['feature'].dropna())
    backlog_features_set = set(feat_backlog.head(5).index)
    nf_yes_df = cur_completed[cur_completed['new_feature'] == 'Yes']
    nf_features_set = set(nf_yes_df['feature'].dropna())
    triple_risk = list(high_p_features_set & backlog_features_set & nf_features_set)

    feature_hotspot = {
        'top_created': hot_feature_created,
        'top_completed': hot_feature_completed,
        'backlog_concentration': backlog_concentration,
        'triple_risk_features': triple_risk
    }

    # 資料品質
    invalid_dates = int(
        (cur['ticket_completed_at'].notna() &
         (cur['ticket_completed_at'] < cur['ticket_created_at'])).sum()
    )
    total_cur = len(cur)
    dq = {
        'invalid_dates': invalid_dates,
        'null_priority_pct': round(cur['priority'].isna().sum() / total_cur * 100, 1) if total_cur > 0 else 0,
        'null_feature_pct':  round(cur['feature'].isna().sum()  / total_cur * 100, 1) if total_cur > 0 else 0,
        'null_resolve_type_pct': round(cur['resolve_type'].isna().sum() / total_cur * 100, 1) if total_cur > 0 else 0,
        'null_new_feature_pct': round(cur['new_feature'].isna().sum()  / total_cur * 100, 1) if total_cur > 0 else 0,
    }

    # 結構性風險旗標
    empty_rt_pct = next((b['pct'] for b in resolve_analysis['backlog']['breakdown'] if b['resolve_type'] == '(Empty)'), 0)
    top_bl_pct   = backlog_concentration[0]['pct'] if backlog_concentration else 0
    p1_bl_increased = p1_backlog_cur > p1_backlog_prev
    consecutive_more_created = (
        overview['created']['cur'] > overview['completed']['cur'] and
        overview['created']['prev'] > overview['completed']['prev']
    )

    risk_flags = {
        'empty_resolve_type': empty_rt_pct > 20,
        'single_feature_backlog': top_bl_pct > 30,
        'p1_backlog_increase': p1_bl_increased,
        'consecutive_created_gt_completed': consecutive_more_created,
        'any': any([empty_rt_pct > 20, top_bl_pct > 30, p1_bl_increased, consecutive_more_created])
    }

    products_data[product] = {
        'empty': False,
        'product': product,
        'overview': overview,
        'resolve_analysis': resolve_analysis,
        'priority_distribution': priority_dist,
        'new_feature_impact': nf_dist,
        'feature_hotspot': feature_hotspot,
        'data_quality': dq,
        'risk_flags': risk_flags,
    }

    print(f"  ✅ {product}: 新增={overview['created']['cur']}, 完成={overview['completed']['cur']}, 積壓={overview['backlog']['cur']}")

# ─── 5. 輸出 JSON ─────────────────────────────────────────────────────────────
output = {
    'generated_at': datetime.now(tw).isoformat(),
    'report_date': report_date_label,
    'week_start': week_start_label,
    'week_end': week_end_label,
    'products': products_data
}

output_dir = Path('docs/data')
output_dir.mkdir(parents=True, exist_ok=True)

# latest.json
latest_path = output_dir / 'latest.json'
with open(latest_path, 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)
print(f"\n✅ 寫入 {latest_path}")

# history.json — 累積歷史
history_path = output_dir / 'history.json'
if history_path.exists():
    with open(history_path, 'r', encoding='utf-8') as f:
        history = json.load(f)
else:
    history = []

# 避免同一週重複寫入
existing_dates = {h['report_date'] for h in history}
if report_date_label not in existing_dates:
    history.insert(0, output)
    history = history[:26]  # 保留最近 26 週
    with open(history_path, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"✅ 更新 {history_path}（共 {len(history)} 週記錄）")
else:
    print(f"⚠️  本週 ({report_date_label}) 已存在於 history，略過")

print("\n🎉 完成！")
