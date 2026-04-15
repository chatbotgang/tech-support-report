"""
generate_report.py
每週一執行：查詢 Supabase → 計算三個產品數據 → AI Insight → 輸出 JSON 至 docs/data/
"""

import os
import json
import requests
import pandas as pd
import pytz
import re
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
    return dt.astimezone(utc).strftime('%Y-%m-%dT%H:%M:%S')

week_start_utc      = to_utc_str(week_start_tw)
week_end_utc        = to_utc_str(week_end_tw)
prev_week_start_utc = to_utc_str(prev_week_start_tw)
prev_week_end_utc   = to_utc_str(prev_week_end_tw)

report_date_label = week_end_tw.strftime('%Y-%m-%d')
week_start_label  = week_start_tw.strftime('%Y-%m-%d')
week_end_label    = week_end_tw.strftime('%Y-%m-%d')

print(f"報告週期: {week_start_label} ～ {week_end_label}")
print(f"UTC 範圍: {week_start_utc} ～ {week_end_utc}")

# ─── 2. Supabase 查詢 ──────────────────────────────────────────────────────────
SUPABASE_URL = os.environ['SUPABASE_URL'].rstrip('/')
SUPABASE_KEY = os.environ['SUPABASE_SERVICE_KEY']
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'count=exact',
}

def fetch_all_tickets():
    url = f"{SUPABASE_URL}/rest/v1/task_state"
    all_rows = []
    offset = 0
    limit = 1000

    while True:
        params = {
            'select': 'id,name,ticket_created_at,ticket_completed_at,assignee_name,custom_fields,ai_summary',
            'name': 'ilike.%Issue Ticket%',
            'limit': limit,
            'offset': offset,
            'order': 'ticket_created_at.asc',
        }
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < limit:
            break
        offset += limit

    print(f"  Supabase 總共回傳: {len(all_rows)} 筆")

    result = []
    for r in all_rows:
        cf = r.get('custom_fields') or {}
        if isinstance(cf, str):
            try:
                cf = json.loads(cf)
            except:
                cf = {}

        name = r.get('name', '')
        product_from_name = None
        m = re.search(r'\[(MAAC|CAAC|DAAC)[/\]]', name)
        if m:
            product_from_name = m.group(1)

        product = cf.get('Product') or product_from_name

        result.append({
            'id': r['id'],
            'name': name,
            'ticket_created_at': r.get('ticket_created_at'),
            'ticket_completed_at': r.get('ticket_completed_at'),
            'assignee_name': r.get('assignee_name'),
            'ai_summary': r.get('ai_summary') or '',
            'product': product,
            'priority': cf.get('Priority'),
            'feature': cf.get('Feature'),
            'resolve_type': cf.get('Resolve Type'),
            'new_feature': cf.get('New Feature'),
        })
    return result

def filter_by_period(df, start_utc_str, end_utc_str):
    if df.empty:
        return df
    start = pd.Timestamp(start_utc_str, tz='UTC')
    end   = pd.Timestamp(end_utc_str,   tz='UTC')
    created   = df['ticket_created_at']
    completed = df['ticket_completed_at']
    mask = (
        ((created >= start) & (created <= end)) |
        ((completed >= start) & (completed <= end)) |
        ((created < start) & (completed.isna() | (completed > end)))
    )
    return df[mask].copy()

# ─── 3. AI Insight 產生 ────────────────────────────────────────────────────────
def generate_ai_insight(product, overview, risk_flags, cur_df, week_start_label, week_end_label):
    """呼叫 Anthropic API，根據 ai_summary 和統計數據產生管理層洞察"""
    if not ANTHROPIC_KEY:
        print(f"  ⚠️  無 ANTHROPIC_API_KEY，跳過 AI Insight")
        return None

    # 收集本週所有票的 ai_summary（最多 60 筆避免 token 超限）
    summaries = cur_df[cur_df['ai_summary'].str.strip() != '']['ai_summary'].tolist()[:60]
    if not summaries:
        return None

    summaries_text = '\n---\n'.join(summaries)

    # 統計數字摘要
    stats = f"""
產品：{product}
報告週期：{week_start_label} ～ {week_end_label}
本週新增工單：{overview['created']['cur']}（WoW {'+' if overview['created']['delta'] >= 0 else ''}{overview['created']['delta']}）
本週完成工單：{overview['completed']['cur']}（WoW {'+' if overview['completed']['delta'] >= 0 else ''}{overview['completed']['delta']}）
積壓工單：{overview['backlog']['cur']}（WoW {'+' if overview['backlog']['delta'] >= 0 else ''}{overview['backlog']['delta']}）
結構性風險：{'是' if risk_flags['any'] else '否'}
- 空值 Resolve Type > 20%：{'是' if risk_flags['empty_resolve_type'] else '否'}
- 單一 Feature 積壓 > 30%：{'是' if risk_flags['single_feature_backlog'] else '否'}
- P1 積壓週環比增加：{'是' if risk_flags['p1_backlog_increase'] else '否'}
- 連續兩週新增 > 完成：{'是' if risk_flags['consecutive_created_gt_completed'] else '否'}
"""

    prompt = f"""你是一位資深的 SaaS 產品支援分析師。以下是 {product} 產品本週的支援工單統計與各工單的 AI 摘要。

## 統計數據
{stats}

## 本週工單 AI 摘要（共 {len(summaries)} 筆）
{summaries_text}

請根據以上資料，用**繁體中文**撰寫管理層洞察報告，必須包含以下 7 個面向，每個面向 2-3 句話，具體且可執行：

1. **產品穩定性風險**：本週最常見的問題類型與嚴重程度
2. **支援工作量結構**：工作量分布特徵，哪些 Feature 佔用最多資源
3. **發版品質影響**：新功能相關工單對支援負擔的影響
4. **積壓健康狀況**：積壓趨勢與潛在風險
5. **升級風險區域**：哪些問題最可能升級為客戶流失風險
6. **新興 Feature 風險**：本週出現的新問題模式或 Feature 風險
7. **立即建議行動**：3-5 條具體可執行的建議（用條列式）

請直接輸出 JSON 格式，不要有任何前言或 markdown：
{{
  "stability_risk": "...",
  "workload_structure": "...",
  "release_quality": "...",
  "backlog_health": "...",
  "escalation_risk": "...",
  "emerging_risk": "...",
  "action_items": ["建議1", "建議2", "建議3"]
}}"""

    try:
        resp = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': ANTHROPIC_KEY,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': 'claude-sonnet-4-5',
                'max_tokens': 1500,
                'messages': [{'role': 'user', 'content': prompt}]
            },
            timeout=60
        )
        resp.raise_for_status()
      content = resp.json()['content'][0]['text'].strip()
        # 清理 markdown
        content = re.sub(r'^```json\s*', '', content)
        content = re.sub(r'^```\s*', '', content)
        content = re.sub(r'\s*```$', '', content)
        # 只取第一個 { 到最後一個 } 之間的內容
        start = content.find('{')
        end = content.rfind('}')
        if start != -1 and end != -1:
            content = content[start:end+1]
        insight = json.loads(content)
        print(f"  ✅ AI Insight 產生完成")
        return insight
    except Exception as e:
        print(f"  ⚠️  AI Insight 失敗: {e}")
        return None

# ─── 4. 取得資料 ─────────────────────────────────────────────────────────────
print("查詢 Supabase 資料...")
all_rows = fetch_all_tickets()

if not all_rows:
    print("⚠️  無資料，請確認 Supabase 連線")
    output = {
        'generated_at': datetime.now(tw).isoformat(),
        'report_date': report_date_label,
        'week_start': week_start_label,
        'week_end': week_end_label,
        'products': {p: {'empty': True, 'product': p} for p in ['MAAC','CAAC','DAAC']}
    }
    output_dir = Path('docs/data')
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / 'latest.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    exit(0)

df_all = pd.DataFrame(all_rows)
df_all['ticket_created_at']   = pd.to_datetime(df_all['ticket_created_at'],   utc=True, errors='coerce')
df_all['ticket_completed_at'] = pd.to_datetime(df_all['ticket_completed_at'], utc=True, errors='coerce')

print(f"  Product 分佈: {df_all['product'].value_counts().to_dict()}")
print(f"  日期範圍: {df_all['ticket_created_at'].min()} ～ {df_all['ticket_created_at'].max()}")

print(f"本週篩選中...")
df_cur  = filter_by_period(df_all, week_start_utc, week_end_utc)
print(f"本週共 {len(df_cur)} 筆")

print(f"上週篩選中...")
df_prev = filter_by_period(df_all, prev_week_start_utc, prev_week_end_utc)
print(f"上週共 {len(df_prev)} 筆")

# ─── 5. 工具函式 ─────────────────────────────────────────────────────────────
def normalize_priority(p):
    if not p: return '(Empty)'
    p = str(p).strip()
    if 'P0' in p: return 'P0'
    if 'P1' in p: return 'P1'
    if any(x in p for x in ['P2','P3','P4']): return 'P2~P4'
    return p

def wow_delta(cur, prev):
    delta = cur - prev
    pct = round((delta / prev) * 100, 1) if prev != 0 else None
    return {'cur': cur, 'prev': prev, 'delta': delta, 'pct': pct}

def top_features(series, n=3):
    counts = series.value_counts().head(n)
    return [{'feature': str(k), 'count': int(v)} for k, v in counts.items()]

# ─── 6. 每個產品分析 ─────────────────────────────────────────────────────────
PRODUCTS = ['MAAC', 'CAAC', 'DAAC']
products_data = {}

week_start = pd.Timestamp(week_start_utc, tz='UTC')
week_end   = pd.Timestamp(week_end_utc,   tz='UTC')
prev_start = pd.Timestamp(prev_week_start_utc, tz='UTC')
prev_end   = pd.Timestamp(prev_week_end_utc,   tz='UTC')

for product in PRODUCTS:
    print(f"\n分析 {product}...")

    cur  = df_cur[df_cur['product'] == product].copy()  if not df_cur.empty  else pd.DataFrame()
    prev = df_prev[df_prev['product'] == product].copy() if not df_prev.empty else pd.DataFrame()

    if cur.empty:
        print(f"  {product}: 無資料")
        products_data[product] = {'empty': True, 'product': product}
        continue

    cur_created   = cur[cur['ticket_created_at'].between(week_start, week_end)]
    cur_completed = cur[cur['ticket_completed_at'].notna() & cur['ticket_completed_at'].between(week_start, week_end)]
    cur_backlog   = cur[cur['ticket_completed_at'].isna() | (cur['ticket_completed_at'] > week_end)]

    prev_created   = prev[prev['ticket_created_at'].between(prev_start, prev_end)]   if not prev.empty else pd.DataFrame()
    prev_completed = prev[prev['ticket_completed_at'].notna() & prev['ticket_completed_at'].between(prev_start, prev_end)] if not prev.empty else pd.DataFrame()
    prev_backlog   = prev[prev['ticket_completed_at'].isna() | (prev['ticket_completed_at'] > prev_end)] if not prev.empty else pd.DataFrame()

    overview = {
        'created':   wow_delta(len(cur_created),   len(prev_created)),
        'completed': wow_delta(len(cur_completed), len(prev_completed)),
        'backlog':   wow_delta(len(cur_backlog),   len(prev_backlog)),
    }

    def resolve_breakdown(done_df, done_prev_df, bl_df, bl_prev_df):
        done_total = len(done_df)
        rt_done = done_df['resolve_type'].fillna('(Empty)').value_counts()
        rt_done_prev = done_prev_df['resolve_type'].fillna('(Empty)').value_counts() if not done_prev_df.empty else pd.Series(dtype=int)
        done_bd = []
        for rt in sorted(set(list(rt_done.index) + list(rt_done_prev.index))):
            cur_n = int(rt_done.get(rt, 0))
            prev_n = int(rt_done_prev.get(rt, 0))
            pct = round(cur_n / done_total * 100, 1) if done_total > 0 else 0
            feat = done_df[done_df['resolve_type'].fillna('(Empty)') == rt]['feature'].value_counts().head(5)
            done_bd.append({'resolve_type': rt, 'count': cur_n, 'pct': pct, 'wow': wow_delta(cur_n, prev_n), 'flag_dominant': pct > 30, 'features': [{'name': str(k), 'count': int(v)} for k, v in feat.items()]})

        bl_total = len(bl_df)
        rt_bl = bl_df['resolve_type'].fillna('(Empty)').value_counts()
        rt_bl_prev = bl_prev_df['resolve_type'].fillna('(Empty)').value_counts() if not bl_prev_df.empty else pd.Series(dtype=int)
        bl_bd = []
        for rt in sorted(set(list(rt_bl.index) + list(rt_bl_prev.index))):
            cur_n = int(rt_bl.get(rt, 0))
            prev_n = int(rt_bl_prev.get(rt, 0))
            pct = round(cur_n / bl_total * 100, 1) if bl_total > 0 else 0
            feat = bl_df[bl_df['resolve_type'].fillna('(Empty)') == rt]['feature'].value_counts().head(5)
            bl_bd.append({'resolve_type': rt, 'count': cur_n, 'pct': pct, 'wow': wow_delta(cur_n, prev_n), 'flag_empty_risk': rt == '(Empty)' and pct > 20, 'features': [{'name': str(k), 'count': int(v)} for k, v in feat.items()]})

        return {'done': {'total': done_total, 'breakdown': done_bd}, 'backlog': {'total': bl_total, 'breakdown': bl_bd, 'top3_features': top_features(bl_df['feature'].fillna('(Empty)'))}}

    resolve_analysis = resolve_breakdown(cur_completed, prev_completed, cur_backlog, prev_backlog)

    cur_cp = cur_created.copy()
    cur_cp['priority_norm'] = cur_cp['priority'].apply(normalize_priority)
    prev_cp = prev_created.copy()
    if not prev_cp.empty:
        prev_cp['priority_norm'] = prev_cp['priority'].apply(normalize_priority)

    p1_bl_cur  = len(cur_backlog[cur_backlog['priority'].apply(normalize_priority) == 'P1'])
    p1_bl_prev = len(prev_backlog[prev_backlog['priority'].apply(normalize_priority) == 'P1']) if not prev_backlog.empty else 0
    total_created = len(cur_cp)
    priority_dist = []
    for p in ['P0','P1','P2~P4','(Empty)']:
        cur_n  = int((cur_cp['priority_norm'] == p).sum())
        prev_n = int((prev_cp['priority_norm'] == p).sum()) if not prev_cp.empty else 0
        pct    = round(cur_n / total_created * 100, 1) if total_created > 0 else 0
        hp_feat = []
        if p in ['P0','P1']:
            f = cur_cp[cur_cp['priority_norm'] == p]['feature'].value_counts().head(5)
            hp_feat = [{'name': str(k), 'count': int(v)} for k, v in f.items()]
        priority_dist.append({'priority': p, 'count': cur_n, 'pct': pct, 'wow': wow_delta(cur_n, prev_n), 'features': hp_feat, 'flag_p1_backlog_increase': p == 'P1' and p1_bl_cur > p1_bl_prev})

    nf_map = cur_completed['new_feature'].fillna('(Empty)')
    nf_prev = prev_completed['new_feature'].fillna('(Empty)') if not prev_completed.empty else pd.Series(dtype=str)
    nf_total = len(cur_completed)
    nf_dist = []
    for val in ['Yes','No','(Empty)']:
        cur_n  = int((nf_map == val).sum())
        prev_n = int((nf_prev == val).sum()) if not nf_prev.empty else 0
        pct    = round(cur_n / nf_total * 100, 1) if nf_total > 0 else 0
        detail = []
        if val == 'Yes' and cur_n > 0:
            yes_df = cur_completed[cur_completed['new_feature'] == 'Yes'].copy()
            yes_df['priority_norm'] = yes_df['priority'].apply(normalize_priority)
            by_p = yes_df['priority_norm'].value_counts()
            by_f = yes_df['feature'].value_counts().head(5)
            detail = {'by_priority': [{'priority': k, 'count': int(v)} for k, v in by_p.items()], 'by_feature': [{'feature': str(k), 'count': int(v)} for k, v in by_f.items()], 'release_risk': any(yes_df['priority_norm'].isin(['P0','P1']))}
        nf_dist.append({'value': val, 'count': cur_n, 'pct': pct, 'wow': wow_delta(cur_n, prev_n), 'detail': detail})

    feat_created   = cur_created['feature'].value_counts()
    feat_completed = cur_completed['feature'].value_counts()
    feat_backlog   = cur_backlog['feature'].value_counts()
    feat_prev_cr   = prev_created['feature'].value_counts() if not prev_created.empty else pd.Series(dtype=int)

    top_cr = []
    for f, cnt in feat_created.head(5).items():
        top_cr.append({'feature': str(f), 'count': int(cnt), 'wow': wow_delta(int(cnt), int(feat_prev_cr.get(f, 0))), 'gap': int(cnt) - int(feat_completed.get(f, 0)), 'backlog': int(feat_backlog.get(f, 0))})

    bl_total_cnt = len(cur_backlog)
    bl_conc = []
    for f, cnt in feat_backlog.head(3).items():
        pct = round(cnt / bl_total_cnt * 100, 1) if bl_total_cnt > 0 else 0
        bl_conc.append({'feature': str(f), 'count': int(cnt), 'pct': pct})

    hp_set = set(cur_cp[cur_cp['priority_norm'].isin(['P0','P1'])]['feature'].dropna().astype(str))
    bl_set = set(str(f) for f in feat_backlog.head(5).index)
    nf_set = set(cur_completed[cur_completed['new_feature']=='Yes']['feature'].dropna().astype(str))

    feature_hotspot = {'top_created': top_cr, 'top_completed': [{'feature': str(f), 'count': int(c)} for f, c in feat_completed.head(5).items()], 'backlog_concentration': bl_conc, 'triple_risk_features': list(hp_set & bl_set & nf_set)}

    total_cur = len(cur)
    invalid_dates = int((cur['ticket_completed_at'].notna() & (cur['ticket_completed_at'] < cur['ticket_created_at'])).sum())
    dq = {'invalid_dates': invalid_dates, 'null_priority_pct': round(cur['priority'].isna().sum() / total_cur * 100, 1) if total_cur > 0 else 0, 'null_feature_pct': round(cur['feature'].isna().sum() / total_cur * 100, 1) if total_cur > 0 else 0, 'null_resolve_type_pct': round(cur['resolve_type'].isna().sum() / total_cur * 100, 1) if total_cur > 0 else 0, 'null_new_feature_pct': round(cur['new_feature'].isna().sum() / total_cur * 100, 1) if total_cur > 0 else 0}

    empty_rt_pct = next((b['pct'] for b in resolve_analysis['backlog']['breakdown'] if b['resolve_type'] == '(Empty)'), 0)
    top_bl_pct   = bl_conc[0]['pct'] if bl_conc else 0
    p1_bl_up     = p1_bl_cur > p1_bl_prev
    consec       = overview['created']['cur'] > overview['completed']['cur'] and overview['created']['prev'] > overview['completed']['prev']

    risk_flags = {'empty_resolve_type': empty_rt_pct > 20, 'single_feature_backlog': top_bl_pct > 30, 'p1_backlog_increase': p1_bl_up, 'consecutive_created_gt_completed': consec, 'any': any([empty_rt_pct > 20, top_bl_pct > 30, p1_bl_up, consec])}

    # AI Insight
    print(f"  產生 AI Insight...")
    ai_insight = generate_ai_insight(product, overview, risk_flags, cur, week_start_label, week_end_label)

    products_data[product] = {
        'empty': False, 'product': product,
        'overview': overview,
        'resolve_analysis': resolve_analysis,
        'priority_distribution': priority_dist,
        'new_feature_impact': nf_dist,
        'feature_hotspot': feature_hotspot,
        'data_quality': dq,
        'risk_flags': risk_flags,
        'ai_insight': ai_insight,
    }
    print(f"  ✅ {product}: 新增={overview['created']['cur']}, 完成={overview['completed']['cur']}, 積壓={overview['backlog']['cur']}")

# ─── 7. 輸出 JSON ─────────────────────────────────────────────────────────────
output = {'generated_at': datetime.now(tw).isoformat(), 'report_date': report_date_label, 'week_start': week_start_label, 'week_end': week_end_label, 'products': products_data}

output_dir = Path('docs/data')
output_dir.mkdir(parents=True, exist_ok=True)

with open(output_dir / 'latest.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)
print(f"\n✅ 寫入 docs/data/latest.json")

history_path = output_dir / 'history.json'
history = []
if history_path.exists():
    with open(history_path, 'r', encoding='utf-8') as f:
        history = json.load(f)

existing_dates = {h['report_date'] for h in history}
if report_date_label not in existing_dates:
    history.insert(0, output)
else:
    for i, h in enumerate(history):
        if h['report_date'] == report_date_label:
            history[i] = output
            break

history = history[:26]
with open(history_path, 'w', encoding='utf-8') as f:
    json.dump(history, f, ensure_ascii=False, indent=2)
print(f"✅ 更新 history.json（共 {len(history)} 週）")
print("\n🎉 完成！")
