"""
generate_report.py
每週一執行：查詢 Supabase → 計算三個產品數據 → AI Insight → 輸出 JSON 至 docs/data/
"""

import os, json, requests, re
import pandas as pd
import pytz
from datetime import datetime, timedelta
from pathlib import Path

# ─── 1. 日期範圍 ───────────────────────────────────────────────────────────────
tw  = pytz.timezone('Asia/Taipei')
utc = pytz.utc
now = datetime.now(tw)

# 找最近一個完整結束的週日（週一跑時是上週日，週三跑時也是上週日）
days_since_monday = now.weekday()
last_monday = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
week_end_tw    = last_monday - timedelta(days=7)   # 04/13 00:00 台灣（篩選截止點）
week_start_tw  = week_end_tw - timedelta(days=7)   # 04/06 00:00 台灣
prev_week_end_tw   = week_start_tw                 # 04/06 00:00
prev_week_start_tw = prev_week_end_tw - timedelta(days=7)  # 03/30 00:00

def to_utc(dt):
    return dt.astimezone(utc).strftime('%Y-%m-%dT%H:%M:%S')

week_start_utc      = to_utc(week_start_tw)
week_end_utc        = to_utc(week_end_tw)
prev_week_start_utc = to_utc(prev_week_start_tw)
prev_week_end_utc   = to_utc(prev_week_end_tw)
week_start_label  = week_start_tw.strftime('%Y-%m-%d')                      # 04/06
week_end_label    = (week_end_tw - timedelta(days=1)).strftime('%Y-%m-%d')  # 04/12
report_date_label = week_end_label

print(f"報告週期: {week_start_label} ~ {week_end_label}")

# ─── 2. 設定 ───────────────────────────────────────────────────────────────────
SUPABASE_URL  = os.environ['SUPABASE_URL'].rstrip('/')
SUPABASE_KEY  = os.environ['SUPABASE_SERVICE_KEY']
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

SB_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'count=exact',
}

# ─── 3. Supabase 查詢 ──────────────────────────────────────────────────────────
def fetch_all_tickets():
    url = f"{SUPABASE_URL}/rest/v1/task_state"
    all_rows = []
    offset = 0
    while True:
        params = {
            'select': 'id,name,ticket_created_at,ticket_completed_at,assignee_name,custom_fields,ai_summary',
            'name': 'ilike.%Issue Ticket%',
            'limit': 1000, 'offset': offset,
            'order': 'ticket_created_at.asc',
        }
        resp = requests.get(url, headers=SB_HEADERS, params=params)
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000

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
        m = re.search(r'\[(MAAC|CAAC|DAAC)[/\]]', name)
        product = cf.get('Product') or (m.group(1) if m else None)
        result.append({
            'id': r['id'], 'name': name,
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

def filter_period(df, s, e):
    if df.empty:
        return df
    start = pd.Timestamp(s, tz='UTC')
    end   = pd.Timestamp(e, tz='UTC')
    c = df['ticket_created_at']
    d = df['ticket_completed_at']
    mask = (
        ((c >= start) & (c <= end)) |
        ((d >= start) & (d <= end)) |
        ((c < start) & (d.isna() | (d > end)))
    )
    return df[mask].copy()

# ─── 4. AI Insight ─────────────────────────────────────────────────────────────
def generate_ai_insight(product, overview, risk_flags, cur_df, ws, we):
    if not ANTHROPIC_KEY:
        print("  skip AI Insight: no key")
        return None
    summaries = [s[:300] for s in cur_df['ai_summary'].tolist() if s and s.strip()][:40]
    if not summaries:
        print("  skip AI Insight: no summaries")
        return None

    stats = (
        f"產品:{product} 週期:{ws}~{we} "
        f"新增:{overview['created']['cur']} 完成:{overview['completed']['cur']} "
        f"積壓:{overview['backlog']['cur']}"
    )
    summaries_text = '\n---\n'.join(summaries)
    prompt = (
        f"你是資深SaaS支援分析師。以下是{product}本週工單統計與AI摘要。\n"
        f"統計:{stats}\n摘要({len(summaries)}筆):\n{summaries_text}\n\n"
        "請用繁體中文撰寫管理層洞察。"
        "只輸出JSON物件，不要任何說明或markdown，格式如下:\n"
        '{"stability_risk":"...","workload_structure":"...","release_quality":"...",'
        '"backlog_health":"...","escalation_risk":"...","emerging_risk":"...",'
        '"action_items":["建議1","建議2","建議3"]}'
    )
    try:
        resp = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': ANTHROPIC_KEY,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': 'claude-haiku-4-5-20251001',
                'max_tokens': 1500,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=60
        )
        resp.raise_for_status()
        raw = resp.json()['content'][0]['text'].strip()
        start = raw.find('{')
        end   = raw.rfind('}')
        if start == -1 or end == -1:
            raise ValueError("no JSON object found")
        insight = json.loads(raw[start:end+1])
        print("  AI Insight OK")
        return insight
    except Exception as e:
        print(f"  AI Insight failed: {e}")
        return None

# ─── 5. 分析工具 ───────────────────────────────────────────────────────────────
def norm_pri(p):
    if not p: return '(Empty)'
    p = str(p)
    if 'P0' in p: return 'P0'
    if 'P1' in p: return 'P1'
    if any(x in p for x in ['P2','P3','P4']): return 'P2~P4'
    return p.strip()

def wow(a, b):
    d = a - b
    return {'cur':a,'prev':b,'delta':d,'pct':round(d/b*100,1) if b else None}

def top_feat(series, n=3):
    return [{'feature':str(k),'count':int(v)} for k,v in series.value_counts().head(n).items()]

# ─── 6. 主流程 ─────────────────────────────────────────────────────────────────
print("查詢資料...")
all_rows = fetch_all_tickets()
if not all_rows:
    print("no data")
    out = {'generated_at':datetime.now(tw).isoformat(),'report_date':report_date_label,
           'week_start':week_start_label,'week_end':week_end_label,
           'products':{p:{'empty':True,'product':p} for p in ['MAAC','CAAC','DAAC']}}
    Path('docs/data').mkdir(parents=True, exist_ok=True)
    with open('docs/data/latest.json','w',encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    exit(0)

df_all = pd.DataFrame(all_rows)
df_all['ticket_created_at']   = pd.to_datetime(df_all['ticket_created_at'],   utc=True, errors='coerce')
df_all['ticket_completed_at'] = pd.to_datetime(df_all['ticket_completed_at'], utc=True, errors='coerce')
print(f"  Product分佈: {df_all['product'].value_counts().to_dict()}")

df_cur  = filter_period(df_all, week_start_utc, week_end_utc)
df_prev = filter_period(df_all, prev_week_start_utc, prev_week_end_utc)
print(f"本週:{len(df_cur)} 上週:{len(df_prev)}")

ws = pd.Timestamp(week_start_utc, tz='UTC')
we = pd.Timestamp(week_end_utc,   tz='UTC')
ps = pd.Timestamp(prev_week_start_utc, tz='UTC')
pe = pd.Timestamp(prev_week_end_utc,   tz='UTC')

products_data = {}
for product in ['MAAC','CAAC','DAAC']:
    print(f"\n{product}...")
    cur  = df_cur[df_cur['product']==product].copy()  if not df_cur.empty  else pd.DataFrame()
    prev = df_prev[df_prev['product']==product].copy() if not df_prev.empty else pd.DataFrame()
    if cur.empty:
        products_data[product] = {'empty':True,'product':product}
        continue

    cc = cur[cur['ticket_created_at'].between(ws, we)]
    cd = cur[cur['ticket_completed_at'].notna() & cur['ticket_completed_at'].between(ws, we)]

    # 積壓：建立時間 <= 本週結束，且尚未完成或完成時間在本週後
    cb = cur[
        (cur['ticket_created_at'] <= we) &
        (cur['ticket_completed_at'].isna() | (cur['ticket_completed_at'] > we))
    ]

    pc  = prev[prev['ticket_created_at'].between(ps, pe)]   if not prev.empty else pd.DataFrame()
    pd_ = prev[prev['ticket_completed_at'].notna() & prev['ticket_completed_at'].between(ps, pe)] if not prev.empty else pd.DataFrame()

    # 上週積壓：建立時間 <= 上週結束，且尚未完成或完成時間在上週後
    pb = prev[
        (prev['ticket_created_at'] <= pe) &
        (prev['ticket_completed_at'].isna() | (prev['ticket_completed_at'] > pe))
    ] if not prev.empty else pd.DataFrame()

    overview = {
        'created':   wow(len(cc), len(pc)),
        'completed': wow(len(cd), len(pd_)),
        'backlog':   wow(len(cb), len(pb)),
    }

    def rt_bd(done, done_p, bl, bl_p):
        dt = len(done)
        rd = done['resolve_type'].fillna('(Empty)').value_counts()
        rp = done_p['resolve_type'].fillna('(Empty)').value_counts() if not done_p.empty else pd.Series(dtype=int)
        dbd = []
        for rt in sorted(set(list(rd.index)+list(rp.index))):
            cn,pn = int(rd.get(rt,0)),int(rp.get(rt,0))
            pct = round(cn/dt*100,1) if dt else 0
            ft = done[done['resolve_type'].fillna('(Empty)')==rt]['feature'].value_counts().head(5)
            dbd.append({'resolve_type':rt,'count':cn,'pct':pct,'wow':wow(cn,pn),'flag_dominant':pct>30,'features':[{'name':str(k),'count':int(v)} for k,v in ft.items()]})
        bt = len(bl)
        rb = bl['resolve_type'].fillna('(Empty)').value_counts()
        rbp = bl_p['resolve_type'].fillna('(Empty)').value_counts() if not bl_p.empty else pd.Series(dtype=int)
        bbd = []
        for rt in sorted(set(list(rb.index)+list(rbp.index))):
            cn,pn = int(rb.get(rt,0)),int(rbp.get(rt,0))
            pct = round(cn/bt*100,1) if bt else 0
            ft = bl[bl['resolve_type'].fillna('(Empty)')==rt]['feature'].value_counts().head(5)
            bbd.append({'resolve_type':rt,'count':cn,'pct':pct,'wow':wow(cn,pn),'flag_empty_risk':rt=='(Empty)' and pct>20,'features':[{'name':str(k),'count':int(v)} for k,v in ft.items()]})
        return {'done':{'total':dt,'breakdown':dbd},'backlog':{'total':bt,'breakdown':bbd,'top3_features':top_feat(bl['feature'].fillna('(Empty)'))}}

    resolve_analysis = rt_bd(cd, pd_, cb, pb)

    ccp = cc.copy()
    ccp['pn'] = ccp['priority'].apply(norm_pri)
    pcp = pc.copy()
    if not pcp.empty:
        pcp['pn'] = pcp['priority'].apply(norm_pri)

    p1bc = len(cb[cb['priority'].apply(norm_pri)=='P1'])
    p1bp = len(pb[pb['priority'].apply(norm_pri)=='P1']) if not pb.empty else 0
    tc = len(ccp)
    pdist = []
    for p in ['P0','P1','P2~P4','(Empty)']:
        cn = int((ccp['pn']==p).sum())
        pn = int((pcp['pn']==p).sum()) if not pcp.empty else 0
        pct = round(cn/tc*100,1) if tc else 0
        hf = []
        if p in ['P0','P1']:
            f = ccp[ccp['pn']==p]['feature'].value_counts().head(5)
            hf = [{'name':str(k),'count':int(v)} for k,v in f.items()]
        pdist.append({'priority':p,'count':cn,'pct':pct,'wow':wow(cn,pn),'features':hf,'flag_p1_backlog_increase':p=='P1' and p1bc>p1bp})

    nfm = cd['new_feature'].fillna('(Empty)')
    nfp = pd_['new_feature'].fillna('(Empty)') if not pd_.empty else pd.Series(dtype=str)
    nft = len(cd)
    nfdist = []
    for val in ['Yes','No','(Empty)']:
        cn = int((nfm==val).sum())
        pn = int((nfp==val).sum()) if not nfp.empty else 0
        pct = round(cn/nft*100,1) if nft else 0
        det = []
        if val=='Yes' and cn>0:
            yd = cd[cd['new_feature']=='Yes'].copy()
            yd['pn'] = yd['priority'].apply(norm_pri)
            det = {'by_priority':[{'priority':k,'count':int(v)} for k,v in yd['pn'].value_counts().items()],'by_feature':[{'feature':str(k),'count':int(v)} for k,v in yd['feature'].value_counts().head(5).items()],'release_risk':any(yd['pn'].isin(['P0','P1']))}
        nfdist.append({'value':val,'count':cn,'pct':pct,'wow':wow(cn,pn),'detail':det})

    fcr = cc['feature'].value_counts()
    fcd = cd['feature'].value_counts()
    fcb = cb['feature'].value_counts()
    fpc = pc['feature'].value_counts() if not pc.empty else pd.Series(dtype=int)
    tcr = []
    for f,cnt in fcr.head(5).items():
        tcr.append({'feature':str(f),'count':int(cnt),'wow':wow(int(cnt),int(fpc.get(f,0))),'gap':int(cnt)-int(fcd.get(f,0)),'backlog':int(fcb.get(f,0))})
    blt = len(cb)
    blc = []
    for f,cnt in fcb.head(3).items():
        pct = round(cnt/blt*100,1) if blt else 0
        blc.append({'feature':str(f),'count':int(cnt),'pct':pct})

    hps = set(ccp[ccp['pn'].isin(['P0','P1'])]['feature'].dropna().astype(str))
    bls = set(str(f) for f in fcb.head(5).index)
    nfs = set(cd[cd['new_feature']=='Yes']['feature'].dropna().astype(str))
    fh  = {'top_created':tcr,'top_completed':[{'feature':str(f),'count':int(c)} for f,c in fcd.head(5).items()],'backlog_concentration':blc,'triple_risk_features':list(hps&bls&nfs)}

    tc2 = len(cur)
    inv = int((cur['ticket_completed_at'].notna()&(cur['ticket_completed_at']<cur['ticket_created_at'])).sum())
    dq = {'invalid_dates':inv,'null_priority_pct':round(cur['priority'].isna().sum()/tc2*100,1) if tc2 else 0,'null_feature_pct':round(cur['feature'].isna().sum()/tc2*100,1) if tc2 else 0,'null_resolve_type_pct':round(cur['resolve_type'].isna().sum()/tc2*100,1) if tc2 else 0,'null_new_feature_pct':round(cur['new_feature'].isna().sum()/tc2*100,1) if tc2 else 0}

    ert = next((b['pct'] for b in resolve_analysis['backlog']['breakdown'] if b['resolve_type']=='(Empty)'),0)
    tbp = blc[0]['pct'] if blc else 0
    rf = {'empty_resolve_type':ert>20,'single_feature_backlog':tbp>30,'p1_backlog_increase':p1bc>p1bp,'consecutive_created_gt_completed':overview['created']['cur']>overview['completed']['cur'] and overview['created']['prev']>overview['completed']['prev'],'any':False}
    rf['any'] = any([rf['empty_resolve_type'],rf['single_feature_backlog'],rf['p1_backlog_increase'],rf['consecutive_created_gt_completed']])

    print("  產生 AI Insight...")
    ai = generate_ai_insight(product, overview, rf, cur, week_start_label, week_end_label)

    products_data[product] = {'empty':False,'product':product,'overview':overview,'resolve_analysis':resolve_analysis,'priority_distribution':pdist,'new_feature_impact':nfdist,'feature_hotspot':fh,'data_quality':dq,'risk_flags':rf,'ai_insight':ai}
    print(f"  done: 新增={overview['created']['cur']} 完成={overview['completed']['cur']} 積壓={overview['backlog']['cur']}")

# ─── 7. 輸出 ───────────────────────────────────────────────────────────────────
output = {'generated_at':datetime.now(tw).isoformat(),'report_date':report_date_label,'week_start':week_start_label,'week_end':week_end_label,'products':products_data}
Path('docs/data').mkdir(parents=True, exist_ok=True)
with open('docs/data/latest.json','w',encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)
print("\nlatest.json written")

hist_path = Path('docs/data/history.json')
history = json.loads(hist_path.read_text(encoding='utf-8')) if hist_path.exists() else []
dates = {h['report_date'] for h in history}
if report_date_label not in dates:
    history.insert(0, output)
else:
    for i,h in enumerate(history):
        if h['report_date']==report_date_label:
            history[i]=output; break
history = history[:26]
hist_path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding='utf-8')
print(f"history.json updated ({len(history)} weeks)")
print("done!")
