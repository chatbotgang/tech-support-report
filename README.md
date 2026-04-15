# 📊 Support Intelligence Dashboard

每週自動從 Supabase 拉取 Support Ticket 資料，產生 Dashboard 並透過 GitHub Pages 呈現。

## 🏗️ 架構

```
GitHub Actions (每週一 11:00 AM 台灣時間)
    │
    ▼
scripts/generate_report.py
  ├─ 查詢 Supabase REST API
  ├─ 計算 MAAC / CAAC / DAAC 三產品數據
  └─ 輸出 docs/data/latest.json + history.json
    │
    ▼
git push → GitHub Pages 自動更新
    │
    ▼
https://<your-username>.github.io/<repo-name>/
```

## 🚀 部署步驟

### 1. 建立 GitHub Repo

```bash
# 在 GitHub 建立新 repo，然後：
git init
git remote add origin https://github.com/<your-username>/<repo-name>.git
git add .
git commit -m "init: support intelligence dashboard"
git push -u origin main
```

### 2. 設定 GitHub Secrets

到 GitHub Repo → **Settings → Secrets and variables → Actions** → **New repository secret**：

| Secret 名稱 | 值 |
|---|---|
| `SUPABASE_URL` | `https://jkkovxgjetvmfcvzxrrf.supabase.co` |
| `SUPABASE_SERVICE_KEY` | 你的 `service_role` API Key |

### 3. 啟用 GitHub Pages

到 GitHub Repo → **Settings → Pages**：
- Source: **Deploy from a branch**
- Branch: `main` / `docs`
- 儲存後等約 1 分鐘，網址會是：`https://<username>.github.io/<repo>/`

### 4. 手動觸發測試

到 **Actions → Weekly Support Report → Run workflow** 測試執行。

## 📁 檔案結構

```
.github/workflows/
  weekly_report.yml       # 排程 + 執行腳本
scripts/
  generate_report.py      # 資料查詢與分析
docs/
  index.html              # Dashboard 主頁
  data/
    latest.json           # 最新週數據
    history.json          # 歷史所有週（保留 26 週）
```

## ⏰ 排程時間

- Cron: `0 3 * * 1`（UTC）= 台灣時間每週一 11:00 AM

## 🔧 本機測試

```bash
pip install pandas pytz requests

export SUPABASE_URL="https://jkkovxgjetvmfcvzxrrf.supabase.co"
export SUPABASE_SERVICE_KEY="your-service-key"

python scripts/generate_report.py
```

產出 `docs/data/latest.json` 和 `docs/data/history.json` 後，
用瀏覽器開啟 `docs/index.html` 即可預覽。
