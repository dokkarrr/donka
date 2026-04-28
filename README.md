# 📰 Advanced Sitemap Extractor Pro
### Auto-runs every day at **5:00 AM Bangladesh Time** via GitHub Actions

---

## 🚀 Quick Setup (5 Minutes)

### Step 1: Fork or Clone This Repo
```
Click the green "Use this template" button → Create your own copy
```

### Step 2: Edit `config/sites.json`
Add your target websites:
```json
[
  {
    "domain": "https://your-news-site.com",
    "output_file": "output/your_site.xlsx",
    "db_file": "output/your_site.db",
    "process_percentage": 100.0
  },
  {
    "domain": "https://another-news-site.com",
    "output_file": "output/another_site.xlsx",
    "db_file": "output/another_site.db",
    "process_percentage": 50.0
  }
]
```

### Step 3: Enable GitHub Actions
- Go to your repo → **Actions** tab → Enable workflows

### Step 4: Done! 🎉
The extractor will run automatically every day at **5:00 AM BDT**.

---

## ⚙️ Configuration Options

| Field | Description | Example |
|-------|-------------|---------|
| `domain` | The website URL to extract | `"https://example.com"` |
| `output_file` | Path to save Excel file | `"output/site.xlsx"` |
| `db_file` | Path to save SQLite database | `"output/site.db"` |
| `process_percentage` | % of URLs to process (0.01–100) | `100.0` |

---

## ⏰ Schedule Details

| Timezone | Time |
|----------|------|
| 🇧🇩 Bangladesh (BDT) | 5:00 AM |
| 🌍 UTC | 11:00 PM (previous day) |

The schedule uses `cron: '0 23 * * *'` in GitHub Actions.

---

## 🖱️ Manual Run

You can trigger extraction manually anytime:
1. Go to **Actions** tab in GitHub
2. Click **Daily Sitemap Extraction**
3. Click **Run workflow**
4. Set custom percentage (optional)
5. Click **Run workflow** (green button)

---

## 📦 Output Files

After each run, you'll find output files in two places:

### 1. Repository (committed automatically)
- `output/*.xlsx` — Excel files with URLs, titles, dates
- `output/*.db` — SQLite databases with full article content
- `logs/extractor.log` — Full extraction log

### 2. GitHub Actions Artifacts
- Available for 30 days
- Download from: **Actions → Select run → Artifacts section**

---

## 📊 Excel Output Format

| Column | Description |
|--------|-------------|
| serial | Row number |
| url | Article URL |
| title | Article title |
| published_date | Publication date |

---

## 🗄️ SQLite Database Schema

```sql
CREATE TABLE news_articles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT UNIQUE,
    title           TEXT,
    published_date  TEXT,
    article_content TEXT,
    extracted_date  TEXT
);
```

---

## 🔧 Local Installation (Optional)

Run on your own machine:

```bash
# Clone repo
git clone https://github.com/YOUR_USERNAME/sitemap-extractor.git
cd sitemap-extractor

# Install dependencies
pip install -r requirements.txt

# Edit your sites
nano config/sites.json

# Run extraction
python src/extractor.py
```

**Requirements:** Python 3.9+

---

## 🖥️ GUI Version

The original GUI desktop app (requires PyQt6) is in `src/gui_app.py`.
Install GUI dependencies:
```bash
pip install PyQt6 qasync
python src/gui_app.py
```

---

## 📋 Date Extraction Priority

The extractor finds publication dates using this priority order:
1. **News sitemap tag** (`<news:publication_date>`)
2. **URL pattern** (`/2024/01/15/article`)
3. **Sitemap lastmod** date
4. **HTML meta tags** (`article:published_time`, `datePublished`)
5. **Article content** text patterns

---

## 🌐 Supported Sitemap Formats

- `sitemap.xml`
- `sitemap_index.xml` (with child sitemaps)
- `news_sitemap.xml` (Google News format)
- Custom locations detected from `robots.txt`

---

## ❓ Troubleshooting

**No URLs found:**
- Check if the site has a public sitemap
- Try visiting `https://yoursite.com/sitemap.xml` in browser

**Extraction fails:**
- Some sites block bots — try reducing `process_percentage`
- Check logs in `Actions → Run → Logs`

**GitHub Actions not running:**
- Make sure Actions are enabled in repo settings
- Check that `config/sites.json` has valid URLs

---

## 📄 License

MIT License — Free to use and modify.
