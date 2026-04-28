import sys
import asyncio
import aiohttp
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
import re
from bs4 import BeautifulSoup
import sqlite3
import random
import os
import json
import logging
from typing import List, Set, Dict
from dateutil import parser as date_parser

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/extractor.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class UltraFastSitemapExtractor:
    def __init__(self, domain: str, output_file: str, db_file: str, process_percentage: float = 100.0):
        self.domain = domain.rstrip('/')
        self.output_file = output_file
        self.db_file = db_file
        self.process_percentage = process_percentage
        self.existing_urls = self.load_existing_urls()
        self.all_urls = set()
        self.urls_metadata = {}
        self.batch_size = 50
        self.semaphore = None

    def load_existing_urls(self) -> Set[str]:
        if os.path.exists(self.output_file):
            try:
                df = pd.read_excel(self.output_file)
                existing = set(df['url'].tolist())
                logger.info(f"Loaded {len(existing)} existing URLs from Excel")
                return existing
            except Exception as e:
                logger.error(f"Error loading existing file: {e}")
        return set()

    def init_database(self):
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS news_articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    title TEXT,
                    published_date TEXT,
                    article_content TEXT,
                    extracted_date TEXT
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON news_articles(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_published_date ON news_articles(published_date)')
            conn.commit()
            conn.close()
            logger.info(f"Database initialized: {self.db_file}")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")

    async def fetch_robots_txt(self, session: aiohttp.ClientSession) -> str:
        robots_url = f"{self.domain}/robots.txt"
        try:
            async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            logger.warning(f"Could not fetch robots.txt: {e}")
        return ""

    def extract_sitemap_from_robots(self, robots_content: str) -> List[str]:
        sitemaps = []
        pattern = r'[Ss]itemap:\s*(https?://\S+)'
        matches = re.findall(pattern, robots_content)
        if matches:
            sitemaps.extend(matches)
            logger.info(f"Found {len(matches)} sitemap(s) in robots.txt")
        else:
            default_sitemaps = [
                f"{self.domain}/sitemap.xml",
                f"{self.domain}/sitemap_index.xml",
                f"{self.domain}/sitemap/sitemap.xml",
                f"{self.domain}/sitemap/sitemap_index.xml",
                f"{self.domain}/sitemap-0.xml",
                f"{self.domain}/sitemap1.xml",
                f"{self.domain}/news_sitemap.xml",
                f"{self.domain}/sitemap_news.xml"
            ]
            sitemaps.extend(default_sitemaps)
            logger.info("No sitemaps in robots.txt, trying common locations")
        return list(set(sitemaps))

    async def fetch_sitemap(self, session: aiohttp.ClientSession, sitemap_url: str) -> tuple:
        try:
            async with self.semaphore:
                async with session.get(sitemap_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        content = await response.text()
                        return content, sitemap_url
                    else:
                        logger.warning(f"Failed to fetch {sitemap_url}: HTTP {response.status}")
        except Exception as e:
            logger.warning(f"Error fetching {sitemap_url}: {e}")
        return "", sitemap_url

    def parse_sitemap_with_metadata(self, content: str, sitemap_url: str) -> tuple:
        urls = set()
        subsitemaps = set()
        url_metadata = {}
        try:
            root = ET.fromstring(content)
            namespaces = {
                'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                'news': 'http://www.google.com/schemas/sitemap-news/0.9',
            }
            if root.find('.//ns:sitemap', namespaces) is not None:
                for sitemap_elem in root.findall('.//ns:sitemap', namespaces):
                    loc = sitemap_elem.find('ns:loc', namespaces)
                    if loc is not None and loc.text:
                        subsitemaps.add(loc.text)
            else:
                for url_elem in root.findall('.//ns:url', namespaces):
                    loc = url_elem.find('ns:loc', namespaces)
                    if loc is not None and loc.text:
                        url = loc.text
                        urls.add(url)
                        metadata = {}
                        news_date_elem = url_elem.find('.//news:publication_date', namespaces)
                        if news_date_elem is not None and news_date_elem.text:
                            metadata['news_date'] = news_date_elem.text
                        news_title_elem = url_elem.find('.//news:title', namespaces)
                        if news_title_elem is not None and news_title_elem.text:
                            metadata['news_title'] = news_title_elem.text
                        lastmod_elem = url_elem.find('ns:lastmod', namespaces)
                        if lastmod_elem is not None and lastmod_elem.text:
                            metadata['lastmod'] = lastmod_elem.text
                        date_from_url = self.extract_date_from_url(url)
                        if date_from_url:
                            metadata['url_date'] = date_from_url
                        url_metadata[url] = metadata
        except Exception as e:
            logger.error(f"Error parsing sitemap XML: {e}")
        return urls, subsitemaps, url_metadata

    def extract_date_from_url(self, url: str) -> str:
        patterns = [
            r'/(\d{4})/(\d{1,2})/(\d{1,2})/',
            r'/(\d{4})-(\d{1,2})-(\d{1,2})/',
            r'/(\d{4})(\d{2})(\d{2})/',
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                try:
                    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        return datetime(year, month, day).strftime("%Y-%m-%d")
                except:
                    continue
        return None

    def parse_date_from_string(self, date_str: str):
        try:
            return date_parser.parse(date_str)
        except:
            return None

    async def crawl_sitemap_tree(self, session: aiohttp.ClientSession, sitemap_url: str):
        visited_sitemaps = set()
        to_visit = [sitemap_url]
        while to_visit and len(visited_sitemaps) < 200:
            current = to_visit.pop(0)
            if current in visited_sitemaps:
                continue
            visited_sitemaps.add(current)
            logger.info(f"Processing sitemap: {current}")
            content, _ = await self.fetch_sitemap(session, current)
            if not content:
                continue
            urls, subsitemaps, url_metadata = self.parse_sitemap_with_metadata(content, current)
            for url in urls:
                if url not in self.existing_urls:
                    self.all_urls.add(url)
                    if url in url_metadata:
                        self.urls_metadata[url] = url_metadata[url]
            for submap in subsitemaps:
                if submap not in visited_sitemaps:
                    to_visit.append(submap)

    def select_urls_by_percentage(self, urls_list: List[str]) -> List[str]:
        if self.process_percentage >= 100.0:
            return urls_list
        total_urls = len(urls_list)
        urls_to_process = max(1, int((self.process_percentage / 100.0) * total_urls))
        selected_urls = random.sample(urls_list, urls_to_process)
        logger.info(f"Processing {self.process_percentage}% of URLs ({urls_to_process}/{total_urls})")
        return selected_urls

    def extract_date_from_html(self, soup: BeautifulSoup):
        date_patterns = [
            ('meta[property="article:published_time"]', 'content'),
            ('meta[name="article:published_time"]', 'content'),
            ('meta[name="date"]', 'content'),
            ('time[datetime]', 'datetime'),
            ('[itemprop="datePublished"]', 'content'),
        ]
        for selector, attr in date_patterns:
            try:
                elements = soup.select(selector)
                for elem in elements:
                    date_str = elem.get(attr, '')
                    if date_str:
                        parsed = self.parse_date_from_string(date_str)
                        if parsed and parsed.year > 2000:
                            return parsed
            except:
                continue
        return None

    def extract_article_content(self, soup: BeautifulSoup) -> str:
        article_paragraphs = []
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text(strip=True)
            if text and len(text) > 20:
                article_paragraphs.append(text)
        if article_paragraphs:
            article_text = '\n\n'.join(article_paragraphs)
            return article_text[:10000] + "..." if len(article_text) > 10000 else article_text
        return ""

    def extract_title(self, soup: BeautifulSoup, metadata: Dict) -> str:
        if 'news_title' in metadata and metadata['news_title']:
            return metadata['news_title']
        h1_tag = soup.find('h1')
        if h1_tag and h1_tag.get_text(strip=True):
            return h1_tag.get_text(strip=True)[:500]
        title_tag = soup.find('title')
        if title_tag and title_tag.get_text(strip=True):
            return title_tag.get_text(strip=True)[:500]
        return ""

    def get_best_published_date(self, url: str, metadata: Dict, soup=None) -> str:
        best_date = None
        if 'news_date' in metadata and metadata['news_date']:
            parsed = self.parse_date_from_string(metadata['news_date'])
            if parsed and parsed.year > 2000:
                best_date = parsed
        if not best_date and 'url_date' in metadata and metadata['url_date']:
            parsed = self.parse_date_from_string(metadata['url_date'])
            if parsed:
                best_date = parsed
        if not best_date and 'lastmod' in metadata and metadata['lastmod']:
            parsed = self.parse_date_from_string(metadata['lastmod'])
            if parsed:
                best_date = parsed
        if not best_date and soup:
            html_date = self.extract_date_from_html(soup)
            if html_date:
                best_date = html_date
        if not best_date:
            return ""
        return best_date.strftime("%d %b %Y, %I:%M %p")

    async def extract_page_metadata_async(self, session: aiohttp.ClientSession, url: str) -> Dict:
        metadata = {'url': url, 'title': '', 'published_date': '', 'article_content': ''}
        try:
            async with self.semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10), headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        url_metadata = self.urls_metadata.get(url, {})
                        metadata['title'] = self.extract_title(soup, url_metadata)
                        metadata['article_content'] = self.extract_article_content(soup)
                        metadata['published_date'] = self.get_best_published_date(url, url_metadata, soup)
        except Exception as e:
            logger.warning(f"Error extracting from {url[:80]}: {e}")
        return metadata

    async def process_all_urls_ultrafast(self):
        all_new_urls = [url for url in self.all_urls if url not in self.existing_urls]
        if not all_new_urls:
            logger.info("No new URLs to process")
            return []
        urls_to_process = self.select_urls_by_percentage(all_new_urls)
        logger.info(f"Processing {len(urls_to_process)} new URLs...")
        results = []
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(urls_to_process), self.batch_size):
                batch = urls_to_process[i:i + self.batch_size]
                tasks = [self.extract_page_metadata_async(session, url) for url in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in batch_results:
                    if isinstance(result, dict):
                        results.append(result)
                logger.info(f"Progress: {min(i + self.batch_size, len(urls_to_process))}/{len(urls_to_process)}")
        return results

    def save_to_database(self, metadata_list: List[Dict]):
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            new_count = 0
            for metadata in metadata_list:
                if metadata['article_content']:
                    try:
                        cursor.execute('''
                            INSERT OR IGNORE INTO news_articles
                            (url, title, published_date, article_content, extracted_date)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            metadata['url'],
                            metadata['title'][:500] if metadata['title'] else '',
                            metadata['published_date'] or None,
                            metadata['article_content'],
                            datetime.now().isoformat()
                        ))
                        if cursor.rowcount > 0:
                            new_count += 1
                    except Exception as e:
                        logger.warning(f"Error saving to DB: {e}")
            conn.commit()
            conn.close()
            logger.info(f"Saved {new_count} news articles to database: {self.db_file}")
        except Exception as e:
            logger.error(f"Error saving to database: {e}")

    def save_to_excel(self, metadata_list: List[Dict]):
        if os.path.exists(self.output_file):
            existing_df = pd.read_excel(self.output_file)
            all_data = existing_df.to_dict('records')
        else:
            all_data = []
        new_count = 0
        for metadata in metadata_list:
            if metadata['url'] not in self.existing_urls:
                all_data.append({
                    'serial': len(all_data) + 1,
                    'url': metadata['url'],
                    'title': metadata.get('title', ''),
                    'published_date': metadata.get('published_date', '')
                })
                new_count += 1
        if all_data:
            df = pd.DataFrame(all_data)
            df = df[['serial', 'url', 'title', 'published_date']]
            df.to_excel(self.output_file, index=False)
            logger.info(f"Saved {new_count} new URLs to Excel: {self.output_file}")
        else:
            logger.info("No new data to save to Excel")

    async def extract_all_sitemaps(self):
        self.semaphore = asyncio.Semaphore(50)
        self.init_database()
        logger.info(f"Starting extraction for {self.domain}")
        logger.info(f"Processing mode: {self.process_percentage}% of total URLs")
        async with aiohttp.ClientSession() as session:
            robots_content = await self.fetch_robots_txt(session)
            sitemap_list = self.extract_sitemap_from_robots(robots_content)
            if not sitemap_list:
                logger.error("No sitemaps found!")
                return
            logger.info(f"Processing {len(sitemap_list)} sitemap(s)...")
            for sitemap in sitemap_list:
                await self.crawl_sitemap_tree(session, sitemap)
            logger.info(f"Total unique URLs found: {len(self.all_urls)}")
            metadata_results = await self.process_all_urls_ultrafast()
            self.save_to_excel(metadata_results)
            self.save_to_database(metadata_results)
            articles_with_content = sum(1 for m in metadata_results if m['article_content'])
            logger.info(f"Extraction complete for {self.domain}:")
            logger.info(f"  - New URLs extracted: {len(metadata_results)}")
            logger.info(f"  - Total URLs found: {len(self.all_urls)}")
            logger.info(f"  - Articles with content: {articles_with_content}")


async def run_extraction(sites_config: List[Dict], process_percentage: float = 100.0):
    """Run extraction for all sites in config"""
    logger.info(f"{'='*60}")
    logger.info(f"Bangladesh Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} BDT")
    logger.info(f"Starting scheduled extraction for {len(sites_config)} site(s)")
    logger.info(f"{'='*60}")

    for site in sites_config:
        domain = site['domain']
        output_file = site.get('output_file', f"output/{re.sub(r'[^a-zA-Z0-9]', '_', domain)}.xlsx")
        db_file = site.get('db_file', f"output/{re.sub(r'[^a-zA-Z0-9]', '_', domain)}.db")
        percentage = site.get('process_percentage', process_percentage)

        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        os.makedirs(os.path.dirname(db_file), exist_ok=True)

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {domain}")
        logger.info(f"Excel: {output_file}")
        logger.info(f"DB: {db_file}")
        logger.info(f"{'='*60}")

        try:
            extractor = UltraFastSitemapExtractor(domain, output_file, db_file, percentage)
            await extractor.extract_all_sitemaps()
        except Exception as e:
            logger.error(f"Failed to process {domain}: {e}")

    logger.info("\n✅ All sites extraction completed!")


def load_sites_config(config_path: str = "config/sites.json") -> List[Dict]:
    """Load sites from JSON config file"""
    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        logger.info("Creating sample config file...")
        sample = [
            {
                "domain": "https://example-news-site.com",
                "output_file": "output/example_news.xlsx",
                "db_file": "output/example_news.db",
                "process_percentage": 100.0
            }
        ]
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, 'w') as f:
            json.dump(sample, f, indent=2)
        logger.info(f"Sample config created at {config_path}. Please edit it and re-run.")
        return []
    with open(config_path, 'r') as f:
        return json.load(f)


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    sites = load_sites_config("config/sites.json")
    if sites:
        asyncio.run(run_extraction(sites))
    else:
        logger.warning("No sites configured. Please edit config/sites.json")
