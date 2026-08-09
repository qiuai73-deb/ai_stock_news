#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东方财富新闻收集器（WEB 爬取模式）

不从 JSON 接口取数（search-api-web 是搜索接口，空关键词必返回 0 条），
而是直接爬取东方财富**服务端渲染**的新闻列表页与文章页：
  - 列表页（finance.eastmoney.com / stock.eastmoney.com）提取文章链接；
  - 文章页提取 标题 / 正文 / 发布时间 / 来源。

返回数据格式与 ChinaNewsRSSCollector.fetch_news() 保持一致，便于
NewsCollector 直接复用 _convert_to_news_item()。
"""

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Dict, Optional

import requests

try:
    from src.utils.logger import logger
except ImportError:
    import logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)


class EastMoneyNewsCollector:
    """东方财富新闻收集器（WEB 爬取模式）"""

    SOURCE_NAME = "东方财富"

    # 服务端渲染的新闻列表页（含大量文章链接，可直接爬）
    DEFAULT_PAGES = [
        "https://finance.eastmoney.com/",
        "https://stock.eastmoney.com/",
    ]

    # 文章链接正则：finance / stock 域下的 /a/<数字>.html
    ARTICLE_RE = re.compile(
        r'href="((?:https?:)?//(?:finance|stock)\.eastmoney\.com/a/\d+\.html)"'
    )

    # 正文候选容器（取第一个非空且最长的）
    CONTENT_PATTERNS = [
        r'<div[^>]*class="contentbox"[^>]*>(.*?)</div>\s*</div>',
        r'<div[^>]*id="article-body"[^>]*>(.*?)</div>',
        r'<div[^>]*class="article-content"[^>]*>(.*?)</div>',
        r'<div[^>]*class="content"[^>]*>(.*?)</div>',
    ]

    def __init__(
        self,
        max_items: int = 50,
        mode: str = "web",
        pages: Optional[List[str]] = None,
        max_workers: int = 8,
    ):
        self.max_items = max_items
        self.mode = mode
        self.pages = pages or self.DEFAULT_PAGES
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            }
        )
        self.timeout = 12

    # ------------------------------------------------------------------ #
    # 公开接口
    # ------------------------------------------------------------------ #
    def fetch_news(self, max_items: Optional[int] = None) -> List[Dict]:
        """获取东方财富财经新闻（WEB 爬取）。"""
        max_items = max_items or self.max_items
        logger.info(f"开始从东方财富(WEB)收集财经新闻（最多 {max_items} 条）...")

        # 1) 从列表页收集文章链接
        article_urls = self._collect_article_urls()
        if not article_urls:
            logger.warning("未能从东方财富列表页收集到任何文章链接")
            return []

        logger.info(f"东方财富列表页共发现 {len(article_urls)} 篇文章，开始抓取正文...")

        # 2) 并发抓取每篇文章的 标题/正文/时间/来源
        #    多抓一些（缓冲），以便按时间排序后取最新 max_items 条
        fetch_pool = article_urls[: max(max_items * 2, 30)]
        news_list: List[Dict] = []
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
                futures = {
                    ex.submit(self._parse_article, url): url for url in fetch_pool
                }
                for fut in as_completed(futures):
                    try:
                        item = fut.result()
                        if item:
                            news_list.append(item)
                    except Exception:
                        continue
        except Exception as e:
            logger.error(f"并发抓取文章失败: {e}")

        # 3) 去重 + 按时间倒序 + 截断
        news_list = self._dedupe(news_list)
        news_list.sort(key=lambda x: x.get("published_time") or "", reverse=True)
        news_list = news_list[:max_items]

        logger.info(f"成功从东方财富(WEB)获取 {len(news_list)} 条新闻")
        return news_list

    def test_connection(self) -> Dict:
        """测试列表页连通性。"""
        try:
            start = time.time()
            html = self._get(self.pages[0])
            urls = self.ARTICLE_RE.findall(html) if html else []
            elapsed = round(time.time() - start, 2)
            return {
                "status": "success" if urls else "empty",
                "response_time": elapsed,
                "article_links": len(urls),
                "error": None,
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # ------------------------------------------------------------------ #
    # 内部实现
    # ------------------------------------------------------------------ #
    def _get(self, url: str) -> Optional[str]:
        """带超时的 GET，返回文本（失败返回 None）。"""
        try:
            r = self.session.get(url, timeout=self.timeout)
            r.encoding = "utf-8"
            return r.text
        except Exception as e:
            logger.error(f"请求失败 {url}: {e}")
            return None

    def _collect_article_urls(self) -> List[str]:
        """遍历列表页，提取去重后的文章 URL（保序）。"""
        seen: Dict[str, None] = {}
        for page in self.pages:
            html = self._get(page)
            if not html:
                continue
            for m in self.ARTICLE_RE.findall(html):
                url = m if m.startswith("http") else "https:" + m
                url = url.split("?", 1)[0]  # 去查询参数
                if url not in seen:
                    seen[url] = None
        return list(seen.keys())

    def _parse_article(self, url: str) -> Optional[Dict]:
        """抓取单篇文章，提取统一格式字段。"""
        html = self._get(url)
        if not html:
            return None

        # 标题
        title = self._extract_title(html)
        if not title:
            return None

        # 正文
        content = self._extract_content(html)

        # 时间
        published_time = self._extract_time(html, url)

        # 来源
        source_name = self._extract_source(html)

        return {
            "title": title,
            "url": url,
            "content": content,
            "published_time": published_time,
            "source": self.SOURCE_NAME,
            "category": "财经新闻",
            "author": source_name or "",
            "tags": [],
            "collected_time": datetime.now(timezone.utc).isoformat(),
            "source_type": "web",
            "source_url": url,
        }

    # ------------------------------ 字段提取 ------------------------------ #
    @staticmethod
    def _extract_title(html: str) -> str:
        m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S)
        title = m.group(1).strip() if m else ""
        if not title:
            m = re.search(r"<title>(.*?)</title>", html, re.S)
            title = m.group(1).strip() if m else ""
        # 去掉常见的站点后缀
        title = re.split(r"_\s*东方财富网| - 东方财富网|_\s*东方财富", title)[0].strip()
        return title

    @staticmethod
    def _extract_content(html: str) -> str:
        best = ""
        for pat in EastMoneyNewsCollector.CONTENT_PATTERNS:
            m = re.search(pat, html, re.S)
            if not m:
                continue
            text = re.sub(r"<[^>]+>", " ", m.group(1))
            text = re.sub(r"&nbsp;", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
            if len(text) > len(best):
                best = text
        # 兜底：拿 <p> 段落拼接
        if not best:
            paras = re.findall(r"<p[^>]*>(.*?)</p>", html, re.S)
            best = re.sub(r"<[^>]+>", "", " ".join(paras))
            best = re.sub(r"\s+", " ", best).strip()
        return best

    @staticmethod
    def _extract_time(html: str, url: str) -> str:
        """优先从正文提取时间，其次从 URL 中的日期推断。"""
        # 中文格式：2026年08月07日 05:04
        m = re.search(r"(\d{4})年(\d{2})月(\d{2})日\s*(\d{2}):(\d{2})", html)
        if m:
            try:
                return datetime(
                    int(m.group(1)), int(m.group(2)), int(m.group(3)),
                    int(m.group(4)), int(m.group(5)), tzinfo=timezone.utc,
                ).isoformat()
            except ValueError:
                pass
        # 横线格式：2026-08-07 05:04
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})", html)
        if m:
            try:
                return datetime(
                    int(m.group(1)), int(m.group(2)), int(m.group(3)),
                    int(m.group(4)), int(m.group(5)), tzinfo=timezone.utc,
                ).isoformat()
            except ValueError:
                pass
        # URL 中日期：/a/202608063834145493.html -> 2026-08-06
        m = re.search(r"/a/(\d{4})(\d{2})(\d{2})\d+\.html", url)
        if m:
            try:
                return datetime(
                    int(m.group(1)), int(m.group(2)), int(m.group(3)),
                    tzinfo=timezone.utc,
                ).isoformat()
            except ValueError:
                pass
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _extract_source(html: str) -> str:
        m = re.search(r"文章来源[：:]\s*([^<>\s（）()]{2,20})", html)
        if m:
            return m.group(1).strip()
        return ""

    @staticmethod
    def _dedupe(items: List[Dict]) -> List[Dict]:
        seen = set()
        out = []
        for it in items:
            key = it.get("url") or it.get("title")
            if key in seen:
                continue
            seen.add(key)
            out.append(it)
        return out


def main():
    """测试函数：直接运行可验证 WEB 爬取是否可用。"""
    collector = EastMoneyNewsCollector(max_items=10)
    print("🔍 测试东方财富(WEB)新闻接口...")
    conn = collector.test_connection()
    print(f"列表页连通: {conn['status']} | 响应: {conn.get('response_time')}s "
          f"| 文章链接数: {conn.get('article_links')}")
    if conn["status"] != "success":
        print("❌ 列表页未能获取文章链接，请检查网络/是否被拦截")
        return

    news_list = collector.fetch_news(max_items=10)
    if not news_list:
        print("❌ 未抓取到新闻（可能文章页被拦截），请把日志发我")
        return

    print(f"\n✅ 成功抓取 {len(news_list)} 条：\n")
    for i, n in enumerate(news_list, 1):
        print(f"--- 新闻 {i} ---")
        print(f"标题: {n['title']}")
        print(f"时间: {n['published_time']}")
        print(f"来源: {n['author'] or '东方财富'}")
        print(f"链接: {n['url']}")
        print(f"正文(前100字): {n['content'][:100]}")
        print()


if __name__ == "__main__":
    main()
