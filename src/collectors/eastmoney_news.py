#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
东方财富新闻收集器

从东方财富「7x24 快讯 / 财经资讯」接口收集 A 股相关新闻。
接口来源：东方财富快讯页（kuaixun.eastmoney.com）当前生产环境使用的
JSONP 接口 search-api-web.eastmoney.com/search/jsonp ，而非已下线的旧接口
newsapi.eastmoney.com/kuaixun/v1/getlist （该路径已 404）。

返回数据格式与 ChinaNewsRSSCollector.fetch_news() 保持一致，便于 NewsCollector
直接复用 _convert_to_news_item()。
"""

import json
import time
import urllib.parse
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
    """东方财富新闻收集器（7x24 快讯 / 财经资讯）"""

    SOURCE_NAME = "东方财富"

    # 当前生产环境真实接口（从 kuaixun.eastmoney.com 现网 JS 反查得到）
    API_URL = "https://search-api-web.eastmoney.com/search/jsonp"

    # cmsColumnList：东方财富快讯各栏目 ID（取自现网 JS，一般无需修改）
    # 覆盖：快讯、股市、公司、行业、基金、债券、外汇、商品、宏观等栏目
    DEFAULT_COLUMNS = (
        "405,406,407,407,408,409,410,411,412,413,414,415,416,417,478,418,"
        "684,752,420,421,804,422,423,424,425,426,427,428,429,430,431,349,"
        "354,366,345,344"
    )

    def __init__(self, max_items: int = 50, columns: Optional[str] = None):
        self.max_items = max_items
        self.columns = columns or self.DEFAULT_COLUMNS
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Referer": "https://kuaixun.eastmoney.com/",
            }
        )
        self.timeout = 15

    # ------------------------------------------------------------------ #
    # 公开接口
    # ------------------------------------------------------------------ #
    def fetch_news(self, max_items: Optional[int] = None) -> List[Dict]:
        """获取东方财富财经新闻。

        Args:
            max_items: 最大获取条数

        Returns:
            新闻字典列表，字段与 ChinaNewsRSSCollector 一致。
        """
        max_items = max_items or self.max_items
        try:
            logger.info(f"开始从东方财富收集财经新闻（最多 {max_items} 条）...")
            raw_items = self._request(
                column="0",
                cms_column_list=self.columns,
                page_index=1,
                page_size=max_items,
            )
            news_list: List[Dict] = []
            for item in raw_items:
                try:
                    parsed = self._parse_item(item)
                    if parsed:
                        news_list.append(parsed)
                except Exception as e:
                    logger.error(f"解析东方财富新闻条目失败: {e}")
                    continue

            logger.info(f"成功从东方财富获取 {len(news_list)} 条新闻")
            return news_list

        except requests.exceptions.RequestException as e:
            logger.error(f"获取东方财富新闻失败（网络）: {e}")
            return []
        except Exception as e:
            logger.error(f"获取东方财富新闻失败: {e}")
            return []

    def test_connection(self) -> Dict:
        """测试接口连通性。"""
        try:
            start = time.time()
            items = self._request(
                column="0", cms_column_list=self.columns, page_index=1, page_size=5
            )
            elapsed = round(time.time() - start, 2)
            return {
                "status": "success" if items else "empty",
                "response_time": elapsed,
                "count": len(items),
                "error": None,
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # ------------------------------------------------------------------ #
    # 内部实现
    # ------------------------------------------------------------------ #
    def _request(
        self,
        column: str,
        cms_column_list: str,
        page_index: int,
        page_size: int,
    ) -> List[Dict]:
        """调用东方财富 JSONP 接口并返回 cmsArticleWebFast 列表。"""
        param = {
            "uid": "",
            "keyword": "",
            "type": ["cmsArticleWebFast"],
            "client": "web",
            "clientVersion": "1.0",
            "clientType": "kuaixun",
            "param": {
                "cmsArticleWebFast": {
                    "column": column,
                    "cmsColumnList": cms_column_list,
                    "pageIndex": page_index,
                    "pageSize": page_size,
                }
            },
        }
        enc = urllib.parse.quote(json.dumps(param, separators=(",", ":")))
        url = f"{self.API_URL}?param={enc}&cb=jQuery&_={int(time.time() * 1000)}"

        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()

        data = self._strip_jsonp(response.text)
        result = (data.get("result") or {}).get("cmsArticleWebFast") or []
        return result

    @staticmethod
    def _strip_jsonp(text: str) -> Dict:
        """去掉 JSONP 包裹（jQueryxxx( ... )），解析为 dict。"""
        t = text.strip()
        if "(" in t and t.rfind(")") > t.find("("):
            t = t[t.find("(") + 1 : t.rfind(")")]
        return json.loads(t)

    def _parse_item(self, item: Dict) -> Optional[Dict]:
        """将东方财富返回的单条数据转换为统一格式。"""
        if not isinstance(item, dict):
            return None

        title = (item.get("title") or "").strip()
        content = (item.get("content") or item.get("summary") or "").strip()
        # 快讯常无标题，以正文作标题兜底
        if not title and content:
            title = content[:60]

        if not title:
            return None

        published_time = self._parse_time(item)
        url = self._build_url(item)

        return {
            "title": title,
            "url": url,
            "content": content,
            "published_time": published_time,
            "source": self.SOURCE_NAME,
            "category": "财经快讯",
            "author": item.get("sourceName") or item.get("author") or "",
            "tags": [],
            "collected_time": datetime.now(timezone.utc).isoformat(),
            "source_type": "api",
            "source_url": "https://kuaixun.eastmoney.com/",
        }

    @staticmethod
    def _parse_time(item: Dict) -> str:
        """解析发布时间，优先使用 Unix 时间戳。"""
        for key in ("showTime", "emit_time", "publishTime", "ctime", "time"):
            val = item.get(key)
            if not val:
                continue
            # 数值时间戳（秒或毫秒）
            if isinstance(val, (int, float)) or (
                isinstance(val, str) and val.isdigit()
            ):
                ts = int(val)
                if ts > 1e12:  # 毫秒
                    ts = ts / 1000
                try:
                    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                except (ValueError, OverflowError, OSError):
                    pass
            # 字符串时间
            if isinstance(val, str):
                for fmt in (
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M",
                ):
                    try:
                        return datetime.strptime(
                            val[:19], fmt
                        ).replace(tzinfo=timezone.utc).isoformat()
                    except ValueError:
                        continue
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _build_url(item: Dict) -> str:
        """构造新闻链接。"""
        url = item.get("url") or item.get("newsUrl") or ""
        if url and url.startswith("http"):
            return url
        item_id = item.get("id") or item.get("newsId") or item.get("artCode")
        if item_id:
            return f"https://kuaixun.eastmoney.com/{item_id}"
        return "https://kuaixun.eastmoney.com/"


def main():
    """测试函数。"""
    collector = EastMoneyNewsCollector(max_items=5)
    print("🔍 测试东方财富新闻接口...")
    result = collector.test_connection()
    print(f"连接状态: {result['status']} | 响应时间: {result.get('response_time')}s | 条数: {result.get('count')}")

    if result["status"] == "success":
        news_list = collector.fetch_news(max_items=5)
        for i, news in enumerate(news_list, 1):
            print(f"\n--- 新闻 {i} ---")
            print(f"标题: {news['title']}")
            print(f"来源: {news['source']} - {news['category']}")
            print(f"时间: {news['published_time']}")
            print(f"链接: {news['url']}")
            print(f"摘要: {news['content'][:100]}...")
    else:
        print(f"❌ 未能获取新闻: {result.get('error', '接口返回空')}")
        print("（如在本机/国内网络运行仍为空，可能是接口参数需更新，请把日志发我）")


if __name__ == "__main__":
    main()
