"""
AI分析模块
使用 DeepSeek / OpenRouter AI 分析新闻对 A 股板块的影响
支持单条与批量并发新闻分析
"""

import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import openai
import yaml
from openai import OpenAI

from ..utils.database import NewsItem, db_manager
from ..utils.logger import get_logger

logger = get_logger("ai_analyzer")


@dataclass
class AnalysisResult:
    """分析结果数据模型"""

    news_id: str
    impact_score: float  # 0到100，50为中性，>50偏正面，<50偏负面
    summary: str
    analysis_time: datetime

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "news_id": self.news_id,
            "impact_score": self.impact_score,
            "summary": self.summary,
            "analysis_time": self.analysis_time.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AnalysisResult":
        """从字典创建 AnalysisResult 对象"""
        analysis_time = (
            datetime.fromisoformat(data["analysis_time"])
            if data.get("analysis_time")
            else datetime.now()
        )

        return cls(
            news_id=data["news_id"],
            impact_score=data["impact_score"],
            summary=data["summary"],
            analysis_time=analysis_time,
        )


class AIAnalyzer:
    """AI 新除分析器，支持单条与并发批量分析"""

    def __init__(self, config_path: str = None, provider: str = "openrouter"):
        """
        初始化 AI 分析器

        Args:
            config_path: 配置文件路径
            provider: API提供商，支持 'deepseek' 或 'openrouter'
        """
        self.config = self._load_config(config_path)
        self.provider = provider if provider in ["openrouter", "deepseek"] else "openrouter"
        
        self.client = None
        self._setup_client()

        # 统计数据与并发锁
        self._stats_lock = threading.Lock()
        self.stats = {
            "analyzed": 0,
            "errors": 0,
            "api_calls": 0,
            "total_tokens": 0,
            "provider": self.provider
        }

    def _load_config(self, config_path: Optional[str]) -> dict:
        """加载配置文件"""
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__), "../../config/config.yaml"
            )

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return {}

    def _setup_client(self):
        """设置 OpenAI 客户端（兼容 DeepSeek 和 OpenRouter API）"""
        if self.provider == "openrouter":
            ai_config = self.config.get("ai_analysis", {}).get("openrouter", {})
            provider_name = "OpenRouter"
            default_base_url = "https://openrouter.ai/api/v1"
            config_path_info = "config/config.yaml -> ai_analysis -> openrouter -> api_key"
        else:  # deepseek
            ai_config = self.config.get("ai_analysis", {}).get("deepseek", {})
            provider_name = "DeepSeek"
            default_base_url = "https://api.deepseek.com/v1"
            config_path_info = "config/config.yaml -> ai_analysis -> deepseek -> api_key"

        api_key = ai_config.get("api_key", "")
        if not api_key:
            error_msg = f"配置文件中未找到 {provider_name} API 密钥，程序无法正常运行"
            logger.error(error_msg)
            logger.error(f"配置路径: {config_path_info}")
            raise ValueError(error_msg)

        try:
            base_url = ai_config.get("base_url", default_base_url)
            logger.info(f"正在初始化 {provider_name} API客户端，base_url: {base_url}")
            
            extra_headers = {}
            if self.provider == "openrouter":
                extra_headers = {
                    "HTTP-Referer": "https://ai-news-collector.com",
                    "X-Title": "AI-News-Analysis-System",
                }
            
            self.client = OpenAI(
                api_key=api_key,
                base_url=base_url,
                default_headers=extra_headers if extra_headers else None,
            )
            logger.info(f"{provider_name} API 客户端初始化成功")
            
        except Exception as e:
            error_msg = f"{provider_name} API 客户端初始化失败: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def analyze_news(self, news_item: NewsItem, save_to_db: bool = True) -> AnalysisResult:
        """
        分析单条新闻

        Args:
            news_item: 新闻项
            save_to_db: 是否自动将结果保存至数据库

        Returns:
            AnalysisResult: 分析结果
        """
        if not self.client:
            logger.error(f"{self.provider.upper()} API 客户端不可用")
            raise RuntimeError(f"{self.provider.upper()} API 客户端不可用")

        try:
            prompt = self._build_analysis_prompt(news_item)
            response = self._call_ai_api(prompt)
            result = self._parse_analysis_response(news_item.id, response)

            if save_to_db:
                self._save_analysis_result(result)

            with self._stats_lock:
                self.stats["analyzed"] += 1

            logger.debug(f"新闻分析成功完成: {news_item.title[:30]}...")
            return result

        except Exception as e:
            with self._stats_lock:
                self.stats["errors"] += 1
            logger.error(f"新闻分析过程出现异常 [{news_item.title[:30]}...]: {e}")
            raise

    def _build_analysis_prompt(self, news_item: NewsItem) -> str:
        """构建分析提示词"""
        prompt = f"""
请你作为一位专业的 A 股市场分析师，对以下新闻进行深度分析，重点关注其对 A 股市场的影响。

新闻信息：
标题：{news_item.title}
内容：{news_item.content}
来源：{news_item.source}
发布时间：{news_item.publish_time}
关键词：{', '.join(news_item.keywords if news_item.keywords else [])}

请按照以下 JSON 格式输出分析结果：
{{
    "impact_score": 数值(0到100),
    "summary": "新闻影响摘要(100字以内)"
}}

分析要求：
1. 影响评分范围：0 到 100（50 表示无影响/中性；0 表示极度利空/负面；100 表示极度利好/正面）。
2. 评价需客观精准，摘要控制在 100 字以内。

请确保输出严格按照 JSON 格式，不要包含任何其他额外的非 JSON 文本。
"""
        return prompt.strip()

    def _call_ai_api(self, prompt: str) -> str:
        """调用 AI API（支持 JSON 模式及详细日志）"""
        if self.provider == "openrouter":
            ai_config = self.config.get("ai_analysis", {}).get("openrouter", {})
            default_model = "deepseek/deepseek-chat"
            default_base_url = "https://openrouter.ai/api/v1"
        else:
            ai_config = self.config.get("ai_analysis", {}).get("deepseek", {})
            default_model = "deepseek-chat"
            default_base_url = "https://api.deepseek.com/v1"

        analysis_params = self.config.get("ai_analysis", {}).get("analysis_params", {})

        model = ai_config.get("model", default_model)
        max_tokens = ai_config.get("max_tokens", 2000)
        temperature = ai_config.get("temperature", 0.1)
        timeout = analysis_params.get("timeout", 60)
        base_url = ai_config.get("base_url", default_base_url)

        logger.info(f"🔄 调用 {self.provider.upper()} API | 模型: {model}")

        start_time = time.time()
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},  # 开启强 JSON 模式输出
                max_tokens=max_tokens,
                temperature=temperature,
                timeout=timeout,
            )

            response_time = time.time() - start_time
            
            # 更新全局线程安全的统计信息
            with self._stats_lock:
                self.stats["api_calls"] += 1
                if hasattr(response, "usage") and response.usage:
                    self.stats["total_tokens"] += response.usage.total_tokens

            logger.info(f"📥 API 响应成功 (耗时: {response_time:.2f}s)")
            response_content = response.choices[0].message.content
            return response_content

        except Exception as e:
            response_time = time.time() - start_time
            logger.error(f"❌ {self.provider.upper()} API 调用失败 (耗时: {response_time:.2f}s): {e}")
            raise

    def _parse_analysis_response(self, news_id: str, response: str) -> AnalysisResult:
        """解析 API 响应，包含多层结构提取与清洗"""
        try:
            cleaned = response.strip()

            # 去除可能的 Markdown 标记包围
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

            start_idx = cleaned.find("{")
            end_idx = cleaned.rfind("}") + 1

            if start_idx == -1 or end_idx == 0:
                raise ValueError("响应中未找到有效的 JSON 结构")

            json_str = cleaned[start_idx:end_idx]
            data = json.loads(json_str)

            impact_score = float(data.get("impact_score", 50))
            impact_score = max(0.0, min(100.0, impact_score))

            return AnalysisResult(
                news_id=news_id,
                impact_score=impact_score,
                summary=data.get("summary", "无摘要"),
                analysis_time=datetime.now(),
            )

        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"解析 API 响应失败: {e}, 原始响应: {response[:200]}...")
            raise ValueError(f"AI 响应解析失败: {e}")

    def _save_analysis_result(self, result: AnalysisResult) -> bool:
        """保存分析结果到数据库"""
        try:
            db_path = (
                self.config.get("database", {})
                .get("sqlite", {})
                .get("db_path", "data/news.db")
            )

            # 确保目录存在
            os.makedirs(os.path.dirname(db_path), exist_ok=True)

            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                data = result.to_dict()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO analysis_results 
                    (news_id, impact_score, summary, analysis_time)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        data["news_id"],
                        data["impact_score"],
                        data["summary"],
                        data["analysis_time"],
                    ),
                )
                conn.commit()
                return True

        except Exception as e:
            logger.error(f"保存分析结果到 DB 失败: {e}")
            return False

    def format_analysis_report(self, results: List[AnalysisResult]) -> str:
        """格式化分析报告（基准分为 50）"""
        if not results:
            return "暂无分析结果"

        report = f"""
# A股新闻影响分析报告

**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**分析新闻数量**: {len(results)}

## 整体概况
"""
        # 以 50 分为基准分类
        positive_count = sum(1 for r in results if r.impact_score > 50)
        negative_count = sum(1 for r in results if r.impact_score < 50)
        neutral_count = sum(1 for r in results if r.impact_score == 50)

        report += f"""
- **偏正面新闻 (>50分)**: {positive_count} 条
- **偏负面新闻 (<50分)**: {negative_count} 条  
- **中性/无明显影响 (=50分)**: {neutral_count} 条
"""

        # 过滤出高影响新闻 (绝对值偏离 50 超过 20 分的案例)
        high_impact_results = [r for r in results if abs(r.impact_score - 50) >= 20]
        if high_impact_results:
            report += f"\n## 高影响新闻列表 ({len(high_impact_results)}条)\n\n"

            for i, result in enumerate(high_impact_results[:5], 1):
                trend = "利好" if result.impact_score > 50 else "利空"
                report += f"""
### {i}. 评分: {result.impact_score:.1f} ({trend}) | ID: {result.news_id}

**摘要**: {result.summary}

---
"""

        return report

    def get_stats(self) -> Dict[str, Any]:
        """获取分析器统计数据"""
        with self._stats_lock:
            return {
                **self.stats,
                "last_analysis_time": datetime.now().isoformat(),
                "api_available": self.client is not None,
            }

    def analyze_news_batch(
        self, news_items: List[NewsItem], max_workers: Optional[int] = None
    ) -> List[AnalysisResult]:
        """并发并行分析多条新闻"""
        if not news_items:
            return []

        if max_workers is None:
            analysis_params = self.config.get("ai_analysis", {}).get("analysis_params", {})
            max_workers = analysis_params.get("max_concurrent", 5)

        logger.info(f"开始并行分析 {len(news_items)} 条新闻，最大并发线程: {max_workers}")

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_news = {
                executor.submit(self.analyze_news, news_item): news_item
                for news_item in news_items
            }

            for future in as_completed(future_to_news):
                news_item = future_to_news[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"批量任务单条处理失败 [{news_item.title[:30]}...]: {e}")
                    continue

        logger.info(f"并发分析完成，成功处理 {len(results)}/{len(news_items)} 条")
        return results


if __name__ == "__main__":
    import sys

    provider = sys.argv[1] if len(sys.argv) > 1 else "openrouter"
    if provider not in ["deepseek", "openrouter"]:
        print("仅支持提供商: deepseek, openrouter")
        sys.exit(1)

    print(f"当前选定 API 提供商: {provider}")
    analyzer = AIAnalyzer(provider=provider)
    
    # 示例调用
    news_list = db_manager.get_news_items(limit=5)
    if not news_list:
        print("未在数据库中找到待分析新闻")
    else:
        results = analyzer.analyze_news_batch(news_list)
        report = analyzer.format_analysis_report(results)
        print(report)
        print("统计数据:", analyzer.get_stats())
