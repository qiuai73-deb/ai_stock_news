"""
钉钉自定义机器人推送模块
通过群机器人 Webhook（支持加签）将 AI 新闻分析报告推送到钉钉群。

配置（config.yaml 或环境变量）：
  notification:
    channel: dingtalk
    dingtalk:
      webhook: "https://oapi.dingtalk.com/robot/send?access_token=xxxx"
      secret:  "SECxxxxxxxx"          # 加签密钥，可留空（不推荐）
环境变量优先级更高：DINGTALK_WEBHOOK / DINGTALK_SECRET
"""

import os
import time
import hmac
import hashlib
import base64
import json
import urllib.parse
from typing import List, Dict, Any, Optional
from datetime import datetime

import requests

from .utils.logger import get_logger
from .ai.ai_analyzer import AnalysisResult

logger = get_logger('dingtalk_notifier')

# 钉钉自定义机器人 markdown 正文长度上限（字符）
DINGTALK_MARKDOWN_LIMIT = 18000
# 单条推送最多展示的新闻条数（避免超限）
MAX_NEWS_PER_MESSAGE = 15


class DingTalkNotifier:
    """钉钉群机器人推送器（替代 EmailSender 发送报告）"""

    def __init__(self, config_path: str = None):
        self.config = self._load_config(config_path)
        # 解析 ${ENV} 占位符（与 email_sender 一致）
        self._resolve_env_vars(self.config)

        nt_cfg = self.config.get('notification', {})
        dt_cfg = nt_cfg.get('dingtalk', {})

        # 环境变量优先（与 GitHub Actions secret 注入一致），其次配置（已做 ${ENV} 解析）
        self.webhook = os.getenv('DINGTALK_WEBHOOK') or (dt_cfg.get('webhook') or '')
        self.secret = os.getenv('DINGTALK_SECRET') or (dt_cfg.get('secret') or '')
        # 二次兜底：若仍残留 ${...} 占位符，视为未配置
        if isinstance(self.webhook, str) and self.webhook.strip().startswith('${'):
            self.webhook = ''
        if isinstance(self.secret, str) and self.secret.strip().startswith('${'):
            self.secret = ''
        self.timeout = int(dt_cfg.get('timeout', 15))

        # 占位符判定（避免拿字面量去请求而误报）
        placeholders = ['DINGTALK_WEBHOOK', 'DINGTALK_SECRET', 'YOUR_DINGTALK_WEBHOOK',
                        'SEC', '']
        if not self.webhook or self.webhook in placeholders or 'YOUR_' in str(self.webhook).upper():
            logger.warning("未配置有效的钉钉 Webhook，推送将降级为仅记录日志（不发送）")
            self.webhook = ''
            self.available = False
        else:
            self.available = True

        self.stats = {'sent': 0, 'failed': 0, 'last_send_time': None}

    def _load_config(self, config_path: Optional[str]) -> dict:
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
        try:
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return {}

    def _resolve_env_vars(self, obj):
        """递归解析 ${ENV} 形式的环境变量占位符"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                obj[key] = self._resolve_env_vars(value)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                obj[i] = self._resolve_env_vars(item)
        elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
            env_var = obj[2:-1]
            return os.getenv(env_var, obj)
        return obj

    # ------------------------------------------------------------------ #
    # 对外接口（与 EmailSender 保持一致的调用方式）
    # ------------------------------------------------------------------ #
    def send_analysis_report(self,
                             analysis_results: List[AnalysisResult],
                             title: str = None) -> bool:
        """发送基于 AnalysisResult 的分析报告（对应 EmailSender.send_analysis_report）"""
        if not analysis_results:
            logger.warning("没有分析结果，跳过钉钉推送")
            return False
        if not self.available:
            logger.warning("钉钉不可用（未配置 Webhook），跳过推送")
            return False

        # 关联数据库获取新闻详情
        from .utils.database import DatabaseManager
        db_manager = DatabaseManager()
        news_details = {}
        for r in analysis_results:
            item = db_manager.get_news_item_by_id(r.news_id)
            if item:
                news_details[r.news_id] = item

        sorted_results = sorted(
            analysis_results, key=lambda x: abs(x.impact_score), reverse=True
        )
        md_title, md_text = self._build_markdown_from_results(sorted_results, news_details, title)
        return self._post_markdown(md_title, md_text)

    def send_news_digest(self,
                         news_list,
                         title: str = "",
                         stats: Dict[str, int] = None) -> bool:
        """发送基于 NewsItem 列表的摘要（对应即时/每日汇总邮件）"""
        if not news_list:
            logger.warning("没有新闻数据，跳过钉钉推送")
            return False
        if not self.available:
            logger.warning("钉钉不可用（未配置 Webhook），跳过推送")
            return False

        sorted_news = sorted(news_list, key=lambda x: getattr(x, 'importance_score', 0), reverse=True)
        md_title, md_text = self._build_markdown_from_news(sorted_news, title, stats)
        return self._post_markdown(md_title, md_text)

    def send_simple_message(self, text: str, title: str = "AI新闻助手") -> bool:
        """发送纯文本消息（便于测试/告警）"""
        if not self.available:
            logger.warning("钉钉不可用（未配置 Webhook），跳过推送")
            return False
        return self._post_markdown(title, text)

    def test_connection(self) -> bool:
        """测试钉钉连接"""
        if not self.available:
            logger.error("钉钉未配置 Webhook，无法测试")
            return False
        ok = self._post_markdown(
            "📤 钉钉推送测试",
            "✅ 钉钉自定义机器人连接成功！\n> 来自 AI 新闻收集与影响分析系统"
        )
        if ok:
            logger.info("钉钉连接测试成功")
        else:
            logger.error("钉钉连接测试失败")
        return ok

    # ------------------------------------------------------------------ #
    # 内部：构造 markdown
    # ------------------------------------------------------------------ #
    def _build_markdown_from_results(self, results, news_details, title=None) -> (str, str):
        total = len(results)
        positive = sum(1 for r in results if r.impact_score > 5)
        negative = sum(1 for r in results if r.impact_score < -5)
        neutral = total - positive - negative

        md_title = title or f"📊 AI新闻分析报告（{total}条）"
        lines = [f"# 🤖 AI新闻影响分析报告",
                 f"> 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}",
                 f"> 共分析 **{total}** 条 ｜ 正面 {positive} ｜ 负面 {negative} ｜ 中性 {neutral}",
                 "---"]

        for r in results[:MAX_NEWS_PER_MESSAGE]:
            item = news_details.get(r.news_id)
            if not item:
                continue
            lines.append(self._news_block(item, impact_score=r.impact_score))
            if len("\n".join(lines)) > DINGTALK_MARKDOWN_LIMIT:
                break

        return md_title, "\n".join(lines)

    def _build_markdown_from_news(self, news_list, title="", stats=None) -> (str, str):
        if stats:
            md_title = title or f"📊 每日新闻汇总（{stats.get('total', len(news_list))}条）"
            lines = [f"# 📊 每日新闻汇总",
                     f"> {datetime.now().strftime('%Y年%m月%d日')}",
                     f"> 总新闻 **{stats.get('total', 0)}** 条 ｜ 高重要性 {stats.get('high', 0)} ｜ "
                     f"中等 {stats.get('medium', 0)} ｜ 低 {stats.get('low', 0)} ｜ "
                     f"平均重要性 {stats.get('avg_score', 0):.1f}",
                     "---"]
        else:
            md_title = title or f"📰 即时新闻（{len(news_list)}条）"
            lines = [f"# 📰 即时新闻报告",
                     f"> {datetime.now().strftime('%Y-%m-%d %H:%M')} ｜ 共 {len(news_list)} 条",
                     "---"]

        for item in news_list[:MAX_NEWS_PER_MESSAGE]:
            lines.append(self._news_block(item))
            if len("\n".join(lines)) > DINGTALK_MARKDOWN_LIMIT:
                lines.append("\n> ……（更多新闻已省略，详见数据库）")
                break

        return md_title, "\n".join(lines)

    def _news_block(self, item, impact_score=None) -> str:
        """单条新闻的 markdown 片段"""
        title = getattr(item, 'title', '无标题')
        url = getattr(item, 'url', '') or ''
        source = getattr(item, 'source', '') or ''
        score = getattr(item, 'importance_score', 0)
        content = getattr(item, 'content', '') or ''
        summary = getattr(item, 'summary', '') or ''
        pt = getattr(item, 'publish_time', None)

        if score >= 70:
            level, emoji = "高", "🔴"
        elif score >= 40:
            level, emoji = "中", "🟡"
        else:
            level, emoji = "低", "🟢"

        time_str = pt.strftime('%m-%d %H:%M') if pt else ''

        title_line = f"### {emoji} {title}"
        if url:
            title_line = f"### {emoji} [{title}]({url})"

        block = [title_line,
                 f"> 重要性 **{score}分({level})** ｜ 来源：{source} ｜ {time_str}"]

        # 展示 AI 摘要或正文片段
        snippet = (summary or content or '')[:120]
        if snippet:
            block.append(f"\n{snippet}…")

        # 深度分析
        deep = getattr(item, 'deep_analysis_report', '') or ''
        if deep:
            block.append(f"\n> 🔍 深度分析：{deep[:120]}…")

        block.append("")
        return "\n".join(block)

    # ------------------------------------------------------------------ #
    # 内部：签名 + 发送
    # ------------------------------------------------------------------ #
    def _sign_url(self) -> str:
        """根据 secret 生成带加签的 webhook 地址"""
        url = self.webhook
        if not self.secret:
            return url
        timestamp = str(round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            self.secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            digestmod=hashlib.sha256
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        sep = '&' if '?' in url else '?'
        return f"{url}{sep}timestamp={timestamp}&sign={sign}"

    def _post_markdown(self, title: str, text: str) -> bool:
        """发送 markdown 消息到钉钉"""
        if not self.available:
            return False
        # 超长截断保护
        if len(text) > DINGTALK_MARKDOWN_LIMIT:
            text = text[:DINGTALK_MARKDOWN_LIMIT] + "\n\n> ……（内容过长已截断）"

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": title[:50],
                "text": text
            }
        }
        try:
            resp = requests.post(
                self._sign_url(),
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=self.timeout
            )
            data = resp.json()
            if data.get('errcode', -1) == 0:
                self.stats['sent'] += 1
                self.stats['last_send_time'] = datetime.now().isoformat()
                logger.info(f"钉钉推送成功：{title}")
                return True
            else:
                self.stats['failed'] += 1
                logger.error(f"钉钉推送失败：errcode={data.get('errcode')} errmsg={data.get('errmsg')}")
                return False
        except Exception as e:
            self.stats['failed'] += 1
            logger.error(f"钉钉推送异常：{e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self.stats,
            'available': self.available,
            'webhook_configured': bool(self.webhook)
        }


# 便捷函数
def send_analysis_report_dingtalk(analysis_results: List[AnalysisResult],
                                  title: str = None) -> bool:
    notifier = DingTalkNotifier()
    return notifier.send_analysis_report(analysis_results, title)


if __name__ == "__main__":
    n = DingTalkNotifier()
    print("钉钉可用:", n.available)
    if n.test_connection():
        print("✅ 钉钉连接测试成功")
    else:
        print("❌ 钉钉连接测试失败（请检查 Webhook / Secret 配置）")
