"""
定时调度模块
实现自动化的新闻收集、AI分析和邮件发送任务调度
包含高级管理、监控和错误恢复功能
"""

import os
import json
import time
import threading
from datetime import datetime, timedelta
from datetime import time as dt_time
from typing import Dict, Any, Optional, Callable, List
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import yaml
import signal
import sys

from .collectors.news_collector import NewsCollector
from .ai.ai_analyzer import AIAnalyzer
from .ai.importance_analyzer import ImportanceAnalyzer
from .ai.deep_analyzer import DeepAnalyzer
from .email_sender import EmailSender
from .dingtalk_notifier import DingTalkNotifier
from .utils.logger import get_logger
from .utils.database import db_manager

logger = get_logger('scheduler')


class TaskScheduler:
    """任务调度器 - 集成管理、监控和错误恢复功能"""
    
    def __init__(self, config_path: str = None, state_file: str = None):
        """
        初始化任务调度器
        
        Args:
            config_path: 配置文件路径
            state_file: 状态保存文件路径
        """
        self.config = self._load_config(config_path)
        self.scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
        self.is_running = False
        self.jobs = {}
        
        # 管理器功能 - 状态管理
        self.config_path = config_path
        self.state_file = state_file or 'data/scheduler_state.json'
        self.start_time = None
        self.error_count = 0
        self.last_error_time = None
        
        # 任务执行历史
        self.execution_history = []
        self.max_history_size = 100
        
        # 线程安全锁
        self._lock = threading.Lock()
        
        # 健康监控
        self.health_status = {
            'overall': 'unknown',
            'components': {},
            'last_check': None
        }
        
        # 监控线程
        self.monitor_thread = None
        self.monitor_interval = 60  # 监控间隔（秒）
        
        # 统计信息
        self.stats = {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'last_execution_time': None,
            'next_execution_time': None,
            'uptime_start': None
        }
        
        # 组件实例
        self.news_collector = None
        self.ai_analyzer = None
        self.importance_analyzer = None
        self.deep_analyzer = None
        self.email_sender = None
        self.notifier = None  # 统一通知器（钉钉/邮件，按 notification.channel 选择）
        
        # 设置事件监听器和信号处理器
        self._setup_event_listeners()
        self._setup_signal_handlers()
    
    def _load_config(self, config_path: Optional[str]) -> dict:
        """加载配置文件"""
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}
                
            # 处理环境变量
            self._resolve_env_vars(config)
            return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return {}
    
    def _resolve_env_vars(self, obj):
        """递归解析环境变量"""
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
    
    def _setup_event_listeners(self):
        """设置事件监听器"""
        self.scheduler.add_listener(
            self._job_executed_listener,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
        )
    
    def _setup_signal_handlers(self):
        """设置信号处理器，优雅关闭"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _filter_news_by_score(self, news_list: List, min_score: int) -> List:
        """
        根据重要性分数过滤新闻
        
        Args:
            news_list: 新闻列表
            min_score: 最低分数阈值
            
        Returns:
            List: 过滤后的新闻列表
        """
        if not news_list:
            return []
        
        try:
            # 确保所有新闻都有重要性分数
            filtered_news = []
            for news in news_list:
                if hasattr(news, 'importance_score') and isinstance(news.importance_score, (int, float)):
                    if news.importance_score >= min_score:
                        filtered_news.append(news)
                else:
                    logger.warning(f"新闻缺少有效的重要性分数: {getattr(news, 'title', 'unknown')}")
            
            if len(filtered_news) != len(news_list):
                logger.debug(f"分数过滤: {len(news_list)} -> {len(filtered_news)} (阈值: {min_score})")
            
            return filtered_news
            
        except Exception as e:
            logger.error(f"过滤新闻时出错: {e}")
            return []
    
    def _calculate_news_stats(self, news_list: List) -> Dict[str, int]:
        """
        计算新闻统计信息
        
        Args:
            news_list: 新闻列表
            
        Returns:
            Dict: 包含各种统计信息的字典
        """
        if not news_list:
            return {'total': 0, 'high': 0, 'medium': 0, 'low': 0, 'avg_score': 0}
        
        total_count = len(news_list)
        high_count = 0
        medium_count = 0
        low_count = 0
        total_score = 0
        
        # 单次遍历计算所有统计信息
        for news in news_list:
            score = news.importance_score
            total_score += score
            
            if score >= 70:
                high_count += 1
            elif score >= 50:
                medium_count += 1
            else:
                low_count += 1
        
        avg_score = total_score / total_count
        
        return {
            'total': total_count,
            'high': high_count,
            'medium': medium_count,
            'low': low_count,
            'avg_score': avg_score
        }
    
    def _validate_config(self) -> bool:
        """
        验证配置文件的完整性
        
        Returns:
            bool: 配置是否有效
        """
        try:
            required_sections = ['email', 'scheduler']
            
            for section in required_sections:
                if section not in self.config:
                    logger.error(f"配置文件缺少必需的部分: {section}")
                    return False
            
            # 验证邮件配置
            email_config = self.config.get('email', {})
            smtp_config = email_config.get('smtp', {})
            
            required_email_fields = ['username', 'password', 'server', 'port']
            for field in required_email_fields:
                if not smtp_config.get(field):
                    logger.warning(f"邮件配置缺少字段: {field}")
            
            # 验证收件人列表
            recipients = email_config.get('recipients', [])
            if not recipients:
                logger.warning("未配置邮件收件人")
            
            logger.info("配置验证完成")
            return True
            
        except Exception as e:
            logger.error(f"配置验证失败: {e}")
            return False
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"接收到信号 {signum}，正在优雅关闭...")
        
        # 确保状态正确保存
        self.is_running = False
        self.record_event('signal_received', True, f"接收到信号 {signum}，开始关闭")
        self.save_state()
        
        self.stop()
        sys.exit(0)
    
    def _job_executed_listener(self, event):
        """任务执行事件监听器"""
        with self._lock:
            self.stats['total_executions'] += 1
            self.stats['last_execution_time'] = datetime.now().isoformat()
            
            if event.exception:
                self.stats['failed_executions'] += 1
            else:
                self.stats['successful_executions'] += 1
        
        # 记录事件（使用独立的锁）
        if event.exception:
            logger.error(f"任务执行失败: {event.job_id}, 异常: {event.exception}")
            self.record_event('job_failed', False, f"任务 {event.job_id} 执行失败: {event.exception}")
        else:
            logger.info(f"任务执行成功: {event.job_id}")
            self.record_event('job_executed', True, f"任务 {event.job_id} 执行成功")
    
    # ===== 状态管理功能 =====
    
    def save_state(self):
        """保存调度器状态"""
        try:
            # 确保目录存在
            state_dir = os.path.dirname(self.state_file)
            if state_dir:
                os.makedirs(state_dir, exist_ok=True)
            
            state = {
                'is_running': self.is_running,
                'start_time': self.start_time,
                'error_count': self.error_count,
                'last_error_time': self.last_error_time,
                'execution_history': self.execution_history[-50:],  # 只保存最近50条
                'health_status': self.health_status,
                'stats': self.stats,
                'saved_at': datetime.now().isoformat(),
                'process_id': os.getpid(),  # 添加进程ID
                'save_reason': 'normal_operation'  # 保存原因
            }
            
            # 原子性写入：先写到临时文件，然后重命名
            temp_file = self.state_file + '.tmp'
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
            
            # 原子性移动
            if os.path.exists(self.state_file):
                backup_file = self.state_file + '.backup'
                if os.path.exists(backup_file):
                    os.remove(backup_file)
                os.rename(self.state_file, backup_file)
            
            os.rename(temp_file, self.state_file)
                
            logger.debug(f"调度器状态已保存 (PID: {os.getpid()})")
            
        except Exception as e:
            logger.error(f"保存状态失败: {e}")
            # 尝试清理临时文件
            try:
                temp_file = self.state_file + '.tmp'
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except OSError:
                pass
    
    def load_state(self):
        """加载调度器状态"""
        try:
            if not os.path.exists(self.state_file):
                logger.info("状态文件不存在，使用默认状态")
                return
            
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            self.error_count = state.get('error_count', 0)
            self.last_error_time = state.get('last_error_time')
            self.execution_history = state.get('execution_history', [])
            self.health_status = state.get('health_status', {
                'overall': 'unknown',
                'components': {},
                'last_check': None
            })
            
            # 恢复统计信息
            saved_stats = state.get('stats', {})
            for key, value in saved_stats.items():
                if key in self.stats:
                    self.stats[key] = value
            
            logger.info("调度器状态已加载")
            
        except Exception as e:
            logger.error(f"加载状态失败: {e}")
    
    def record_event(self, event_type: str, success: bool, message: str):
        """记录事件"""
        event = {
            'timestamp': datetime.now().isoformat(),
            'type': event_type,
            'success': success,
            'message': message
        }
        
        with self._lock:
            self.execution_history.append(event)
        
        # 限制历史记录数量
        if len(self.execution_history) > self.max_history_size:
            self.execution_history = self.execution_history[-self.max_history_size:]
        
        logger.debug(f"记录事件: {event_type} - {message}")
    
    # ===== 监控功能 =====
    
    def start_monitoring(self):
        """启动监控线程"""
        if self.monitor_thread and self.monitor_thread.is_alive():
            return
        
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="SchedulerMonitor"
        )
        self.monitor_thread.start()
        
        logger.info("监控线程已启动")
    
    def stop_monitoring(self):
        """停止监控线程"""
        if self.monitor_thread and self.monitor_thread.is_alive():
            # 监控线程会在下一个循环检查is_running状态
            logger.info("监控线程将在下次检查时停止")
    
    def _monitor_loop(self):
        """监控循环"""
        while self.is_running:
            try:
                # 执行健康检查
                self.check_health()
                
                # 检查错误恢复
                self.check_error_recovery()
                
                # 保存状态
                self.save_state()
                
                # 等待下次检查
                time.sleep(self.monitor_interval)
                
            except Exception as e:
                logger.error(f"监控循环错误: {e}")
                time.sleep(30)  # 出错时等待更长时间
    
    def check_health(self):
        """健康检查"""
        try:
            logger.debug("执行健康检查...")
            
            # 检查调度器状态
            scheduler_healthy = self.is_running
            
            # 检查组件状态
            components_status = {
                'scheduler': scheduler_healthy,
                'news_collector': self.news_collector is not None,
                'ai_analyzer': self.ai_analyzer is not None,
                'importance_analyzer': self.importance_analyzer is not None,
                'deep_analyzer': self.deep_analyzer is not None,
                'email_sender': self.email_sender is not None,
                'notifier': self.notifier is not None
            }
            
            # 检查任务执行状态
            recent_failures = self.stats.get('failed_executions', 0)
            total_executions = self.stats.get('total_executions', 0)
            
            # 计算健康分数
            failure_rate = recent_failures / max(total_executions, 1)
            
            if failure_rate > 0.5:
                overall_status = 'critical'
            elif failure_rate > 0.2:
                overall_status = 'warning'
            elif all(components_status.values()):
                overall_status = 'healthy'
            else:
                overall_status = 'degraded'
            
            self.health_status = {
                'overall': overall_status,
                'components': components_status,
                'last_check': datetime.now().isoformat(),
                'failure_rate': failure_rate,
                'stats': self.stats.copy()
            }
            
            # 记录健康状态变化
            if overall_status != 'healthy':
                self.record_event(
                    'health_check',
                    overall_status == 'healthy',
                    f"健康状态: {overall_status}, 失败率: {failure_rate:.2%}"
                )
            
            logger.debug(f"健康检查完成: {overall_status}")
            
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            self.health_status['overall'] = 'error'
    
    def check_error_recovery(self):
        """检查是否需要错误恢复"""
        try:
            # 如果健康状态为critical，考虑重启
            if self.health_status.get('overall') == 'critical':
                logger.warning("检测到严重健康问题，考虑重启调度器")
                
                # 检查最近是否已经重启过
                recent_restarts = [
                    event for event in self.execution_history[-10:]
                    if event.get('type') == 'scheduler_restarted'
                    and datetime.fromisoformat(event['timestamp']) > datetime.now() - timedelta(hours=1)
                ]
                
                if len(recent_restarts) < 3:  # 1小时内最多重启3次
                    logger.info("执行自动恢复重启...")
                    self.restart()
                    self.record_event('scheduler_restarted', True, "自动恢复重启")
                else:
                    logger.error("重启次数过多，停止自动恢复")
                    self.record_event('auto_recovery_disabled', False, "重启次数超限")
            
        except Exception as e:
            logger.error(f"错误恢复检查失败: {e}")
    
    # ===== 仪表板和状态功能 =====
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """获取仪表板数据"""
        return {
            'manager_status': {
                'is_running': self.is_running,
                'start_time': self.start_time,
                'uptime': self._calculate_uptime(),
                'error_count': self.error_count,
                'last_error_time': self.last_error_time
            },
            'scheduler_stats': self.stats.copy(),
            'health_status': self.health_status,
            'recent_events': self.execution_history[-10:],
            'jobs_info': self._get_jobs_info()
        }
    
    def _calculate_uptime(self) -> Optional[str]:
        """计算运行时间"""
        if not self.start_time:
            return None
        
        start_dt = datetime.fromisoformat(self.start_time)
        uptime = datetime.now() - start_dt
        
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        
        return f"{days}天 {hours}小时 {minutes}分钟"
    
    def _get_jobs_info(self) -> List[Dict[str, Any]]:
        """获取任务信息"""
        try:
            jobs_info = []
            
            for job in self.scheduler.get_jobs():
                jobs_info.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                    'trigger': str(job.trigger)
                })
            
            return jobs_info
            
        except Exception as e:
            logger.error(f"获取任务信息失败: {e}")
            return []
    
    def get_status(self) -> Dict[str, Any]:
        """获取调度器状态（兼容性方法）"""
        try:
            dashboard = self.get_dashboard_data()
            
            # 简化状态信息
            jobs_list = []
            for job in dashboard['jobs_info']:
                jobs_list.append(f"{job['name']} (下次执行: {job.get('next_run_time', '未知')})")
            
            return {
                'running': self.is_running,
                'start_time': self.start_time,
                'uptime': dashboard['manager_status']['uptime'],
                'job_count': len(dashboard['jobs_info']),
                'jobs': jobs_list,
                'health': dashboard['health_status']['overall'],
                'error_count': self.error_count
            }
        except Exception as e:
            logger.error(f"获取状态失败: {e}")
            return {
                'running': False,
                'error': str(e)
            }
    
    def run_with_ui(self):
        """运行调度器并显示状态信息"""
        try:
            logger.info("启动调度器监控模式...")
            
            while self.is_running:
                # 清屏并显示状态
                os.system('cls' if os.name == 'nt' else 'clear')
                
                print("=" * 80)
                print("📊 AI新闻收集与影响分析系统 - 调度器监控")
                print("=" * 80)
                
                dashboard = self.get_dashboard_data()
                
                # 显示管理器状态
                print(f"\n🔧 管理器状态:")
                print(f"  运行状态: {'🟢 运行中' if self.is_running else '🔴 已停止'}")
                print(f"  运行时间: {dashboard['manager_status']['uptime'] or '未知'}")
                print(f"  错误次数: {dashboard['manager_status']['error_count']}")
                
                # 显示健康状态
                health = dashboard['health_status']
                status_emoji = {
                    'healthy': '🟢',
                    'warning': '🟡',
                    'critical': '🔴',
                    'error': '⚫'
                }.get(health.get('overall', 'unknown'), '❓')
                
                print(f"\n💊 健康状态: {status_emoji} {health.get('overall', '未知')}")
                
                # 显示调度器统计
                stats = dashboard['scheduler_stats']
                print(f"\n📈 执行统计:")
                print(f"  总执行次数: {stats.get('total_executions', 0)}")
                print(f"  成功次数: {stats.get('successful_executions', 0)}")
                print(f"  失败次数: {stats.get('failed_executions', 0)}")
                print(f"  上次执行: {stats.get('last_execution_time', '未知')}")
                
                # 显示活动任务
                jobs = dashboard['jobs_info']
                print(f"\n⏰ 活动任务 ({len(jobs)}个):")
                for job in jobs:
                    next_run = job['next_run_time']
                    if next_run:
                        next_run = datetime.fromisoformat(next_run).strftime('%H:%M:%S')
                    print(f"  📋 {job['name']} - 下次执行: {next_run or '未知'}")
                
                # 显示最近事件
                events = dashboard['recent_events']
                print(f"\n📝 最近事件:")
                for event in events[-5:]:
                    timestamp = datetime.fromisoformat(event['timestamp']).strftime('%H:%M:%S')
                    status = '✅' if event['success'] else '❌'
                    print(f"  {timestamp} {status} {event['message']}")
                
                print(f"\n按 Ctrl+C 停止调度器")
                print("=" * 80)
                
                time.sleep(5)  # 每5秒刷新一次
                
        except KeyboardInterrupt:
            print("\n接收到停止信号...")
        finally:
            self.stop()
    
    def initialize_components(self):
        """初始化组件"""
        try:
            logger.info("初始化组件...")
            
            self.news_collector = NewsCollector()
            self.ai_analyzer = AIAnalyzer()
            self.importance_analyzer = ImportanceAnalyzer()
            self.deep_analyzer = DeepAnalyzer()
            self.email_sender = EmailSender()
            self.notifier = self._build_notifier()

            logger.info("组件初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"组件初始化失败: {e}")
            return False

    def _build_notifier(self):
        """
        根据 notification.channel 构建统一通知器
        - dingtalk : 钉钉自定义机器人（默认，替代邮件）
        - email    : 保留原邮件发送
        """
        channel = self.config.get('notification', {}).get('channel', 'dingtalk')
        if channel == 'email':
            logger.info("通知渠道：邮件（email）")
            return EmailSender()
        # 默认钉钉
        try:
            notifier = DingTalkNotifier()
            logger.info(f"通知渠道：钉钉（dingtalk），可用={notifier.available}")
            return notifier
        except Exception as e:
            logger.error(f"钉钉通知初始化失败，回退到邮件：{e}")
            return EmailSender()

    def add_news_collection_job(self, interval_minutes: int = None):
        """
        添加新闻收集任务
        
        Args:
            interval_minutes: 执行间隔（分钟），默认从配置读取
        """
        if interval_minutes is None:
            interval_minutes = self.config.get('news_collection', {}).get('collection_interval', 30)
        
        job = self.scheduler.add_job(
            func=self._news_collection_task,
            trigger=IntervalTrigger(minutes=interval_minutes),
            id='news_collection',
            name='新闻收集任务',
            max_instances=1,  # 避免重复执行
            coalesce=True,    # 合并延迟的任务
            misfire_grace_time=300  # 5分钟宽限期
        )
        
        self.jobs['news_collection'] = job
        logger.info(f"新闻收集任务已添加，执行间隔: {interval_minutes} 分钟")
    
    def add_analysis_and_email_job(self, 
                                  analysis_interval_minutes: int = None,
                                  email_cron: str = None):
        """
        添加分析和邮件发送任务
        
        Args:
            analysis_interval_minutes: 分析间隔（分钟）
            email_cron: 邮件发送的cron表达式，如 "0 9,18 * * *" 表示每天9点和18点
        """
        # 分析任务
        if analysis_interval_minutes is None:
            analysis_interval_minutes = self.config.get('scheduler', {}).get('analysis_interval', 60)
        
        analysis_job = self.scheduler.add_job(
            func=self._analysis_task,
            trigger=IntervalTrigger(minutes=analysis_interval_minutes),
            id='ai_analysis',
            name='AI分析任务',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600  # 10分钟宽限期
        )
        
        self.jobs['ai_analysis'] = analysis_job
        logger.info(f"AI分析任务已添加，执行间隔: {analysis_interval_minutes} 分钟")
        
        # 邮件发送任务
        if email_cron is None:
            email_cron = self.config.get('scheduler', {}).get('email_cron', '0 9,18 * * *')
        
        email_job = self.scheduler.add_job(
            func=self._email_task,
            trigger=CronTrigger.from_crontab(email_cron),
            id='email_report',
            name='邮件报告任务',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800  # 30分钟宽限期
        )
        
        self.jobs['email_report'] = email_job
        logger.info(f"邮件报告任务已添加，执行计划: {email_cron}")
    
    def add_full_pipeline_job(self, interval_minutes: int = None):
        """
        添加完整流程任务（收集+分析+邮件）
        
        Args:
            interval_minutes: 执行间隔（分钟）
        """
        if interval_minutes is None:
            interval_minutes = self.config.get('scheduler', {}).get('pipeline_interval', 120)
        
        job = self.scheduler.add_job(
            func=self._full_pipeline_task,
            trigger=IntervalTrigger(minutes=interval_minutes),
            id='full_pipeline',
            name='完整流程任务',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=900  # 15分钟宽限期
        )
        
        self.jobs['full_pipeline'] = job
        logger.info(f"完整流程任务已添加，执行间隔: {interval_minutes} 分钟")
    
    def add_enhanced_strategy_jobs(self):
        """添加增强版调度策略任务"""
        strategy_config = self.config.get('scheduler', {}).get('strategy', {})
        
        # 1. 早上8点收集并发送邮件
        morning_config = strategy_config.get('morning_collection', {})
        if morning_config.get('enabled', True):
            job = self.scheduler.add_job(
                func=self._morning_collection_with_email,
                trigger=CronTrigger(
                    hour=morning_config.get('hour', 8), 
                    minute=morning_config.get('minute', 0)
                ),
                id='morning_collection',
                name='早上8点收集并发送邮件',
                max_instances=1,
                coalesce=True
            )
            self.jobs['morning_collection'] = job
            logger.info("早上8点收集任务已添加")
        
        # 2. 交易时间每3分钟收集
        trading_config = strategy_config.get('trading_hours', {})
        if trading_config.get('enabled', True):
            job = self.scheduler.add_job(
                func=self._trading_hours_collection,
                trigger=IntervalTrigger(minutes=trading_config.get('interval_minutes', 3)),
                id='trading_hours_collection',
                name='交易时间收集',
                max_instances=1,
                coalesce=True
            )
            self.jobs['trading_hours_collection'] = job
            logger.info(f"交易时间收集任务已添加，间隔: {trading_config.get('interval_minutes', 3)}分钟")
        
        # 3. 晚上10点收集
        evening_config = strategy_config.get('evening_collection', {})
        if evening_config.get('enabled', True):
            job = self.scheduler.add_job(
                func=self._evening_collection_no_email,
                trigger=CronTrigger(
                    hour=evening_config.get('hour', 22), 
                    minute=evening_config.get('minute', 0)
                ),
                id='evening_collection',
                name='晚上10点收集',
                max_instances=1,
                coalesce=True
            )
            self.jobs['evening_collection'] = job
            logger.info("晚上10点收集任务已添加")
        
        # 4. 每日汇总邮件
        summary_config = strategy_config.get('daily_summary', {})
        if summary_config.get('enabled', True):
            job = self.scheduler.add_job(
                func=self._daily_summary_email,
                trigger=CronTrigger(
                    hour=summary_config.get('hour', 23), 
                    minute=summary_config.get('minute', 30)
                ),
                id='daily_summary',
                name='每日汇总邮件',
                max_instances=1,
                coalesce=True
            )
            self.jobs['daily_summary'] = job
            logger.info("每日汇总邮件任务已添加")

    def add_maintenance_job(self):
        """添加维护任务（数据清理等）"""
        # 每天凌晨3点执行维护任务
        job = self.scheduler.add_job(
            func=self._maintenance_task,
            trigger=CronTrigger(hour=3, minute=0),
            id='maintenance',
            name='维护任务',
            max_instances=1,
            coalesce=True
        )
        
        self.jobs['maintenance'] = job
        logger.info("维护任务已添加，执行时间: 每天凌晨3点")
    
    def _news_collection_task(self):
        """新闻收集任务"""
        logger.info("=== 开始执行新闻收集任务 ===")
        
        try:
            if not self.news_collector:
                self.news_collector = NewsCollector()
            
            start_time = time.time()
            news_list = self.news_collector.collect_all_news()
            end_time = time.time()
            
            duration = end_time - start_time
            logger.info(f"新闻收集完成: {len(news_list)} 条新闻，耗时: {duration:.2f} 秒")
            
            return len(news_list)
            
        except Exception as e:
            logger.error(f"新闻收集任务失败: {e}")
            raise
    
    def _analysis_task(self):
        """AI分析任务（使用增强版并发分析）"""
        logger.info("=== 开始执行AI分析任务（并发模式）===")
        
        try:
            if not self.ai_analyzer:
                self.ai_analyzer = AIAnalyzer()
            
            # 获取未分析的新闻
            news_list = db_manager.get_news_items(limit=20)  # 减少批量大小
            if not news_list:
                logger.info("没有待分析的新闻")
                return 0
            
            start_time = time.time()
            # 使用单条分析逐个处理
            results = []
            for news_item in news_list:
                try:
                    result = self.ai_analyzer.analyze_news(news_item)
                    results.append(result)
                    # 保存分析结果到数据库
                    self.ai_analyzer._save_analysis_result(result)
                except Exception as e:
                    logger.error(f"分析单条新闻失败: {e}")
                    continue
            
            end_time = time.time()
            duration = end_time - start_time
            avg_time = duration / len(results) if results else 0
            logger.info(f"AI分析完成: {len(results)} 条新闻，总耗时: {duration:.2f} 秒，平均: {avg_time:.2f} 秒/条")
            
            return len(results)
            
        except Exception as e:
            logger.error(f"AI分析任务失败: {e}")
            raise
    
    def _email_task(self):
        """报告发送任务（钉钉/邮件，按 notification.channel 选择）"""
        logger.info("=== 开始执行报告发送任务 ===")
        
        try:
            if not self.notifier:
                self.notifier = self._build_notifier()
            
            # 获取最近的分析结果
            recent_hours = self.config.get('scheduler', {}).get('email_recent_hours', 24)
            cutoff_time = datetime.now() - timedelta(hours=recent_hours)
            
            # 这里需要从数据库获取分析结果，暂时使用最新新闻
            news_list = db_manager.get_news_items(limit=10)
            if not news_list:
                logger.info("没有新闻数据，跳过报告发送")
                return False
            
            # 过滤分数低于50的新闻
            filtered_news = self._filter_news_by_score(news_list, 50)
            
            if not filtered_news:
                logger.info(f"有 {len(news_list)} 条新闻，但没有分数达到50分的重要新闻，跳过报告发送")
                return False
            
            # 逐个分析过滤后的新闻用于报告
            results = []
            for news_item in filtered_news[:5]:
                try:
                    result = self.ai_analyzer.analyze_news(news_item)
                    results.append(result)
                except Exception as e:
                    logger.error(f"分析新闻失败: {e}")
                    continue
            
            # 发送报告
            success = self.notifier.send_analysis_report(results)
            
            if success:
                logger.info(f"报告发送任务完成，包含 {len(results)} 条重要新闻（原始 {len(news_list)} 条）")
            else:
                logger.error("报告发送任务失败")
            
            return success
            
        except Exception as e:
            logger.error(f"报告发送任务失败: {e}")
            raise
    
    def _full_pipeline_task(self):
        """完整流程任务"""
        logger.info("=== 开始执行完整流程任务 ===")
        
        try:
            # 1. 新闻收集
            news_count = self._news_collection_task()
            
            # 2. AI分析
            if news_count > 0:
                analysis_count = self._analysis_task()
                
                # 3. 邮件发送（如果有分析结果，会自动过滤分数低于50的新闻）
                if analysis_count > 0:
                    email_success = self._email_task()
                    
                    logger.info(f"完整流程完成: 收集 {news_count} 条新闻，分析 {analysis_count} 条，邮件发送 {'成功' if email_success else '失败'}")
                else:
                    logger.info(f"完整流程完成: 收集 {news_count} 条新闻，无新分析结果")
            else:
                logger.info("完整流程完成: 无新新闻收集")
            
            return True
            
        except Exception as e:
            logger.error(f"完整流程任务失败: {e}")
            raise
    
    def collect_and_analyze_news(self, save_to_db: bool = True) -> List:
        """
        收集新闻并分析重要性
        
        Args:
            save_to_db: 是否保存到数据库
            
        Returns:
            分析后的新闻列表
        """
        try:
            # 1. 收集新闻
            logger.info("开始收集新闻...")
            if not self.news_collector:
                self.news_collector = NewsCollector()
            
            news_list = self.news_collector.collect_all_news()
            
            if not news_list:
                logger.info("没有收集到新的新闻")
                return []
            
            logger.info(f"收集到 {len(news_list)} 条新闻")
            
            # 2. 分析重要性（ImportanceAnalyzer）
            logger.info("开始分析新闻重要性...")
            if not self.importance_analyzer:
                self.importance_analyzer = ImportanceAnalyzer()
            importance_results = self.importance_analyzer.batch_analyze_importance(news_list)
            for news_item, result in zip(news_list, importance_results):
                news_item.importance_score = result.importance_score
                news_item.importance_reasoning = result.reasoning
                news_item.importance_factors = result.key_factors

            # 3. 深度分析（针对高重要性新闻）
            logger.info("开始深度分析高重要性新闻...")
            if not self.deep_analyzer:
                self.deep_analyzer = DeepAnalyzer()
            
            deep_analysis_results = self.deep_analyzer.batch_analyze_deep(news_list)
            
            # 将深度分析结果应用到对应的新闻项
            deep_analysis_dict = {result.news_id: result for result in deep_analysis_results}
            for news_item in news_list:
                if news_item.id in deep_analysis_dict:
                    deep_result = deep_analysis_dict[news_item.id]
                    news_item.update_with_deep_analysis(deep_result)
                    logger.debug(f"应用深度分析结果: {news_item.title[:30]}... -> {deep_result.adjusted_score}分")

            # 4. AI分析以获取影响程度
            logger.info("开始AI影响分析...")
            if not self.ai_analyzer:
                self.ai_analyzer = AIAnalyzer()
            
            # 使用并行分析替代逐个分析
            logger.info(f"开始并行AI分析 {len(news_list)} 条新闻")
            ai_results = self.ai_analyzer.analyze_news_batch(news_list)

            # 5. 将AI分析的影响级别映射到NewsItem.impact_degree
            try:
                # ai_results 与 news_list 一一对应
                for news_item, ar in zip(news_list, ai_results):
                    if hasattr(ar, 'impact_level'):
                        news_item.impact_degree = ar.impact_level
            except Exception as _e:
                logger.warning(f"映射影响程度失败: {_e}")

            # 6. 保存到数据库（包含重要性、深度分析与影响程度）
            if save_to_db:
                saved_count = db_manager.save_news_items_batch(news_list)
                logger.info(f"保存 {saved_count} 条新闻（含重要性、深度分析与影响程度）到数据库")
            
            return news_list
            
        except Exception as e:
            logger.error(f"收集和分析新闻失败: {e}")
            return []
    
    def _morning_collection_with_email(self):
        """早上8点：收集、分析并发送邮件"""
        try:
            logger.info("=== 执行早上8点收集任务 ===")
            
            # 收集和分析新闻
            news_list = self.collect_and_analyze_news()
            
            if news_list:
                # 过滤分数低于50的新闻
                filtered_news = self._filter_news_by_score(news_list, 50)
                
                if filtered_news:
                    # 发送邮件
                    self._send_instant_email(filtered_news, "早间新闻报告")
                    logger.info(f"早间新闻报告发送完成，包含 {len(filtered_news)} 条新闻（原始 {len(news_list)} 条）")
                else:
                    logger.info(f"早间收集到 {len(news_list)} 条新闻，但没有分数达到50分的重要新闻，跳过邮件发送")
                
        except Exception as e:
            logger.error(f"早上8点任务执行失败: {e}")
            raise
    
    def _trading_hours_collection(self):
        """交易时间收集（8:00-16:00）"""
        try:
            current_time = datetime.now().time()
            
            # 只在交易时间执行
            if dt_time(8, 0) <= current_time <= dt_time(16, 0):
                logger.info("=== 执行交易时间收集任务 ===")
                
                # 收集和分析新闻，所有新闻都存入数据库
                news_list = self.collect_and_analyze_news()
                
                if news_list:
                    # 对于交易时间收集的新闻，只有分数>=70的才发送即时邮件
                    high_priority_news = self._filter_news_by_score(news_list, 70)
                    
                    if high_priority_news:
                        # 发送即时邮件
                        self._send_instant_email(high_priority_news, "交易时间重要新闻")
                        logger.info(f"交易时间收集到 {len(news_list)} 条新闻，发送 {len(high_priority_news)} 条高优先级新闻邮件")
                    else:
                        logger.info(f"交易时间收集到 {len(news_list)} 条新闻，但无分数达到70分的重要新闻，已存入数据库，等待晚上汇总")
            else:
                logger.debug("当前不在交易时间，跳过收集")
                
        except Exception as e:
            logger.error(f"交易时间收集任务失败: {e}")
            raise
    
    def _evening_collection_no_email(self):
        """晚上10点：收集但不发送邮件"""
        try:
            logger.info("=== 执行晚上10点收集任务 ===")
            
            # 收集和分析新闻
            news_list = self.collect_and_analyze_news()
            
            if news_list:
                logger.info(f"晚上10点收集到 {len(news_list)} 条新闻，已保存但不发送邮件")
                
        except Exception as e:
            logger.error(f"晚上10点任务执行失败: {e}")
            raise
    
    def _daily_summary_email(self):
        """每日汇总邮件（晚上11:30）"""
        try:
            logger.info("=== 生成每日汇总邮件 ===")
            
            # 获取今天的所有新闻（包括交易时间收集但未即时发送的新闻）
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_news = db_manager.get_news_items_by_date_range(today_start, datetime.now())
            
            if not today_news:
                logger.info("今天没有新闻，跳过汇总邮件")
                return
            
            # 按重要性排序
            sorted_news = sorted(today_news, key=lambda x: x.importance_score, reverse=True)
            
            # 统计信息（在生成报告前计算，避免重复）
            stats = self._calculate_news_stats(sorted_news)
            
            # 生成汇总报告（包含所有新闻，不进行分数过滤，因为这是汇总报告）
            report = self._generate_daily_summary_report(sorted_news, stats)
            
            # 发送报告（钉钉/邮件，优先使用结构化新闻列表 + 统计）
            if not self.notifier:
                self.notifier = self._build_notifier()
            title = f"📊 每日新闻汇总 - {datetime.now().strftime('%Y年%m月%d日')}"
            self.notifier.send_news_digest(sorted_news, title=title, stats=stats)
            
            logger.info(f"每日汇总报告发送成功，包含 {len(sorted_news)} 条新闻（高重要性: {stats['high']}, 中等: {stats['medium']}, 低重要性: {stats['low']}）")
            
        except Exception as e:
            logger.error(f"每日汇总邮件发送失败: {e}")
            raise

    def _maintenance_task(self):
        """维护任务"""
        logger.info("=== 开始执行维护任务 ===")
        
        try:
            # 数据库清理
            retention_days = self.config.get('database', {}).get('retention', {}).get('max_days', 30)
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # 这里可以添加数据库清理逻辑
            logger.info(f"执行数据清理，保留 {retention_days} 天内的数据")
            
            # 日志文件清理
            self._cleanup_logs()
            
            # 系统状态检查
            self._health_check()
            
            logger.info("维护任务完成")
            return True
            
        except Exception as e:
            logger.error(f"维护任务失败: {e}")
            raise
    
    def _cleanup_logs(self):
        """清理日志文件"""
        try:
            log_dir = self.config.get('logging', {}).get('file', 'data/logs/app.log')
            log_dir = os.path.dirname(log_dir)
            
            if not os.path.exists(log_dir):
                return
            
            # 清理7天前的日志文件
            cutoff_time = time.time() - (7 * 24 * 3600)
            
            for filename in os.listdir(log_dir):
                filepath = os.path.join(log_dir, filename)
                if os.path.isfile(filepath) and os.path.getmtime(filepath) < cutoff_time:
                    os.remove(filepath)
                    logger.debug(f"删除旧日志文件: {filename}")
            
            logger.info("日志文件清理完成")
            
        except Exception as e:
            logger.error(f"日志文件清理失败: {e}")
    
    def _send_instant_email(self, news_list, title_prefix: str = ""):
        """发送即时新闻报告（钉钉/邮件）"""
        try:
            if not self.notifier:
                self.notifier = self._build_notifier()
            
            # 过滤分数低于50的新闻
            filtered_news = self._filter_news_by_score(news_list, 50)
            
            if not filtered_news:
                logger.info(f"要发送的 {len(news_list)} 条新闻中没有分数达到50分的，跳过报告发送")
                return
            
            # 按重要性排序
            sorted_news = sorted(filtered_news, key=lambda x: x.importance_score, reverse=True)
            
            # 发送报告
            subject = f"📰 {title_prefix} - {datetime.now().strftime('%H:%M')}"
            self.notifier.send_news_digest(sorted_news, title=subject)
            
            logger.info(f"即时报告发送成功: {title_prefix}，包含 {len(sorted_news)} 条重要新闻（原始 {len(news_list)} 条）")
            
        except Exception as e:
            logger.error(f"发送即时报告失败: {e}")
    
    def _send_summary_email(self, report: str):
        """发送汇总邮件"""
        try:
            if not self.email_sender:
                self.email_sender = EmailSender()
            
            # 获取配置的收件人
            recipients = self.config.get('email', {}).get('recipients', [])
            
            # 发送邮件
            subject = f"📊 每日新闻汇总 - {datetime.now().strftime('%Y年%m月%d日')}"
            self.email_sender.send_simple_email(
                recipients=recipients,
                subject=subject,
                content=report,
                is_html=True
            )
            
            logger.info("每日汇总邮件发送成功")
            
        except Exception as e:
            logger.error(f"发送汇总邮件失败: {e}")
    
    def _generate_instant_report(self, news_list) -> str:
        """生成即时新闻报告（HTML格式）"""
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; }}
                .news-item {{ margin: 20px 0; padding: 15px; border-left: 4px solid #667eea; background: #f8f9fa; }}
                .importance-high {{ border-left-color: #e74c3c; }}
                .importance-medium {{ border-left-color: #f39c12; }}
                .importance-low {{ border-left-color: #27ae60; }}
                .score {{ display: inline-block; padding: 3px 8px; border-radius: 3px; font-weight: bold; }}
                .score-high {{ background: #e74c3c; color: white; }}
                .score-medium {{ background: #f39c12; color: white; }}
                .score-low {{ background: #27ae60; color: white; }}
                .summary {{ margin: 10px 0; color: #555; }}
                .factors {{ font-size: 0.9em; color: #777; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📰 即时新闻报告</h1>
                <p>时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M')}</p>
                <p>新闻数量: {len(news_list)} 条</p>
            </div>
        """
        
        for news in news_list[:10]:  # 最多显示10条
            # 确定重要性等级
            if news.importance_score >= 70:
                importance_class = "importance-high"
                score_class = "score-high"
                importance_emoji = "🔴"
            elif news.importance_score >= 40:
                importance_class = "importance-medium"
                score_class = "score-medium"
                importance_emoji = "🟡"
            else:
                importance_class = "importance-low"
                score_class = "score-low"
                importance_emoji = "🟢"
            
            html += f"""
            <div class="news-item {importance_class}">
                <h3>{importance_emoji} {news.title}</h3>
                <p>
                    <span class="score {score_class}">重要性: {news.importance_score}分</span>
                    <span style="margin-left: 10px;">来源: {news.source}</span>
                </p>
                <div class="summary">
                    <strong>摘要:</strong> {news.content[:200]}...
                </div>
                <div class="factors">
                    <strong>关键因素:</strong> {news.importance_factors if hasattr(news, 'importance_factors') and news.importance_factors else '暂无'}
                </div>
            </div>
            """
        
        html += """
        </body>
        </html>
        """
        
        return html
    
    def _generate_daily_summary_report(self, news_list, stats: Dict[str, int] = None) -> str:
        """生成每日汇总报告（HTML格式）"""
        
        # 使用传入的统计信息或计算新的
        if stats is None:
            stats = self._calculate_news_stats(news_list)
        
        total_count = stats['total']
        high_importance = stats['high'] 
        medium_importance = stats['medium']
        low_importance = stats['low']
        avg_score = stats['avg_score']
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: 'Microsoft YaHei', Arial, sans-serif; line-height: 1.6; color: #333; background: #f5f5f5; }}
                .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 20px; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; text-align: center; }}
                .stats {{ display: flex; justify-content: space-around; margin: 30px 0; }}
                .stat-card {{ text-align: center; padding: 20px; background: #f8f9fa; border-radius: 8px; flex: 1; margin: 0 10px; }}
                .stat-number {{ font-size: 2em; font-weight: bold; color: #667eea; }}
                .news-section {{ margin: 30px 0; }}
                .section-title {{ font-size: 1.5em; color: #333; border-bottom: 2px solid #667eea; padding-bottom: 10px; margin-bottom: 20px; }}
                .news-item {{ margin: 15px 0; padding: 15px; border-radius: 8px; background: #f8f9fa; }}
                .news-title {{ font-weight: bold; color: #333; margin-bottom: 5px; }}
                .importance {{ display: inline-block; padding: 3px 10px; border-radius: 15px; font-size: 0.9em; font-weight: bold; }}
                .importance-high {{ background: #e74c3c; color: white; }}
                .importance-medium {{ background: #f39c12; color: white; }}
                .importance-low {{ background: #27ae60; color: white; }}
                .summary {{ color: #666; margin: 10px 0; }}
                .footer {{ text-align: center; margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #999; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 每日新闻汇总报告</h1>
                    <p style="font-size: 1.2em;">{datetime.now().strftime('%Y年%m月%d日')}</p>
                </div>
                
                <div class="stats">
                    <div class="stat-card">
                        <div class="stat-number">{total_count}</div>
                        <div>总新闻数</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">{avg_score:.1f}</div>
                        <div>平均重要性</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">{high_importance}</div>
                        <div>高重要性新闻</div>
                    </div>
                </div>
        """
        
        # 高重要性新闻
        high_news = [n for n in news_list if n.importance_score >= 70]
        if high_news:
            html += """
                <div class="news-section">
                    <h2 class="section-title">🔴 高重要性新闻</h2>
            """
            for news in high_news[:10]:
                html += f"""
                    <div class="news-item">
                        <div class="news-title">
                            {news.title}
                            <span class="importance importance-high">{news.importance_score}分</span>
                        </div>
                        <div class="summary">{news.content[:150]}...</div>
                    </div>
                """
            html += "</div>"
        
        # 中等重要性新闻
        medium_news = [n for n in news_list if 40 <= n.importance_score < 70]
        if medium_news:
            html += """
                <div class="news-section">
                    <h2 class="section-title">🟡 中等重要性新闻</h2>
            """
            for news in medium_news[:10]:
                html += f"""
                    <div class="news-item">
                        <div class="news-title">
                            {news.title}
                            <span class="importance importance-medium">{news.importance_score}分</span>
                        </div>
                        <div class="summary">{news.content[:150]}...</div>
                    </div>
                """
            html += "</div>"
        
        # 低重要性新闻摘要
        if low_importance > 0:
            html += f"""
                <div class="news-section">
                    <h2 class="section-title">🟢 其他新闻</h2>
                    <p>今日还有 {low_importance} 条低重要性新闻，主要涉及日常市场动态和公司公告。</p>
                </div>
            """
        
        html += f"""
                <div class="footer">
                    <p>本报告由AI新闻分析系统自动生成</p>
                    <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html

    def _health_check(self):
        """系统健康检查"""
        try:
            # 检查组件状态
            components_status = {
                'news_collector': self.news_collector is not None,
                'ai_analyzer': self.ai_analyzer is not None,
                'importance_analyzer': self.importance_analyzer is not None,
                'deep_analyzer': self.deep_analyzer is not None,
                'email_sender': self.email_sender is not None,
                'notifier': self.notifier is not None,
                'database': True,  # 基本检查
                'scheduler': self.is_running
            }
            
            # 检查配置
            config_status = {
                'news_sources': len(self.config.get('news_collection', {}).get('sources', {}).get('rss_feeds', [])) > 0,
                'email_configured': bool(self.config.get('email', {}).get('smtp', {}).get('username')),
            }
            
            logger.info(f"组件状态: {components_status}")
            logger.info(f"配置状态: {config_status}")
            
            return all(components_status.values()) and all(config_status.values())
            
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return False
    
    def start(self, 
             enable_news_collection: bool = True,
             enable_analysis: bool = True,
             enable_email: bool = True,
             enable_full_pipeline: bool = False,
             enable_enhanced_strategy: bool = False,
             enable_maintenance: bool = True,
             enable_monitoring: bool = True):
        """
        启动调度器（增强版 - 包含监控和错误恢复）
        
        Args:
            enable_news_collection: 是否启用新闻收集任务
            enable_analysis: 是否启用分析任务
            enable_email: 是否启用邮件任务
            enable_full_pipeline: 是否启用完整流程任务
            enable_enhanced_strategy: 是否启用增强版调度策略
            enable_maintenance: 是否启用维护任务
            enable_monitoring: 是否启用监控功能
        """
        try:
            logger.info("正在启动任务调度器（增强版）...")
            
            # 验证配置
            if not self._validate_config():
                logger.warning("配置验证失败，但继续启动（可能影响功能）")
            
            # 加载状态
            self.load_state()
            
            # 初始化组件
            if not self.initialize_components():
                logger.error("组件初始化失败，无法启动调度器")
                return False
            
            # 添加任务
            if enable_enhanced_strategy:
                # 使用增强版调度策略
                self.add_enhanced_strategy_jobs()
            elif enable_full_pipeline:
                self.add_full_pipeline_job()
            else:
                if enable_news_collection:
                    self.add_news_collection_job()
                
                if enable_analysis and enable_email:
                    self.add_analysis_and_email_job()
                elif enable_analysis:
                    # 只添加分析任务
                    analysis_interval = self.config.get('scheduler', {}).get('analysis_interval', 60)
                    job = self.scheduler.add_job(
                        func=self._analysis_task,
                        trigger=IntervalTrigger(minutes=analysis_interval),
                        id='ai_analysis_only',
                        name='仅AI分析任务',
                        max_instances=1,
                        coalesce=True
                    )
                    self.jobs['ai_analysis_only'] = job
            
            if enable_maintenance:
                self.add_maintenance_job()
            
            # 启动调度器
            self.scheduler.start()
            self.is_running = True
            self.start_time = datetime.now().isoformat()
            self.stats['uptime_start'] = self.start_time
            
            # 启动监控线程
            if enable_monitoring:
                self.start_monitoring()
            
            # 记录启动事件
            self.record_event('scheduler_started', True, "调度器启动成功")
            
            # 更新下次执行时间
            self._update_next_execution_time()
            
            logger.info("任务调度器启动成功（增强版）")
            logger.info(f"已添加 {len(self.jobs)} 个任务")
            
            # 显示任务列表
            self.print_jobs()
            
            return True
            
        except Exception as e:
            logger.error(f"启动调度器失败: {e}")
            self.error_count += 1
            self.last_error_time = datetime.now().isoformat()
            self.record_event('scheduler_start_failed', False, f"启动失败: {e}")
            return False
    
    def stop(self):
        """停止调度器（增强版 - 包含优雅停止）"""
        try:
            logger.info("正在优雅停止任务调度器...")
            
            # 首先设置运行状态为False
            self.is_running = False
            
            # 停止监控
            self.stop_monitoring()
            
            # 停止调度器
            if self.scheduler.running:
                self.scheduler.shutdown(wait=True)
            
            # 记录停止事件
            self.record_event('scheduler_stopped', True, "调度器正常停止")
            
            # 保存最终状态
            self.save_state()
            
            logger.info("任务调度器已优雅停止")
            
        except Exception as e:
            logger.error(f"停止调度器失败: {e}")
            # 即使出错也要保存状态
            self.is_running = False
            self.record_event('scheduler_stop_error', False, f"停止时出错: {e}")
            self.save_state()
    
    def restart(self, **kwargs):
        """重启调度器"""
        logger.info("重启调度器...")
        
        self.stop()
        time.sleep(2)  # 等待完全停止
        
        return self.start(**kwargs)
    
    def wait(self):
        """等待调度器停止（兼容性方法）"""
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("收到停止信号")
            raise
    
    # ===== 兼容性方法 =====
    
    def start_with_recovery(self, **kwargs):
        """启动调度器（兼容性方法）"""
        return self.start(**kwargs)
    
    def stop_gracefully(self):
        """优雅停止调度器（兼容性方法）"""
        self.stop()
    
    def restart_scheduler(self, **kwargs):
        """重启调度器（兼容性方法）"""
        return self.restart(**kwargs)

    def pause(self):
        """暂停调度器"""
        if self.scheduler.running:
            self.scheduler.pause()
            logger.info("任务调度器已暂停")

    def resume(self):
        """恢复调度器"""
        if self.scheduler.running:
            self.scheduler.resume()
            logger.info("任务调度器已恢复")

    def run_job_once(self, job_id: str):
        """立即执行指定任务"""
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                logger.info(f"立即执行任务: {job_id}")
                job.func()
                return True
            else:
                logger.error(f"未找到任务: {job_id}")
                return False
                
        except Exception as e:
            logger.error(f"执行任务失败: {job_id}, 错误: {e}")
            return False

    def print_jobs(self):
        """打印任务列表"""
        if not self.scheduler.get_jobs():
            logger.info("没有活动的任务")
            return
        
        logger.info("=== 活动任务列表 ===")
        for job in self.scheduler.get_jobs():
            next_run = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else "未知"
            logger.info(f"任务: {job.name} (ID: {job.id})")
            logger.info(f"  下次执行: {next_run}")
            logger.info(f"  触发器: {job.trigger}")
            logger.info("")

    def _update_next_execution_time(self):
        """更新下次执行时间"""
        next_times = []
        for job in self.scheduler.get_jobs():
            if job.next_run_time:
                next_times.append(job.next_run_time)
        
        if next_times:
            earliest = min(next_times)
            self.stats['next_execution_time'] = earliest.isoformat()

    def get_stats(self) -> Dict[str, Any]:
        """获取调度器统计信息"""
        self._update_next_execution_time()
        
        return {
            **self.stats,
            'is_running': self.is_running,
            'active_jobs_count': len(self.scheduler.get_jobs()) if self.scheduler else 0,
            'scheduler_state': 'running' if self.is_running else 'stopped'
        }

    def run_forever(self):
        """持续运行调度器"""
        try:
            logger.info("调度器进入持续运行模式，按 Ctrl+C 停止")
            
            while self.is_running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("接收到停止信号")
        finally:
            self.stop()


def create_default_scheduler() -> TaskScheduler:
    """创建默认配置的调度器"""
    scheduler = TaskScheduler()
    return scheduler


# 向后兼容性别名
def create_scheduler_manager(config_path: str = None) -> TaskScheduler:
    """创建调度器管理器实例（兼容性别名）"""
    return TaskScheduler(config_path)


# 兼容性类别名
SchedulerManager = TaskScheduler


if __name__ == "__main__":
    # 测试调度器
    scheduler = create_default_scheduler()
    
    try:
        # 启动调度器（仅测试，不启用邮件）
        if scheduler.start(enable_email=False, enable_full_pipeline=False):
            print("调度器启动成功，运行10秒后停止...")
            time.sleep(10)
        else:
            print("调度器启动失败")
    
    finally:
        scheduler.stop()
        print("测试完成") 
