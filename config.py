"""
config.py — 全局配置加载模块

负责从 .env 文件读取环境变量，并提供统一的配置入口供各模块使用。
"""

import os
from dotenv import load_dotenv

load_dotenv()


def get_polygon_api_key():
    """返回 Polygon.io API Key"""
    pass


def get_eodhd_api_key():
    """返回 EODHD API Key"""
    pass


def get_anthropic_api_key():
    """返回 Anthropic Claude API Key"""
    pass


def get_telegram_config():
    """返回 Telegram Bot Token 和 Chat ID"""
    pass


def get_app_settings():
    """返回应用级配置（日志级别、输出目录等）"""
    pass
