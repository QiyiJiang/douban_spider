#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一的日志配置模块

供项目中的所有脚本共用，避免重复配置
"""

import sys
from pathlib import Path
from loguru import logger


# 标记是否已经配置过（避免重复配置）
_logger_configured = False


def setup_logger(module_name: str = "app", console_level: str = "INFO", file_level: str = "DEBUG"):
    """
    配置统一的日志系统
    
    参数:
        module_name: 模块名称，用于区分不同模块的日志文件
        console_level: 控制台日志级别（INFO/DEBUG/WARNING/ERROR）
        file_level: 文件日志级别（INFO/DEBUG/WARNING/ERROR）
    
    返回:
        logger: 配置好的 logger 对象
    """
    global _logger_configured
    
    # 如果已经配置过，只返回 logger（避免重复添加 handler）
    if _logger_configured:
        return logger
    
    # 移除默认的 handler
    logger.remove()
    
    # 1. 添加控制台输出（彩色，简洁格式）
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=console_level,
        colorize=True,
        enqueue=True  # 线程安全
    )
    
    # 2. 添加文件输出（详细格式，按日期分割）
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f"{module_name}_{{time:YYYY-MM-DD}}.log"
    
    logger.add(
        str(log_file),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        rotation="00:00",  # 每天午夜轮转
        retention="7 days",  # 保留7天
        encoding="utf-8",
        level=file_level,
        enqueue=True  # 线程安全
    )
    
    _logger_configured = True
    logger.info(f"日志系统初始化完成 [模块: {module_name}]")
    
    return logger


def get_logger():
    """
    获取已配置的 logger
    
    如果尚未配置，使用默认配置
    """
    global _logger_configured
    
    if not _logger_configured:
        setup_logger()
    
    return logger


# 为了方便导入，直接暴露配置好的 logger
__all__ = ['setup_logger', 'get_logger', 'logger']

