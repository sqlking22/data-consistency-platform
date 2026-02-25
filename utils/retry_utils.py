#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 12:43
# @Author  : hejun
import time
import logging
import random
from functools import wraps
from typing import Callable

logger = logging.getLogger(__name__)

def retry_decorator(max_retries: int = 3, delay: int = 5, exceptions: tuple = (Exception,)):
    """重试装饰器"""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    logger.warning(f"执行{func.__name__}失败，重试第{retries}次，错误：{str(e)}")
                    if retries >= max_retries:
                        logger.error(f"执行{func.__name__}重试{max_retries}次后仍失败")
                        raise
                    wait_time = delay * (2 ** (retries - 1)) * random.uniform(0.5, 1.5)
                    time.sleep(wait_time)  # 指数退避
            return None
        return wrapper
    return decorator