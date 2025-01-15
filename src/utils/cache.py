from functools import lru_cache
from typing import Callable, Any

# 默认缓存大小
DEFAULT_CACHE_SIZE = 128

def cached_function(maxsize: int = DEFAULT_CACHE_SIZE) -> Callable:
    """
    装饰器：为函数添加缓存功能。

    :param maxsize: 缓存的最大大小（默认为 128）
    :return: 带缓存的函数
    """
    def decorator(func: Callable) -> Callable:
        return lru_cache(maxsize=maxsize)(func)
    return decorator

class CacheManager:
    """
    缓存管理器，用于管理多个缓存实例。
    """
    def __init__(self):
        self._caches = {}

    def get_cache(self, name: str, maxsize: int = DEFAULT_CACHE_SIZE) -> lru_cache:
        """
        获取或创建一个缓存实例。

        :param name: 缓存名称
        :param maxsize: 缓存的最大大小
        :return: 缓存实例
        """
        if name not in self._caches:
            self._caches[name] = lru_cache(maxsize=maxsize)
        return self._caches[name]

    def clear_cache(self, name: str) -> None:
        """
        清除指定名称的缓存。

        :param name: 缓存名称
        """
        if name in self._caches:
            self._caches[name].cache_clear()

    def clear_all_caches(self) -> None:
        """
        清除所有缓存。
        """
        for cache in self._caches.values():
            cache.cache_clear()

# 全局缓存管理器实例
cache_manager = CacheManager()