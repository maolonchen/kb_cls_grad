import os
import sys
import logging
import threading
from typing import Optional
from pathlib import Path

# 线程锁，确保初始化只执行一次
_lock = threading.Lock()
_default_handler: Optional[logging.Handler] = None
_file_handler: Optional[logging.Handler] = None

# 支持的日志级别映射
log_levels = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

# 默认日志级别
_default_log_level = logging.INFO
# 环境变量名，用于外部配置日志级别
_VERBOSITY_ENV = "LOG_VERBOSITY"
# 日志文件路径环境变量
_LOG_FILE_ENV = "LOG_FILE_PATH"
# 默认日志文件路径
_DEFAULT_LOG_FILE = "./logs/app.log"


def _get_default_logging_level() -> int:
    """
    从环境变量获取日志级别名称，返回对应的 logging.LEVEL。
    环境变量值无效或未设置时，返回默认级别。
    """
    name = os.getenv(_VERBOSITY_ENV, "").lower()
    if name in log_levels:
        return log_levels[name]
    if name:
        print(
            f"Unknown {_VERBOSITY_ENV}={name}, valid options: {list(log_levels.keys())}",
            file=sys.stderr,
        )
    return _default_log_level


def _configure_root_logger() -> None:
    """
    配置根 logger，创建并添加唯一的 StreamHandler。
    本函数线程安全，且仅生效一次。
    """
    global _default_handler, _file_handler
    with _lock:
        if _default_handler:
            return
        # 确保 stderr 可用
        if sys.stderr is None:
            sys.stderr = open(os.devnull, "w")
        level = _get_default_logging_level()

        # 获取根logger
        root = logging.getLogger()
        # 清除可能已存在的handlers（处理basicConfig的情况）
        for handler in root.handlers[:]:
            root.removeHandler(handler)

        # 配置控制台输出
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setLevel(level)
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        root.addHandler(handler)
        root.setLevel(level)
        _default_handler = handler

        # 配置文件输出
        log_file_path = os.getenv(_LOG_FILE_ENV, _DEFAULT_LOG_FILE)
        if log_file_path:
            # 确保日志目录存在
            log_path = Path(log_file_path)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            # 添加文件处理器
            try:
                _file_handler = logging.FileHandler(
                    log_file_path, encoding='utf-8')
                _file_handler.setLevel(level)
                _file_handler.setFormatter(formatter)
                root.addHandler(_file_handler)
            except Exception as e:
                print(f"无法创建日志文件 {log_file_path}: {e}", file=sys.stderr)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    获取已配置的 logger。如传入 name，返回同名 logger，否则返回根 logger。
    """
    _configure_root_logger()
    return logging.getLogger(name)


def set_verbosity(level: int) -> None:
    """
    动态设置全局日志级别。
    """
    _configure_root_logger()
    root = logging.getLogger()
    root.setLevel(level)
    if _default_handler:
        _default_handler.setLevel(level)
    if _file_handler:
        _file_handler.setLevel(level)


def get_verbosity() -> int:
    """
    返回当前全局日志级别。
    """
    _configure_root_logger()
    return logging.getLogger().getEffectiveLevel()


def capture_warnings(enable: bool) -> None:
    """
    将 Python 警告模块 warnings 的输出重定向到 logging 系统。
    """
    from logging import captureWarnings as _captureWarnings
    logger = get_logger("py.warnings")
    if _default_handler and not logger.handlers:
        logger.addHandler(_default_handler)
    if _file_handler and _file_handler not in logger.handlers:
        logger.addHandler(_file_handler)
    logger.setLevel(get_verbosity())
    _captureWarnings(enable)
