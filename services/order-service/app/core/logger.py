import sys

from loguru import logger

from app.core.config import get_settings

settings = get_settings()

logger.remove()

if settings.production:
    logger.add(
        sys.stdout,
        serialize=True,
        level="INFO",
        enqueue=True,
        backtrace=True,
        diagnose=False,
    )
else:
    logger.add(
        sys.stdout,
        level="DEBUG",
        enqueue=True,
        backtrace=True,
        diagnose=True,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "{message} | "
            "{extra}"
        ),
    )

__all__ = ["logger"]