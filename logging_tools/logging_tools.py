import logging
from functools import wraps
from time import perf_counter


def get_logger(name="AppLogger", level=logging.INFO, file: str = None) -> logging.Logger:
    """
    Создаёт и настраивает логгер.

    Args:
        name (str): имя логгера
        level: уровень логирования
        file (str, optional): если указан — лог будет писаться в файл

    Returns:
        logging.Logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # если логгер уже имеет обработчики, не добавляем новые
    if not logger.handlers:
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%H:%M:%S")

        # вывод в консоль
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # вывод в файл
        if file is not None:
            fh = logging.FileHandler(file)
            fh.setFormatter(formatter)
            logger.addHandler(fh)

    return logger


def log_time(logger, name=None):
    """
    Декоратор для логирования времени выполнения функции/метода.

    Args:
        logger: экземпляр logging.Logger
        name: опциональное имя задачи, если не указано — берётся имя функции
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            task_name = name or func.__name__
            logger.info(f"Started {task_name}...")
            start = perf_counter()
            result = func(*args, **kwargs)
            end = perf_counter()
            logger.info(f"{task_name} done in {end - start:.3f} secs")
            return result

        return wrapper

    return decorator
