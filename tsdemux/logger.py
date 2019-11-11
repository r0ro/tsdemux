import logging

from colorlog import ColoredFormatter


loggers = {}


def get_logger(name, level=logging.INFO):
    if name in loggers:
        return loggers[name]

    formatter = ColoredFormatter(
        "%(asctime)s | %(log_color)s%(levelname)-8s%(reset)s | %(log_color)s%(message)s%(reset)s")
    main_stream = logging.StreamHandler()
    main_stream.setLevel(logging.DEBUG)
    main_stream.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(main_stream)
    loggers[name] = logger
    return logger


class LogEnabled:

    def __init__(self, log_name="ts", prefix="", verbose=False):
        self.verbose_debug = verbose
        if prefix == "":
            self.log_prefix = ""
        else:
            self.log_prefix = prefix + " "
        self.logger = get_logger(log_name, logging.DEBUG)

    def verbose(self, msg, *args):
        if self.verbose_debug:
            self.logger.debug(self.log_prefix + msg, *args)

    def debug(self, msg, *args):
        self.logger.debug(self.log_prefix + msg, *args)

    def info(self, msg, *args):
        self.logger.info(self.log_prefix + msg, *args)

    def warning(self, msg, *args):
        self.logger.warning(self.log_prefix + msg, *args)

    def error(self, msg, *args):
        self.logger.error(self.log_prefix + msg, *args)

