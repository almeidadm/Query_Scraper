import logging


class LoggingClass:
    def __init__(self, logger: logging.getLogger = None, **kw_args):
        """
        A LoggingClass to storage logger message setup-us
        Focus on flexible approach of import logging

        Args:
            logger (logging.getLogger, optional): logger define by class. Defaults to None.
        """
        self.logger = (
            logger if logger is not None else logging.getLogger(self.__class__.__name__)
        )

    def info(self, message, *args, **kw_args):
        self.logger.info(message, *args, **kw_args)

    def warning(self, message, *args, **kw_args):
        self.logger.warning(message, *args, **kw_args)

    def error(self, message, *args, **kw_args):
        self.logger.error(message, *args, **kw_args)

    def debug(self, message, *args, **kw_args):
        self.logger.debug(message, *args, **kw_args)
        