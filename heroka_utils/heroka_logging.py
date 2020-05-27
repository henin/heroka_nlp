import logging
import os
import sys
from logging.config import dictConfig

PROJECT = os.path.basename(os.path.dirname(os.path.realpath(__file__)))

# This level must be set to DEBUG mode for development purpose otherwise in
# production should be set to ERROR

class HerokaLogger():
    def __init__(self, level=None, filename=None, handler=None, maxBytes=None, backup_count=None):

        self.level = level if level else "DEBUG"
        self.filename = filename if filename else "{}.log".format(PROJECT)

        self.logger_level = getattr(logging, self.level)
        self.handler = handler if handler else "logging.handlers.RotatingFileHandler"
        self.backup_count = 7
        self.maxBytes = 52428800

        self.logger_type = self.filename + '_logger'

    def get_logger(self, **kwargs):
        # if kwargs:
        #     log_filename = kwargs.get('filename')
        #     handler = kwargs.get('handler')

        # for k, v in kwargs:
        #     setattr(self, k, v)

        try:
            logging_config = {
                "version": 1,
                "formatters": {
                    "verbose": {
                        "format": "[%(asctime)s].%(msecs)03d %(levelname)s: [%(name)s: %(filename)s:: %(funcName)s(): %(lineno)s] %(message)s",
                        "datefmt": "%Y-%m-%d %H:%M:%S"
                    },
                    "simple": {
                        "format": "%(levelname)s %(message)s"
                    },
                    "default": {
                        "format": "%(asctime)s - %(levelname)s :[%(name)s: %(filename)s:: %(funcName)s(): %(lineno)s] %(message)s", 
                        "datefmt": "%Y-%m-%d %H:%M:%S"
                        }
                },

                "handlers": 
                {
                    "console": {
                        "level": self.level,
                        "class": "logging.StreamHandler",
                        "formatter": "default",
                        "stream": "ext://sys.stdout"
                    },
                    "file": {
                        "class": self.handler,
                        "formatter": "verbose",
                        "level": self.level,
                        "filename": self.filename,
                        "maxBytes": self.maxBytes,
                        "backupCount": self.backup_count
                    },
                },

                "loggers": {
                    "default":
                    {
                        "handlers": ["file", "console"],
                        "level": self.level
                    }
                },
                "disable_existing_loggers": False
            }

            dictConfig(logging_config)
            # Logger names
            logger = logging.getLogger("default")
            return logger

        except Exception as error:
            print(error)

if __name__ == '__main__':
    HerokaLogger()
