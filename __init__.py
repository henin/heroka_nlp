import os
import sys
project_path = os.path.join(os.getcwd(), 'src')
sys.path.insert(0, project_path)
from heroka_utils import heroka_logging

__author__  = "Henin Roland Karkada <henin.roland@gmail.com>"
__status__  = "production"
__version__ = "0.0.1"
__date__    = "27 April 2020"

PROJECT = os.path.basename(os.path.dirname(os.path.realpath(__file__)))
LOGGER_FILENAME = "{}.log".format(PROJECT)
logger = heroka_logging.HerokaLogger(filename=LOGGER_FILENAME, level="DEBUG").get_logger()
