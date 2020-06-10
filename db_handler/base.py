import logging
from abc import ABC, abstractmethod


class _BaseReader(ABC):

    def __init__(self, **kwargs):
        """
        Init Reader logger
        """
        
        self.logger = self.init_logger()

    @abstractmethod
    def read_table(self, **kwargs):
        """
        Read the data from DB as pandas DataFrame
        :return: pandas DataFrame
        """

    @abstractmethod
    def read_table_from_sp(self, **kwargs):
        """
        Read the data from DB as pandas DataFrame
        :return: pandas DataFrame
        """

    def init_logger(self):
        """
        Initialize a Logger obj to report reader progress
        :return: Logger obj
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # create console handler and set level to info
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("'%(asctime)s %(levelname)s %(message)s'")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger
