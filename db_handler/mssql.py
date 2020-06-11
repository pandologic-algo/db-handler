import pyodbc
import pandas as pd
import math
import time

# internal
from .base import _BaseReader


class MSSQLTableReader(_BaseReader):
    def __init__(self, server, database, schema, user, password, driver):
        super().__init__()

        # db params
        self.server = server
        self.database = database
        self.schema = schema
        self.user = user
        self.password = password
        self.driver = driver

    def read_table(self, table_name, where='where 1=1', top=None, offset=0, chunk_size=10 ** 5, column=None):
        """
        Read the data from DB as pandas DataFrame
        :param table_name: table name to read
        :param where: where string statement
        :param top: max records to return from table (if column is not None then need to add to top +1 => top = top + 1)
        :param offset: table start offset (column start value)
        :param chunk_size: number of rows to read (column max value)
        :param column: when reading in chunks, set the order of the records by column
        :return: pandas DataFrame
        """
        read_finished = False
        table = None
        tries = 0
        conn = None
        running_offset = offset
        offset_statement = ''
        order_by_statement = ''

        # loop for cases when timeout exception occur
        while not read_finished:
            # max time to try read from DB
            if tries > 5:
                return None

            # connection creation
            try:
                conn = pyodbc.connect('Driver={' + self.driver + '}'
                                      ';SERVER=' + self.server +
                                      ';DATABASE=' + self.database +
                                      ';UID=' + self.user +
                                      ';PWD=' + self.password)
            except Exception as con_e:  # change exceptions
                self.logger.exception('SQL Reader - Connection Error')
                self.logger.exception('SQL Reader - Exception: {}'.format(str(con_e)))

            if column is None:
                order_by_statement = ' ORDER BY(SELECT NULL)'

            max_offset = math.inf
            
            if top is not None:
                max_offset = top + offset

            fetch_all_finished = False

            if chunk_size > top:
                chunk_size = top

            try:

                while not fetch_all_finished:
                    if column is None:
                        offset_statement = """OFFSET {} Rows FETCH NEXT {} ROWS ONLY""".format(running_offset,
                                                                                               chunk_size)
                        where_statement = where
                    else:
                        where_statement = where + ' and {0}<={1} and {1}<{2}'.format(running_offset,
                                                                                     column,
                                                                                     running_offset + chunk_size)

                    qry = "SELECT * FROM " + self.database + "." + self.schema + "." + table_name + \
                          " with(nolock)" + " " + where_statement + " " + order_by_statement + " " + offset_statement

                    if running_offset == offset:
                        # batch read from SQL DB
                        table = pd.read_sql(qry, conn)
                        self.logger.info('{} rows were read'.format(table.shape[0]))

                    elif running_offset >= max_offset:
                        fetch_all_finished = True

                    else:
                        added_table = pd.read_sql(qry, conn)
                        if added_table.shape[0] > 0:
                            table = table.append(added_table, ignore_index=True)
                            self.logger.info('{} rows were read'.format(table.shape[0]))
                        else:
                            fetch_all_finished = True

                    running_offset = running_offset + chunk_size

                    if (running_offset + chunk_size) > max_offset:
                        chunk_size = max_offset - running_offset

                read_finished = True

            except Exception as e:  # change exceptions
                time.sleep(60)
                tries = tries + 1
                self.logger.exception('SQL Reader - Read Error')
                self.logger.exception('SQL Reader - Exception: {}'.format(str(e)))
            finally:
                try:
                    conn.close()
                except:
                    pass

        return table

    def read_table_from_sp(self, sp, sp_params):
        """
        Read the data from DB as pandas DataFrame
        :param sp: stored procedure name
        :param sp_params: stored procedure parameters
        """
        read_finished = False
        table = None
        tries = 0
        conn = None

        # loop for cases when timeout exception occur
        while not read_finished:
            # max time to try read from DB
            if tries > 5:
                return None

            # connection creation
            try:
                conn = pyodbc.connect(self.driver + self.server +
                                      ';DATABASE=' + self.database +
                                      ';UID=' + self.user +
                                      ';PWD=' + self.password)
            except Exception as con_e:  # change exceptions
                self.logger.exception('SQL Reader - Connection Error')
                self.logger.exception('SQL Reader - Exception: {}'.format(str(con_e)))

            try:
                qry = "exec {}.{}.{} {}".format(self.database, self.schema, sp, sp_params)

                # batch read from SQL DB
                table = pd.read_sql(qry, conn)
                self.logger.info('{} rows were read'.format(table.shape[0]))

                read_finished = True

            except Exception as e:  # change exceptions
                time.sleep(60)
                tries = tries + 1
                self.logger.exception('SQL Reader - Read Error')
                self.logger.exception('SQL Reader - Exception: {}'.format(str(e)))
            finally:
                try:
                    conn.close()
                except:
                    pass

        return table
