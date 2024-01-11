import os
from dotenv import load_dotenv
import pymysql

load_dotenv()

class ETL(object):
    def __init__(self):
        self.source_connection = pymysql.connect(
            host=(os.getenv('SOURCE_MYSQL_HOST')),
            port=(int(os.getenv('SOURCE_MYSQL_PORT'))),
            user=(os.getenv('SOURCE_MYSQL_USER')),
            password=(os.getenv('SOURCE_MYSQL_PASSWORD')),
            db=(os.getenv('SOURCE_MYSQL_DB'))
        )

        self.destination_connection = pymysql.connect(
            host=(os.getenv('DESTINATION_MYSQL_HOST')),
            port=(int(os.getenv('DESTINATION_MYSQL_PORT'))),
            user=(os.getenv('DESTINATION_MYSQL_USER')),
            password=(os.getenv('DESTINATION_MYSQL_PASSWORD')),
            db=(os.getenv('DESTINATION_MYSQL_DB'))
        )

    def run(self):
        source_cursor = self.source_connection.cursor()
        destination_cursor = self.destination_connection.cursor()
        source_cursor.execute("SELECT * FROM test_table")

        for row in source_cursor:
            destination_cursor.execute("INSERT INTO test_table (id, value) VALUES (%s, %s)", row)
            self.destination_connection.commit()
