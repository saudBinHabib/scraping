
import os
import sqlite3

from sqlite3 import Error


class MySqliteDatabase:

    conn = None
    cursor = None
    DATABASE_PATH = '../database/files.db'

    def __init__(self):
        self.PROJECT_ROOT = os.path.dirname(os.path.realpath('../database/'))

    def close_connection(self):
        self.conn.close()

    def connect_db(self):
        self.conn = sqlite3.Connection(self.DATABASE_PATH)
        self.cursor = self.conn.cursor()

    def create_files_table(self) -> bool:
        # creating the files table
        try:
            self.cursor.execute(
                """
                    CREATE TABLE files (
                        id INTEGER PRIMARY KEY,
                        file_url TEXT UNIQUE NOT NULL
                    )
                """
            )
            return True
        except Error:
            return False

    def insert_file(self, file_url: str) -> bool:
        try:
            with self.conn:
                self.cursor.execute(
                    "INSERT INTO files (file_url) VALUES (:file)",
                    {'file': str(file_url)}
                )
            return True
        except Error as err:
            return False

    def file_existed(self, file: str) -> bool:
        try:
            self.cursor.execute(
                "select * FROM files where file_url = '{}'".format(file)
            )
            result = self.cursor.fetchall()
            return True if result else False
        except Error as err:
            return False
