import sqlite3

class DatabaseManager:
    def __init__(self, db_name="logs.db"):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

    def close(self):
        if self.conn:
            self.conn.close()

    def execute(self, query, params=None):
        if params is None:
            params = []
        self.cursor.execute(query, params)
        self.conn.commit()

    def fetch_all(self, query, params=None):
        if params is None:
            params = []
        self.cursor.execute(query, params)
        return self.cursor.fetchall()