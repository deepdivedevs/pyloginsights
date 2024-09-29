from db import DatabaseManager
from logger import Logger

class PyLogInsight:
    def __init__(self, db_name="logs.db"):
        self.db_manager = DatabaseManager(db_name)
        self.logger = Logger(self.db_manager)

    def capture(self, func):
        return self.logger.capture(func)

    def query_logs(self, **kwargs):
        return self.logger.query_logs(**kwargs)

    def export_logs(self, df, file_path, format="csv"):
        return self.logger.export_logs(df, file_path, format)