import functools
import io
import time
from contextlib import redirect_stdout
import psutil
import pandas as pd

class Logger:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.db_manager.connect()
        self.db_manager.execute('''
            CREATE TABLE IF NOT EXISTS logs
            (timestamp REAL, function_name TEXT, execution_time REAL, avg_memory REAL, stdout TEXT)
        ''')

    def capture(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            process = psutil.Process()
            start_time = time.perf_counter_ns()
            start_memory = process.memory_info().rss / 1024 / 1024

            stdout_capture = io.StringIO()
            with redirect_stdout(stdout_capture):
                result = func(*args, **kwargs)

            end_time = time.perf_counter_ns()
            end_memory = process.memory_info().rss / 1024 / 1024
            execution_time = (end_time - start_time) / 1e9

            avg_memory = (start_memory + end_memory) / 2
            stdout_output = stdout_capture.getvalue().strip()

            self.db_manager.execute('''
                INSERT INTO logs (timestamp, function_name, execution_time, avg_memory, stdout)
                VALUES (?, ?, ?, ?, ?)
            ''', (start_time, func.__name__, execution_time, avg_memory, stdout_output))

            return result
        return wrapper

    def query_logs(self, filters=None, sort_by=None):
        query = "SELECT * FROM logs WHERE 1=1"
        params = []

        if filters:
            query, params = self._apply_filters(query, params, filters)

        try:
            self.db_manager.connect()
            df = pd.read_sql_query(query, self.db_manager.conn, params=params)
            print(f"Query returned {len(df)} rows")
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()
        finally:
            self.db_manager.close()

        if sort_by:
            df = self._apply_sorting(df, sort_by)

        return df

    def _apply_filters(self, query, params, filters):
        if "start_time" in filters:
            query += " AND timestamp >= ?"
            params.append(filters["start_time"])
        if "end_time" in filters:
            query += " AND timestamp <= ?"
            params.append(filters["end_time"])
        if "function_name" in filters:
            query += " AND function_name = ?"
            params.append(filters["function_name"])
        if "min_memory" in filters:
            query += " AND avg_memory >= ?"
            params.append(filters["min_memory"])
        if "max_memory" in filters:
            query += " AND avg_memory <= ?"
            params.append(filters["max_memory"])
        if "stdout" in filters:
            query += " AND stdout LIKE ?"
            params.append(f"%{filters['stdout']}%")
        return query, params

    def _apply_sorting(self, df, sort_by):
        try:
            sort_columns = [col for col, _ in sort_by]
            sort_ascending = [direction == "asc" for _, direction in sort_by]
            return df.sort_values(by=sort_columns, ascending=sort_ascending)
        except Exception as e:
            print(f"Error sorting DataFrame: {e}")
            return df

    def export_logs(self, df, file_path, format="csv"):
        try:
            if format.lower() == "csv":
                df.to_csv(file_path, index=False)
            elif format.lower() == "json":
                df.to_json(file_path, orient="records", lines=True)
            else:
                raise ValueError(f"Unsupported format: {format}. Use 'csv' or 'json'.")

            print(f"Logs exported successfully to {file_path}")
            return True
        except Exception as e:
            print(f"Error exporting logs: {e}")
            return False