import sqlite3
import functools
import time
import io
from contextlib import redirect_stdout
import psutil
import pandas as pd
import os

# Database setup
conn = sqlite3.connect('logs.db')
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS logs
(timestamp REAL, function_name TEXT, execution_time REAL, avg_memory REAL, stdout TEXT)
''')
conn.commit()

# Logging decorator
def capture(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process()
        start_time = time.perf_counter_ns()
        start_memory = process.memory_info().rss / 1024 / 1024  # Memory in MB

        # Capture stdout
        stdout_capture = io.StringIO()
        with redirect_stdout(stdout_capture):
            result = func(*args, **kwargs)
        
        end_time = time.perf_counter_ns()  # Use nanosecond precision
        end_memory = process.memory_info().rss / 1024 / 1024
        execution_time = (end_time - start_time) / 1e9  # Convert to seconds 

        avg_memory = (start_memory + end_memory) / 2

        stdout_output = stdout_capture.getvalue().strip()
        
        cursor.execute('''
        INSERT INTO logs (timestamp, function_name, execution_time, avg_memory, stdout)
        VALUES (?, ?, ?, ?, ?)
        ''', (start_time, func.__name__, execution_time, avg_memory, stdout_output))
        conn.commit()
        
        return result
    return wrapper

# TODO:
# Advanced filtering
#
def query_logs(filters=None, sort_by=None):
    conn = sqlite3.connect('logs.db')
    
    query = "SELECT * FROM logs WHERE 1=1"
    params = []

    if filters:
        if 'start_time' in filters:
            query += " AND timestamp >= ?"
            params.append(filters['start_time'])
        if 'end_time' in filters:
            query += " AND timestamp <= ?"
            params.append(filters['end_time'])
        if 'function_name' in filters:
            query += " AND function_name = ?"
            params.append(filters['function_name'])
        if 'min_memory' in filters:
            query += " AND avg_memory >= ?"
            params.append(filters['min_memory'])
        if 'max_memory' in filters:
            query += " AND avg_memory <= ?"
            params.append(filters['max_memory'])
        if 'stdout' in filters:
            query += " AND stdout LIKE ?"
            params.append(f"%{filters['stdout']}%")

    try:
        df = pd.read_sql_query(query, conn, params=params)
        print(f"Query returned {len(df)} rows")
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

    if sort_by:
        try:
            sort_columns = [col for col, _ in sort_by]
            sort_ascending = [direction == 'asc' for _, direction in sort_by]
            df = df.sort_values(by=sort_columns, ascending=sort_ascending)
        except Exception as e:
            print(f"Error sorting DataFrame: {e}")

    return df

def export_logs(df, file_path, format='csv'):
    """
    Export logs to a file in the specified format.
    
    :param df: pandas DataFrame containing the log data
    :param file_path: path where the file should be saved
    :param format: 'csv' or 'json'
    :return: True if export was successful, False otherwise
    """
    try:
        if format.lower() == 'csv':
            df.to_csv(file_path, index=False)
        elif format.lower() == 'json':
            df.to_json(file_path, orient='records', lines=True)
        else:
            raise ValueError(f"Unsupported format: {format}. Use 'csv' or 'json'.")
        
        print(f"Logs exported successfully to {file_path}")
        return True
    except Exception as e:
        print(f"Error exporting logs: {e}")
        return False

# Example usage
@capture
def example_function(x, y):
    print(f"Processing {x} and {y}")
    result = x + y
    print(f"Result: {result}")
    return result

@capture
def example_function_two(x):
    print("Testing")
    return x + 1

example_function(1, 2)
example_function_two(5)


logs = query_logs(
    filters={'stdout': 'testing'},
    sort_by=[('execution_time', 'desc'), ('function_name', 'asc')]
)
print(logs)

csv_path = os.path.join(os.getcwd(), 'logs_export.csv')
export_logs(logs, csv_path, format='csv')

# Clean up
conn.close()