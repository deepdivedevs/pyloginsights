import sqlite3
import functools
import time
import io
from contextlib import redirect_stdout
import psutil

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

# Query function
def query_logs(start_time=None, end_time=None, function_name=None, stdout=None, min_memory=None, max_memory=None):
    query = "SELECT * FROM logs WHERE 1=1"
    params = []
    
    if start_time:
        query += " AND timestamp >= ?"
        params.append(start_time)
    if end_time:
        query += " AND timestamp <= ?"
        params.append(end_time)
    if function_name:
        query += " AND function_name = ?"
        params.append(function_name)
    if stdout:
        query += " AND stdout LIKE ?"
        params.append(f"%{stdout}%") 
    if min_memory:
        query += " AND avg_memory >= ?"
        params.append(min_memory)
    if max_memory:
        query += " AND avg_memory <= ?"
        params.append(max_memory)
    
    cursor.execute(query, params)
    return cursor.fetchall()

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

# Query and print logs
print("All logs:")
all_logs = query_logs(stdout="processing")
for log in all_logs:
    print(f"Timestamp: {log[0]}, Function: {log[1]}, Execution Time: {log[2]:.9f}s")
    print(f"Avg Memory: {log[3]:.2f} MB")
    print(f"Stdout: {log[4]}")

# Clean up
conn.close()