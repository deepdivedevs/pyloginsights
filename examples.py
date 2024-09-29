from pyloginsight import PyLogInsight
import random

log_insight = PyLogInsight()

@log_insight.capture
def square_numbers(n):
    print(f"Squaring numbers up to: {n}") 
    result = 0
    for i in range(n):
        result += i ** 2
    print(f"square_numbers result: {result}")
    return result

@log_insight.capture
def find_average(size):
    print(f"Finding average of size: {size}")
    large_list = [random.random() for _ in range(size)]
    result = sum(large_list) / len(large_list)
    print(f"find_average result: {result}")
    return result

@log_insight.capture
def fibonacci(n):
    print(f"Finding fibonacci of {n}")
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)

@log_insight.capture
def bubble_sort(arr):
    print("Running bubble sort")
    n = len(arr)
    for i in range(n):
        for j in range(0, n-i-1):
            if arr[j] > arr[j+1]:
                arr[j], arr[j+1] = arr[j+1], arr[j]
    return arr


square_numbers(10)
find_average(100)
fibonacci(5)
bubble_sort([random.random() for _ in range(100)])

logs = log_insight.query_logs()
print(logs)

#log_insight.export_logs(logs, "example_logs.csv")