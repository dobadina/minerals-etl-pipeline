import sys
sys.path.append('.')
from utils.db_utils import get_connection

conn = get_connection()

print('--- Row counts ---')
print('sampling_requests:', conn.execute('SELECT COUNT(*) FROM silver.sampling_requests').fetchone()[0])
print('task_assignment:', conn.execute('SELECT COUNT(*) FROM silver.task_assignment').fetchone()[0])
print('sample_collection:', conn.execute('SELECT COUNT(*) FROM silver.sample_collection').fetchone()[0])
print('lab_test_results:', conn.execute('SELECT COUNT(*) FROM silver.lab_test_results').fetchone()[0])
print('fact:', conn.execute('SELECT COUNT(*) FROM gold.fact_sampling_requests').fetchone()[0])

print()
print('--- Requests with multiple task assignments ---')
print(conn.execute("""
    SELECT request_id, COUNT(*) as cnt
    FROM silver.task_assignment
    GROUP BY request_id
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 5
""").fetchall())

print()
print('--- Requests with multiple collections ---')
print(conn.execute("""
    SELECT request_id, COUNT(*) as cnt
    FROM silver.sample_collection
    GROUP BY request_id
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 5
""").fetchall())

print()
print('--- Requests with multiple lab results ---')
print(conn.execute("""
    SELECT request_id, COUNT(*) as cnt
    FROM silver.lab_test_results
    GROUP BY request_id
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 5
""").fetchall())

conn.close()