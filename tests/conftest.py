# conftest.py
import pytest
import psycopg2
import os

@pytest.fixture(scope="session")
def db_params():
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('DB_NAME', 'arsenalfc_analytics'),
        'user': os.getenv('DB_USER', 'analytics_user'),
        'password': os.getenv('DB_PASSWORD', 'analytics_pass')
    }

@pytest.fixture(scope="session")
def db_conn(db_params):
    conn = psycopg2.connect(**db_params)
    yield conn
    conn.close()