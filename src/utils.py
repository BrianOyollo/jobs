import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def insert_jobs(jobs):
    """
    Insert a list of job dicts into the jobs table.
    Each dict should have keys:
    title, link, published, updated, author, category, fetched_at
    """
    conn = None
    try:
        # Get DB credentials from env
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", 5432),
        )
        cur = conn.cursor()

        # Define insert query
        insert_query = """
            INSERT INTO jobs (title, link, published, author, category, fetched_at)
            VALUES %s
            ON CONFLICT (link) DO NOTHING
        """

        # Prepare data as list of tuples
        values = [
            (
                job.get("title"),
                job.get("link"),
                job.get("published"),
                job.get("author"),
                job.get("category"),
                job.get("fetched_at"),
            )
            for job in jobs
        ]

        # Bulk insert
        execute_values(cur, insert_query, values)

        conn.commit()
        print(f"{cur.rowcount} jobs inserted.")

        cur.close()
    except Exception as e:
        print("Error inserting jobs:", e)
    finally:
        if conn:
            conn.close()
