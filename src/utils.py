import os
import psycopg2
import requests
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

ntfy_topic = os.getenv("NTFY_TOPIC")

def db_connection():
    """
    Establish a connection to the PostgreSQL database using credentials 
    stored in environment variables.

    Environment variables required:
        - POSTGRES_DB: Name of the database
        - POSTGRES_USER: Username for authentication
        - POSTGRES_PASSWORD: Password for authentication
        - POSTGRES_HOST: Database host (e.g., localhost or a remote server)
        - POSTGRES_PORT: Database port (default for PostgreSQL is 5432)

    Returns:
        psycopg2.extensions.connection: An open connection object to the database.

    Raises:
        Exception: If the connection to the database fails for any reason.
    """
    try:
        # Get DB credentials from environment variables
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )
        return conn
    
    except Exception:
        # Reraise the exception to be handled by the calling function
        raise



def notify_jobs(conn, cur, notification_title: str, query: str):
    """
    Fetch jobs from the database using the given SQL query,
    format them into a string, and send a notification via ntfy.

    Args:
        conn (psycopg2.extensions.connection): Active database connection.
        cur (psycopg2.extensions.cursor): Cursor object for executing queries.
        notification_title (str): Title to display in the ntfy notification.
        query (str): SQL query to fetch job records. 
                     The query must return at least:
                     - id
                     - title
                     - link
                     - published
                     - author
                     - category
                     - fetched_at

    Returns:
        list[int]: A list of job IDs that were included in the notification.
                   Returns an empty list if no jobs are found.

    Side Effects:
        - Closes the cursor and connection if no jobs are found.

    Raises:
        Exception: Propagates any exceptions raised during query execution 
                   or the notification request.
    """
    cur = conn.cursor()

    # Run the query to fetch jobs
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    jobs = [dict(zip(columns, row)) for row in rows]

    # If no jobs were found, clean up and return
    if not jobs:
        conn.close()
        cur.close()
        return []

    # Format job details into a single string
    job_string = "\n\n".join(
        f"Title: {job['title']}\n"
        f"Link: {job['link']}\n"
        f"Published: {job['published']}\n"
        f"Author: {job['author']}\n"
        f"Category: {job['category']}\n"
        f"Fetched At: {job['fetched_at']}"
        for job in jobs
    )

    # Send notification to ntfy
    requests.post(
        f"https://ntfy.sh/{ntfy_topic}",
        data=job_string,
        headers={
            "Title": notification_title,
            "Priority": "4",
            "Tags": "briefcase",
            "Markdown": "yes"
        }
    )

    # Return IDs of notified jobs
    return [job['id'] for job in jobs]




def insert_jobs(jobs, notification_title: str):
    """
    Insert a list of job dictionaries into the `jobs` table and 
    trigger a notification for newly inserted jobs.

    Args:
        jobs (list[dict]): 
            A list of job records. Each dictionary must contain the keys:
            - title (str): Job title
            - link (str): Unique job URL (used for conflict resolution)
            - published (datetime or str): Published timestamp
            - author (str): Job author / organization
            - category (str): Job category
            - fetched_at (datetime or str): Timestamp when the job was fetched
        notification_title (str): 
            The title of the notification sent after insertion.

    Workflow:
        1. Establish a database connection.
        2. Prepare the job data as tuples and insert into the `jobs` table.
           - Uses `ON CONFLICT (link) DO NOTHING` to skip duplicates.
        3. Commit the transaction.
        4. Send a notification containing all jobs with 
           `application_status = 'pending'`.

    Returns:
        None

    Raises:
        Exception: Any database or notification errors will be raised 
                   after logging.
    """
    try:
        # Get DB connection
        conn = db_connection()
        cur = conn.cursor()

        # Insert query with conflict handling
        insert_query = """
            INSERT INTO jobs (title, link, published, author, category, fetched_at)
            VALUES %s
            ON CONFLICT (link) DO NOTHING;
        """

        # Prepare values as tuples
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

        # Bulk insert all jobs
        execute_values(cur, insert_query, values)

        conn.commit()
        print(f"{cur.rowcount} jobs inserted.")

        # Send notifications for new pending jobs
        notify_jobs(
            conn=conn,
            cur=cur,
            query="""
                SELECT id, title, link, published, author, category, fetched_at 
                FROM jobs 
                WHERE application_status = 'pending'
            """,
            notification_title=notification_title
        )

    except Exception as e:
        print("Error inserting jobs:", e)
        raise
    finally:
        # Ensure cleanup
        if cur:
            cur.close()
        if conn:
            conn.close()



def send_reminders():
    """
    Send reminder notifications for jobs that are still in 'pending' status
    and have received fewer than 15 reminders. Each reminder is logged in 
    the `reminders` table.

    Workflow:
        1. Open a database connection and cursor.
        2. Use a query with a CTE (common table expression) to:
            - Count the number of reminders per job.
            - Select all jobs where `application_status = 'pending'`
              AND reminder count < 15.
        3. Pass the results to `notify_jobs` to send a notification 
           via ntfy.
        4. Collect the IDs of jobs that were reminded.
        5. Insert a new reminder record for each job into the `reminders` table.
        6. Commit the transaction and close the connection.

    Returns:
        None

    Raises:
        Exception: Any errors during database operations or notification 
                   delivery are propagated.
    """

    # get db conn
    conn = db_connection()
    cur = conn.cursor()

    # Notify pending jobs that have fewer than 15 reminders
    reminded_ids = notify_jobs(
        conn=conn,
        cur=cur,
        query="""
            WITH reminder_count AS (
                SELECT job_id, COUNT(*) AS count
                FROM reminders
                GROUP BY job_id
            )
            SELECT j.id, j.title, j.link, j.author, j.published, 
                   j.fetched_at, j.category, 
                   COALESCE(rc.count, 0) AS reminder_count
            FROM jobs j
            LEFT JOIN reminder_count rc
            ON j.id = rc.job_id
            WHERE j.application_status = 'pending' 
              AND COALESCE(rc.count, 0) < 15;
        """,
        notification_title="Reminder: Pending Job Applications"
    )

    # Insert new reminder entries
    if reminded_ids:
        cur.executemany(
            "INSERT INTO reminders (job_id) VALUES (%s)",
            [(job_id,) for job_id in reminded_ids]
        )

    conn.commit()
    cur.close()
    conn.close()



send_reminders()