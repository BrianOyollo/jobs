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



def notify_jobs(cur, notification_title: str, query: str, new_job_ids: list | None = None):
    """
    Fetch matching jobs from the database and send a formatted notification
    to an ntfy topic.

    Args:
        cur (psycopg2.extensions.cursor):
            Active database cursor used to execute SQL.
        notification_title (str):
            Title text to display in the ntfy notification header.
        query (str):
            SQL query to run when `new_job_ids` is not provided.
            Must return the following columns:
                id, title, link, published, author, category, fetched_at
        new_job_ids (list[int] | None, optional):
            If provided, overrides `query` and limits the notification
            to rows whose `id` is in this list **and** whose
            `application_status` is 'pending'.

    Returns:
        list[int]:
            A list of job IDs included in the notification.
            Returns an empty list if no rows match.

    Behavior:
        When `new_job_ids` is given:
            Executes a parameterized query:
                SELECT id, title, link, published, author, category, fetched_at
                FROM jobs
                WHERE id = ANY(%s) AND application_status = 'pending'
        Otherwise:
            Executes the supplied `query` as-is.

        The resulting rows are converted to dictionaries, formatted as
        a Markdown string, and sent to the ntfy topic defined by the
        global `ntfy_topic` variable.

    Raises:
        Exception:
            Propagates any database or network errors encountered while
            executing the query or posting the notification.
    """


    # Run the query to fetch jobs
    if new_job_ids:
        print(f"new_job_ids: {new_job_ids}")
        query="""
            SELECT id, title, link, published, author, category, fetched_at 
            FROM jobs 
            WHERE id = ANY(%s) AND application_status = 'pending'
        """
        cur.execute(query, (new_job_ids,))
        rows = cur.fetchall()
    else:
        cur.execute(query)
        rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]
    jobs = [dict(zip(columns, row)) for row in rows]


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




def insert_jobs(conn, cur, jobs: list[dict], notification_title: str):
    """
    Bulk-insert new job postings into the `jobs` table and notify about
    any rows that were actually inserted.

    Args:
        conn (psycopg2.extensions.connection):
            Active PostgreSQL connection object.
        cur (psycopg2.extensions.cursor):
            Cursor bound to `conn` for executing SQL.
        jobs (list[dict]):
            List of job records. Each dictionary must include:
                - title (str)       : Job title.
                - link (str)        : Unique job URL (used for conflict detection).
                - published (datetime | str): Published timestamp.
                - author (str)      : Job author / organization.
                - category (str)    : Job category or type.
                - fetched_at (datetime | str): Timestamp of the fetch.
        notification_title (str):
            Title to display in the ntfy notification triggered after insertion.

    Workflow:
        1. Prepares a bulk INSERT statement:
               INSERT INTO jobs (title, link, published, author, category, fetched_at)
               VALUES %s
               ON CONFLICT (link) DO NOTHING
               RETURNING id;
           • `ON CONFLICT` ensures duplicate `link` values are skipped.
        2. Uses psycopg2.extras.execute_values to insert all rows in one round trip.
           • The RETURNING clause yields only the IDs of rows that were
             newly inserted (duplicates return nothing).
        3. Commits the transaction.
        4. If any rows were inserted, calls `notify_jobs` to send a notification
           containing only those new rows that still have
           application_status = 'pending'.

    Returns:
        None

    Raises:
        Exception:
            Any database or notification-related errors are propagated
            after being printed to stdout.
    """

    try:

        # Insert query with conflict handling
        insert_query = """
            INSERT INTO jobs (title, link, published, author, category, fetched_at)
            VALUES %s
            ON CONFLICT (link) DO NOTHING
            RETURNING id;
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
        print(values)

        # Bulk insert all jobs
        new_jobs = execute_values(cur, insert_query, values, fetch=True)
        conn.commit()
        
        print(f"{len(new_jobs)} jobs inserted.")
        print(new_jobs)

        # Send notifications for new pending jobs
        new_job_ids = [row[0] for row in new_jobs]
        print(new_job_ids)

        if len(new_jobs) > 0:
            notify_jobs(
                cur=cur,
                query="""
                    SELECT id, title, link, published, author, category, fetched_at 
                    FROM jobs 
                    WHERE application_status = 'pending'
                """,
                notification_title=notification_title,
                new_job_ids = new_job_ids
            )

    except Exception as e:
        print("Error inserting jobs:", e)
        raise



def send_reminders(conn, cur):
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

    # Notify pending jobs that have fewer than 15 reminders
    reminded_ids = notify_jobs(
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

