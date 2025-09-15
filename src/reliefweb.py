import feedparser
from datetime import datetime
from utils import insert_jobs




def extract_jobs(rss_feed_link):
    feed  = feedparser.parse(rss_feed_link)
    jobs = []

    for entry in feed.entries:
        job = {
            "title": entry.get("title"),
            "link": entry.get("link"),
            "published": entry.get("published"),
            "updated": entry.get("updated"),
            "author": entry.get("author"),
            "fetched_at": datetime.now(),
            "category": "Information and Communications Technology"
        }

        jobs.append(job)
    return jobs

