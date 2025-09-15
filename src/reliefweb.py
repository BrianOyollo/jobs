import feedparser
from datetime import datetime
from utils import insert_jobs




def extract_jobs(rss_feed_link):
    feed  = feedparser.parse(rss_feedlink)
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

rss_feedlink = "https://reliefweb.int/jobs/rss.xml?advanced-search=%28CC6866%29_%28C131%29"
jobs = extract_jobs(rss_feedlink)
insert_jobs(jobs, notification_title='Jobs from Relief Web')

