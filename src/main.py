from utils import insert_jobs, send_reminders
import reliefweb


def main():

    # ReliefWeb
    rss_feedlink = "https://reliefweb.int/jobs/rss.xml?advanced-search=%28CC6866%29_%28C131%29"
    jobs = reliefweb.extract_jobs(rss_feedlink)
    insert_jobs(jobs, notification_title='Jobs from Relief Web')


main()