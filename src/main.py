import utils
import reliefweb


def main():

    # db connection
    conn = utils.db_connection()
    cur = conn.cursor()

    # ReliefWeb
    rss_feedlink = "https://reliefweb.int/jobs?advanced-search=%28CC6866%29_%28C131%29"
    # rss_feedlink="/home/brianoyollo/Downloads/rss(1).xml"
    jobs = reliefweb.extract_jobs(rss_feedlink)
    utils.insert_jobs(conn, cur, jobs, notification_title='Jobs from Relief Web')


    # close db connections
    cur.close()
    conn.close()



main()