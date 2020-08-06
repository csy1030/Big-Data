import time, requests, json, csv, random, pymysql
from queue import Queue
import pyodbc
import config

server = "bigdatacsy.database.windows.net"
user = "sam"
password = "NYUBigData!"
db = "bigdata_db"
driver = '{ODBC Driver 17 for SQL Server}'


class SalarySpider:
    """:arg
    get salary data from www.monster.com
    """

    def __init__(self):
        self.detail_list = []
        self.count = 0
        # url should be "New York City NY"
        self.url = "https://www.monster.com/SalaryProfileHandler.ashx?Occupation={}&location={}"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.68 Safari/537.36 Edg/84.0.522.28"
        }
        self.url_queue = Queue()
        self.job_details = []
        self.db = pyodbc.connect(
            'DRIVER=' + driver + ';PORT=1433;SERVER=' + server + ';PORT=1443;DATABASE=' + db + ';UID=' + user + ';PWD=' + password)
        self.cur = self.db.cursor()

    def get_detail(self):
        """:arg
        get different jobs median salary in different cities.
        """
        while True:
            if self.url_queue.empty():
                break
            item = self.url_queue.get()
            url = item[0]
            job = item[1]
            loc = item[2]
            print(url)
            json_parse = requests.get(
                url=url,
                headers=self.headers
            ).json()
            if "bylocation" not in json_parse[0]:
                continue
            try:
                salary_min = json_parse[0]["bylocation"]["salary"]["Percentile_10"]
                salary_med = json_parse[0]["bylocation"]["salary"]["Percentile_50"]
                salary_max = json_parse[0]["bylocation"]["salary"]["Percentile_90"]
                demand = json_parse[0]["bylocation"]["demand"]
            except Exception:
                continue
            print(salary_min, salary_med, salary_max, demand)
            job_detail_tuple = (job, loc, salary_min, salary_med, salary_max, demand)
            self.job_details.append(job_detail_tuple)
            self.count += 1
            print('parsing {}...'.format(self.count))

    def write_csv(self, job, pages):
        with open("{}_{}pages.csv".format(job, pages), "w", newline='', encoding='utf-8-sig', errors='ignore') as f:
            print('writing in CSV...')
            w = csv.writer(f)
            w.writerow(('query', 'title', 'company', 'location', 'description'))
            w.writerows(self.job_details)

    def insert_sql(self):
        sql = 'insert into salary_info (job,loc,salary_min,salary_med,salary_max,demand) values (?,?,?,?,?,?);'
        print('Writing in MySQL...')
        self.cur.executemany(sql, self.job_details)
        self.db.commit()

        print('Written in MySQL succesfully!')

    def main(self):
        time_start = time.time()
        job_list = config.job_list_salary
        loc_dict = config.location_dict
        for job in job_list:
            for loc in loc_dict:
                job_mod = "%20".join(job.split(" "))
                loc_mod = "%20".join(loc.split(" "))
                self.url_queue.put([self.url.format(job_mod, loc_mod), job, loc])
        self.get_detail()
        # self.write_csv(job,pages)
        self.insert_sql()
        time_end = time.time()
        print("{} s,{} records".format(round((time_end - time_start), 2), self.count))
        self.cur.close()
        self.db.close()

    def create_table(self):
        sql = 'create table if not exists salary_info(id int identity(1,1) primary key, job varchar(60),loc varchar(30),salary_min int,salary_med int,salary_max int,demand varchar(20));'

        self.cur.execute(sql)
        self.cur.commit()


if __name__ == '__main__':
    spider = SalarySpider()
    spider.main()
