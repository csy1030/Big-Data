import time, requests, json, csv, random, pymysql
from threading import Thread
from queue import Queue
from lxml import etree
import pyodbc
import config

server = "bigdatacsy.database.windows.net"
user = "sam"
password = "NYUBigData!"
db = "bigdata_db"
driver = '{ODBC Driver 17 for SQL Server}'


class JobSpider:
    """
    scrape the data from www.monster.com using request, xpath
    """

    def __init__(self):
        self.detail_list = []
        self.count = 0
        self.url = "http://www.monster.com/jobs/search/pagination/?q={}&isDynamicPage=true&isMKPagination=true&page={}"
        self.headers = config.job_spider_headers
        self.url_queue = Queue()
        self.job_details = []
        self.db = pyodbc.connect(
            'DRIVER=' + driver + ';PORT=1433;SERVER=' + server + ';PORT=1443;DATABASE=' + db + ';UID=' + user + ';PWD=' + password)
        self.cur = self.db.cursor()

    def get_link(self, job, page):
        """
        Get secondary links (25 records on one page)
        """
        json_parse = requests.get(
            url=self.url.format(job, page),
            headers=self.headers
        ).json()
        print('url{}...'.format(page))
        for item in json_parse:
            # ignore line holder
            if 'InlineApasAd' in item:
                continue
            # get detail link from one page
            else:
                self.url_queue.put(item["TitleLink"])

    def get_detail(self, job):
        """
        Parse the HTML to get title, company, location, job_description
        """
        while True:
            if self.url_queue.empty():
                break
            url = self.url_queue.get()
            xpath_title = "//h1[@name='job_title']/text()"
            xpath_company = "//div[@name='job_company_name']/text()"
            xpath_loc = "//div[@name='job_company_location']/text()"
            xpath_desc_p = "//div[@name='job_description']//p/text()"
            xpath_desc_div = "//div[@name='job_description']//div/text()"
            headers = {"User-Agent": "Mozilla/5.0"}
            html = requests.get(url=url,
                                headers=headers).text
            html_parse = etree.HTML(html)

            # xpath title
            title_ori = html_parse.xpath(xpath_title)
            title = ''
            for item in title_ori:
                if item != ' ':
                    title = item
                    break

            # xpath company name
            company_ori = html_parse.xpath(xpath_company)
            company = ''
            for item in company_ori:
                if item:
                    company = item
                    break

            # xpath location
            loc_ori = html_parse.xpath(xpath_loc)
            location = ''
            for item in loc_ori:
                if item:
                    location = item

            # xpath description
            description = html_parse.xpath(xpath_desc_p)
            if description:
                re_desc = ''
                for item in description:
                    if item != r'\xa0':
                        re_desc += item
                        re_desc += '\n'
            else:
                description = html_parse.xpath(xpath_desc_div)
                re_desc = ''
                for item in description:
                    if item != r'\xa0':
                        re_desc += item
                        re_desc += '\n'
            job_detail_tuple = (job, title, company, location, re_desc.strip())
            self.job_details.append(job_detail_tuple)
            self.count += 1
            print('parsing {}...'.format(self.count))

    def insert_sql(self):
        """:arg
        Insert into Azure MySQL
        """
        sql = 'insert into job_info (job_query,title,company,location,description) values (?,?,?,?,?);'
        print('Writing in MySQL...')
        self.cur.executemany(sql, self.job_details)
        self.db.commit()
        print('Written in MySQL succesfully!')

    def write_csv(self, job, pages):
        with open("{}_{}pages.csv".format(job, pages), "w", newline='', encoding='utf-8-sig', errors='ignore') as f:
            print('writing in MySQL...')
            w = csv.writer(f)
            w.writerow(('query', 'title', 'company', 'location', 'description'))
            w.writerows(self.job_details)

    def create_table(self):
        sql = 'create table if not exists job_info(id int identity(1,1) primary key, job_query varchar(30),title varchar(200),company varchar(200),location varchar(80),description text);'
        self.cur.execute(sql)
        self.cur.commit()

    def main(self):
        time_start = time.time()
        job_list = config.job_list_query
        pages = 20
        for job in job_list:
            for i in range(1, pages + 1):
                self.get_link(job, i)
            t_list = []
            for _ in range(20):
                t = Thread(target=self.get_detail, args=(job,))
                t_list.append(t)
                t.start()
            for k in t_list:
                k.join()

            # self.write_csv(job,pages)
            self.insert_sql()
            time_end = time.time()
            print("{} s,{} records".format(round((time_end - time_start), 2), self.count))
        self.cur.close()
        self.db.close()


if __name__ == '__main__':
    spider = JobSpider()
    spider.main()
