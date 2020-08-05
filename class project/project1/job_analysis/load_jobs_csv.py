import pyodbc, csv
import config

server = "bigdatacsy.database.windows.net"
user = "sam"
password = "NYUBigData!"
db = "bigdata_db"
driver = '{ODBC Driver 17 for SQL Server}'


class LoadData:
    def __init__(self):
        self.db = pyodbc.connect(
            'DRIVER=' + driver + ';PORT=1433;SERVER=' + server + ';PORT=1443;DATABASE=' + db + ';UID=' + user + ';PWD=' + password)
        self.cur = self.db.cursor()
        self.job = config.job_list_query
        self.job_details = []

    def load_data(self,job):
        self.job_details = []
        # query = "select * from job_info where job_query = ?"
        query = "select * from job_info_no_dup"
        self.cur.execute(query)
        result = self.cur.fetchall()
        for item in result:
            self.job_details.append(item)
        print('Loading from MySQL...')

    def write_csv(self,job):
        # job = '_'.join(job.split(' '))
        with open("job_db.csv".format(job), "w", newline='', encoding='utf-8-sig', errors='ignore') as f:
        # with open("jobs_csv/{}.csv".format(job), "w", newline='', encoding='utf-8-sig', errors='ignore') as f:
            print('writing in csv...',job)
            w = csv.writer(f)
            w.writerow(('id', 'job-query', 'title', 'company', 'location', 'description'))
            w.writerows(self.job_details)

    def main(self):
        # for item in self.job:

        self.load_data(0)
        self.write_csv(0)


if __name__ == '__main__':
    ld = LoadData()
    ld.main()
