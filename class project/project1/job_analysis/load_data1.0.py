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
        self.job_details = []
        self.loc_dict = config.location_dict
        self.demand_dict = {
            'Very High': 5,
            'High': 4,
            'Medium': 3,
            'Low': 2,
            'Very Low': 1,
        }
        self.jobs = config.job_list_query

    def load_data(self):
        self.job_details = []
        query = "select * from job_info where job_query = ?"
        self.cur.execute(query, self.jobs)
        result = self.cur.fetchall()
        for item in result:
            self.job_details.append(item)

    def get_salary(self, job):
        """ get salary in same job different location
        """
        self.job_details = []
        query = "select job,loc,salary_med,demand from salary_info where job = ?;"
        self.cur.execute(query, job)
        self.job_details = self.cur.fetchall()

    def get_same_loc_diff_job(self, loc):
        self.job_details = []
        query = "select loc, job, salary_med, demand from salary_info where loc = ?;"
        self.cur.execute(query, loc)
        self.job_details = self.cur.fetchall()

    def write_csv(self, job):
        job = ''.join(job.split(" ")).replace('/', '')
        with open("salary_csv/job/{}_salary.csv".format(job), 'w', newline='', encoding='utf-8-sig',
                  errors='ignore') as f:
            print('writing in csv...')
            w = csv.writer(f)
            w.writerow(('job', 'location', 'salary_med', 'demand', 'lon', 'lat'))
            content_to_write = []
            for item in self.job_details:
                item = (item[0], item[1], item[2], self.demand_dict[item[3]], self.loc_dict[item[1]][0],
                        self.loc_dict[item[1]][1])
                content_to_write.append(item)
            w.writerows(content_to_write)



    def write_same_loc_diff_job(self, loc):
        loc = "_".join(loc.split(",")[0].split(" "))
        with open("salary_csv/location/{}.csv".format(loc), 'w', newline='', encoding='utf-8-sig',
                  errors='ignore') as f:
            print('writing in csv...')
            w = csv.writer(f)
            w.writerow(('location', 'job', 'salary_med', 'demand'))
            content_to_write = []
            for item in self.job_details:
                item = (item[0], item[1], item[2], self.demand_dict[item[3]])
                content_to_write.append(item)
            w.writerows(content_to_write)

    def main(self):
        job_list = ["Software Developer / Engineer", "IT manager", "Web Developer", "Database Administrator",
                    "Cyber Security Analyst", "Systems Analyst", "Network Engineer / Architect",
                    "Integrated Circuit (IC) Design Engineer",
                    "Hardware Design Engineer",
                    "Embedded Software Engineer", "Electrical Design Engineer", "Telecommunications Network Technician",
                    "Data Scientist"]
        loc_list = []
        loc_dict = config.location_dict
        for key in loc_dict:
            loc_list.append(key)
        while loc_list:
            loc = loc_list.pop(0)
            self.get_same_loc_diff_job(loc)
            # self.get_salary(job)
            # self.load_data()
            self.write_same_loc_diff_job(loc)
            # self.write_csv(job)


if __name__ == '__main__':
    ld = LoadData()
    ld.main()
