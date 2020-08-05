import codecs
import time,requests,json,csv,random,pymysql

import re
from lxml import etree

class MonsterSpider:
    def __init__(self):
        # job name in query should be connected by '-',page start from 1
        self.url = "http://www.monster.com/jobs/search/pagination/?q={}&isDynamicPage=true&isMKPagination=true&page={}"
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Connection': 'keep-alive',
            'Cookie': 'fingerprint=0e4899d6394771c550c2e0a04fdaa48a; SSALR_NonModal_Interaction=true; ASP.NET_SessionId=edzh4xjieypczu2dxqkuv3rt; viewercountry=US; check=true; AMCV_A86C525A548992260A4C98C6%40AdobeOrg=1256414278%7CMCMID%7C09721714665083437873758005962808455971%7CMCAID%7CNONE; s_cc=true; js_abs=13|True; jsr_hybd=1|377782396; 58_JSRadius=20; SortBy=rv.di.dt; js_xys=20|False; jsr_trvxsplt=16|774595554; Ssr_StatesExclude=1|824233132; Ssr_RestrictKwdParam=2|589489524; jsr_searchfallback=3|189360501; jsr_eptysrchsplt=2|743732991; jsr_multiphyloc=1|1; jsr_descsnippet=3|306743047; jsr_usebrandingclogo=3|959125322; jsr_FitIconScoreCard=0|791534291; jsr_fltrsqnc=1|313143866; jsr_fenced=1|993787890; jsr_mdlysplt=9|True; jsr_pure=1|52347030; jsr_radiusinheader=0|185220510; jsr_bmlitesplt=3|129967386; jsr_splt=11|True; jsr_vwsplt=7|6; jsr_qlfytgsplt=4|155233727; typeaheads_abs=3|2; jsr_sldsp=0|1; jsr_savedjobssplt=1|940384626; jsr_jvnonjobcontent=4|453894373; atmResolver=|Seeker|lpf|58|164|; jsr_mbfltr=1|1; m_dvfpid=0e4899d6394771c550c2e0a04fdaa48a; G_ENABLED_IDPS=google; jsr_medleysplt=1|1; AdCtxViewedJobs=%5B%22plid%3D470%26pcid%3D660%26poccid%3D11970%22%2C%22plid%3D470%26pcid%3D660%26poccid%3D11970%22%2C%22plid%3D362%26pcid%3D660%26poccid%3D11970%22%2C%22plid%3D702%26pcid%3D660%26poccid%3D11970%22%2C%22plid%3D383%26pcid%3D1%26poccid%3D11803%22%5D; AdCtxViewedJobsKeyword=Financial%20Analyst; s_sq=%5B%5BB%5D%5D; jsCrit=q%3dsoftware%2520engineer%26brd%3d1%2c2%26ispowersearch%3d1; rcnt.srch=Financial-Analyst%7c%7c%7c%7c%7c%7c6%2f17%2f2020+10%3a46%3a28+PM%7c%7c%7cFinancial%2bAnalyst%7c%7c%7c%2fsearch%2f%3fq%3dFinancial-Analyst%5e%5e%5eSoftware-Engineer%7c%7c%7c%7c%7c%7c6%2f17%2f2020+10%3a45%3a27+PM%7c%7c%7cSoftware%2bEngineer%7c%7c%7c%2fsearch%3fq%3dSoftware-Engineer%5e%5e%5eSoftware-Engineer%7c%7c%7c%7c%7c%7c6%2f18%2f2020+12%3a12%3a36+AM%7c%7c%7cSoftware-Engineer%7c%7c%7c%2fsearch%2f%3fq%3dSoftware-Engineer; prsv.rcnt.srch=Financial-Analyst%7c%7c%7c%7c%7c%7c6%2f17%2f2020+10%3a46%3a28+PM%7c%7c%7cFinancial%2bAnalyst%7c%7c%7c%2fsearch%2f%3fq%3dFinancial-Analyst%5e%5e%5eSoftware-Engineer%7c%7c%7c%7c%7c%7c6%2f17%2f2020+10%3a45%3a27+PM%7c%7c%7cSoftware%2bEngineer%7c%7c%7c%2fsearch%3fq%3dSoftware-Engineer%5e%5e%5eSoftware-Engineer%7c%7c%7c%7c%7c%7c6%2f18%2f2020+12%3a12%3a36+AM%7c%7c%7cSoftware-Engineer%7c%7c%7c%2fsearch%2f%3fq%3dSoftware-Engineer; apas_trgtng={%22mesco%22:[1500127001001%2C1500128001001]}; jsrm_ads=0|320711426',
            'Host': 'www.monster.com',
            'Referer': 'https://www.monster.com/jobs/search/?q=software-engineer&stpage=1&page=2',
            'sec-ch-ua': '"\\Not\"A;Brand";v="99", "Chromium";v="84", "Microsoft Edge";v="84"',
            'sec-ch-ua-mobile': '?0',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.45 Safari/537.36 Edg/84.0.522.20',
        }
        self.db = pymysql.connect('localhost','root','cao10300',port='3306',db='job')
        self.cur = self.db.cursor()

    def get_link(self, job, page):
        print("Trying to enter...")
        json_parse = requests.get(
            url = self.url.format(job,page),
            headers = self.headers
        ).json()
        print('Website Entered')
        detail_link_list = []
        for item in json_parse:
            # line holder
            if 'InlineApasAd' in item:
                continue
            # get detail link from one page
            else: detail_link_list.append(item["TitleLink"])
        return detail_link_list

    def get_detail(self,url,job):
        xpath_title = "//h1[@name='job_title']/text()"
        xpath_company = "//div[@name='job_company_name']/text()"
        xpath_loc ="//div[@name='job_company_location']/text()"
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
                    re_desc +='\n'
        else:
            description = html_parse.xpath(xpath_desc_div)
            re_desc = ''
            for item in description:
                if item != r'\xa0':
                    re_desc += item
                    re_desc += '\n'

        job_detail_tuple = (job,title,company,location,re_desc)
        return job_detail_tuple

    def write_csv(self,file,job,pages):
        with open("{}_{}pages.csv".format(job,pages), "w",newline='',encoding='utf-8-sig',errors='ignore') as f:

            w = csv.writer(f)
            w.writerow(('query','title','company','location','description'))
            w.writerows(file)

    def insert_sql(self,detail_list):
        sql = 'insert into job values(%s,%s,%s,%s,%s)'
        self.cur.executemany(sql,detail_list)
        self.db.commit()
        self.cur.close()
        self.db.close()

    def test(self):
        url = "https://job-openings.monster.com/perm-sr-full-stack-engineer-platform-interfaces-python-java-new-york-ny-us-winterwyman/218322796"

        headers = {"User-Agent": "Mozilla/5.0"}
        html = requests.get(url=url,
                            headers=headers).text
        with open('detail2.html','w') as f:
            f.write(html)

    def main(self):
        time_start = time.time()
        job = input('input job:')
        job = "-".join(job.split(' '))
        pages = int(input('Input pages you want to scrape:'))
        detail_list = []
        count = 0
        for i in range(1,pages+1):
            detail_links = self.get_link(job, i)
            for url in detail_links:
                detail_list.append(self.get_detail(url,job))
                # time.sleep(random.randint(1,2))
                count += 1
                print('writing {}...'.format(count))
                # print(url)

        self.write_csv(detail_list,job,pages)
        self.insert_sql(detail_list)
        time_end = time.time()
        print("{} s,{} records".format(round((time_end-time_start),2),count))

if __name__ == '__main__':
    spider = MonsterSpider()
    spider.main()