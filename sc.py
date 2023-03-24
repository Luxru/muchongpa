import argparse
import time
import requests 
import sqlite3
import hashlib
import datetime
from loguru import logger
from lxml import etree
from school import get_college_list


class Item():
    ___211_list,__985_list = get_college_list()
    def __init__(self,title:str,date:str,hc:int,herf:str,school:str="",major:str="") -> None:
        type = Item.__find_school_type(school)
        self.__fmt_str = "%Y-%m-%d %H:%M"
        self.__title = title
        self.__school = school
        self.__type = type
        self.__major = major
        self.__href = herf
        self.__hc = hc
        self.__date = datetime.datetime.strptime(date,self.__fmt_str)

    def cmp_date(self,date:datetime.datetime)->bool:
        # if self > arg return true
        if(self.__date>date):
            return True
        return False
    
    def get_date_ref(self):
        return self.__date

    def get_insert_data_str(self)->tuple:
        return (self.__title,self.__school,self.__type,self.__major,self.__href,self.__hc,self.cal_md5(),self.__date)

    def cal_md5(self)->str:
        m = hashlib.md5()
        m.update((self.__title+self.__href+str(self.__hc)+self.__date.strftime(self.__fmt_str)).encode("utf-8"))
        return m.hexdigest()
        
    def __str__(self):
        return f"标题：{self.__title}\n学校：{self.__school}\n链接：{self.__href}\n专业：{self.__major}\n招生人数：{self.__hc}\n类型：{self.__type}\n时间：{self.__date}\n"

    @classmethod
    def __find_school_type(self,school:str)->str:
        if(school==""):
            return ""
        else:
            if(school in Item.__985_list):
                return "985"
            elif(school in Item.___211_list):
                return "211"
        return "双非"

class Papani():
    def __init__(self,maxpage,minpage=1) -> None:
        self.__s = requests.Session()
        self.__header = {
            "user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70"
        }
        self.__s.headers.update(self.__header)
        self.__baseurl = "http://muchong.com/bbs/kaoyan.php"
        self.__maxpage = maxpage
        self.__minpage = minpage
        self.__db = Database()

    
    def __find_page_bydate(self,date:datetime.datetime)->int:
        i=self.__minpage;j=self.__maxpage;mid = (self.__minpage+j)//2;f = 1
        while f and i<j:
            items = self.__get_msgs_page(mid)
            for item in items:
                s = (date - item.get_date_ref()).total_seconds()
                logger.info(f"Finding date:{date} --> page:{mid}")
                if(s>3600*24):
                    j = mid-1
                    mid = (i+j)//2
                elif(s<-3600*24):
                    i = mid+1
                    mid = (j+i)//2
                else:
                    f = 0
                break
        logger.info(f"Found date:{date} --> page:{mid}")
        return mid


    def __get_msgs_page(self,page)->list[Item]:
        page_l = []
        if(page>1):
            page_l.append(page-1)
        page_l.append(page)
        r:requests.Response = self.__s.get(self.__baseurl,params={"page":page_l},timeout=10)
        return self.__parase_html(r.text)

    def __parase_html(self,text)->list[Item]:
        root:etree._Element = etree.HTML(text,etree.HTMLParser())
        if(root is None):
            raise Exception("parase failed")
        result:etree._Element = root.xpath(r'//tbody[@class="forum_body_manage"]')
        items = []
        for el in result[0].getchildren():
            a = el.find('.//a')
            title = a.text
            href = a.get("href")
            infos = [x.text for x in el.findall('.//td')]
            school = infos[1]
            major = infos[2]
            hc = infos[3]
            date = infos[4]
            item = Item(title=title,date=date,herf=href,hc=int(hc),school=school,major=major)
            items.append(item)
        return items

# time: [now........past] -> forward  <-backward
    def __get_forward_list(self)->list[int]:
        start_time = self.__db.get_pubdate(False)
        i = self.__find_page_bydate(start_time)
        j = self.__maxpage
        return [x for x in range(i,j+1)]

    def __get_backward_list(self)->list[int]:
        end_time = self.__db.get_pubdate(True)
        i = self.__minpage
        j = self.__find_page_bydate(end_time)
        return [x for x in range(i,j+1)]

    def __collect_list(self,page_list:list[int]): 
        x = 0
        while x<len(page_list):
            try:
                items = self.__get_msgs_page(page_list[x])
                rowcount = self.__db.save_msg(items)
                logger.success(f"Insert success, records effected:{rowcount} msg-len:{len(items)}, Current page:{page_list[x]}")
            except requests.exceptions.Timeout as exp:
                logger.error(f"Connection timeout: {exp}")
                time.sleep(10)
                continue
            else:
                x+=1
                time.sleep(0.01)
   
    def collect_auto(self):
        logger.info("Collect start") 
        while True:
            try:
                for pages in [self.__get_backward_list(),self.__get_forward_list()]:
                    self.__collect_list(pages)
            except Exception as exp:   
                logger.error(f"Exp:{exp}, continue")
                continue
                # raise
            logger.info(f"Collect end, quit")
            break

    def collect_range(self):
        logger.info("Collect start")
        while True:
            try:
                self.__collect_list([x for x in range(self.__minpage,self.__maxpage+1)])
            except Exception as exp:   
                logger.error(f"Exp:{exp}, continue")
                continue
                # raise
            logger.info(f"Collect end, quit")
            break

class Database():
    def __init__(self) -> None:        
        self.__con = sqlite3.connect("./sc.db",detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self.__con.execute(
            '''CREATE TABLE IF NOT EXISTS sc
        (ID INTEGER PRIMARY KEY  AUTOINCREMENT,
        Title VARCHAR(255) NOT NULL,
        School VARCHAR(255),
        Type VARCHAR(20),
        Major VARCHAR(255),
        Href VARCHAR(255) NOT NULL,
        HC INT,
        INFO_MD5 CAHR(32) UNIQUE NOT NULL,
        PubDate timestamp NOT NULL,
        InsertDate timestamp DEFAULT CURRENT_TIMESTAMP);
        '''
        )
        self.__con.commit()

    def save_msg(self,msg_list: list[Item])->int:
        data = []
        for x in msg_list:
            data.append(x.get_insert_data_str())
        try:
            cur = self.__con.cursor()
            cur.executemany("INSERT OR IGNORE INTO sc(Title,School,Type,Major,Href,HC,INFO_MD5,PubDate) VALUES ({})".format("".join(["?," for x in range(8)])[:-1]),data)
            self.__con.commit()
        except Exception as exp:
            logger.error("Insert failed,  {}".format(exp))
            raise 
        else:
            return cur.rowcount
        finally:
            cur.close()

    def get_pubdate(self,is_latest:bool)->datetime.datetime:
        try:
            cur = self.__con.cursor()
            cur.execute("SELECT PUBDATE FROM sc ORDER BY PUBDATE {};".format(("DESC","ASC")[not is_latest]))
            if(res:=cur.fetchone()):
                return res[0]
            else:
                raise sqlite3.DataError("There's no date in the database")
        except Exception as exp:
            logger.error(f"Failed when:{exp}")
            raise
        finally:
            cur.close()

def main():
    parser = argparse.ArgumentParser(description='爬取小木虫调剂信息.')
    parser.add_argument('maxpage', metavar='maxpage', type=int,help='结束页面的页码')
    parser.add_argument('--minpage', metavar='num', type=int, nargs='?',default=1,help='开始页面的页码，默认从第一页开始')
    parser.add_argument('--auto', action='store_const', default=False, const=True, help='是否开启自动模式，默认顺序存取到结束页')
    args = parser.parse_args()
    pa = Papani(args.maxpage,args.minpage)
    if args.auto:
        pa.collect_auto()
    else:
        pa.collect_range()
 
if __name__=="__main__": 
    main()
