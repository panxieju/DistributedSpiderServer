import random

from scrapy.http import HtmlResponse
from bs4 import BeautifulSoup
import requests

# 常用的用户代理列表
userAgentList = [
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
]


# 下载赶集网的网页
def downloadHttpResponse(url):
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    html = requests.get(url=url, headers={'User-Agent': random.choice(userAgentList)}, verify=False)
    return HtmlResponse(body=html.content, url=url, status=html.status_code)


def downloadHttpsResponse(url):
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    html = requests.get(url=url, headers={'User-Agent': random.choice(userAgentList)}, verify=False)
    return HtmlResponse(body=html.content, url=url, status=html.status_code)


# 打印ＨＴＭＬ的内容
def showHtml(html):
    soup = BeautifulSoup(html, "html.parser")
    print(soup.prettify())


# 打印Request下载的内容
def showRequestDownload(download):
    soup = BeautifulSoup(download.content, "html.parser")
    print(soup.prettify())


# 答应Scrapy的HtmlResponse的内容
def showHtmlResponse(response):
    showHtml(response.body)


def downloadFangBeijing(url):
    header = {'User-Agent': random.choice(userAgentList),
              'Referer': 'http://zu.gz.fang.com/',
              'Upgrade-Insecure-Requests': '1',
              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
              'Accept-Encoding': 'gzip, deflate',
              'Accept-Language': 'zh-CN,zh;q=0.9',
              'Cache-Control': 'max-age=0',
              'Connection': 'keep-alive',
              'Host': 'zu.fang.com',
              'Cookie': 'global_cookie=d1ng91ntpoiccv0c9xsa183l92yj5rtcjxy; integratecover=1; SoufunSessionID_Rent=3_1514950642_13197; __utmz=147393320.1514962258.6.5.utmcsr=zu.gz.fang.com|utmccn=(referral)|utmcmd=referral|utmcct=/; Integrateactivity=notincludemc; lastscanpage=0; SoufunSessionID_Esf=3_1515989349_30436; newhouse_user_guid=60E02F62-57D5-172D-6107-7632480C1091; vh_newhouse=3_1515989669_4429%5B%3A%7C%40%7C%3A%5D7e4022874033a06875532a9f59fd1107; new_search_uid=b31f50382cb93e51b0af1aafca05f1c4; searchLabelN=3_1515989696_7705%5B%3A%7C%40%7C%3A%5Dc82b237bf8aafc0ba9777478cd49105b; searchConN=3_1515989696_7967%5B%3A%7C%40%7C%3A%5D6dd08a45d62d74babebad3bb9c343367; sf_source=; s=; showAdgz=1; indexAdvLunbo=lb_ad1%2C0%7Clb_ad2%2C0%7Clb_ad5%2C1%7Clb_ad6%2C0; __utma=147393320.1226144692.1501484911.1515989073.1516677010.8; __utmc=147393320; ASP.NET_SessionId=ibcctjlq3xqpwthn3zptbs5q; polling_imei=4794ddae21034975; city=www; __utmb=147393320.51.10.1516677010; Captcha=423148536E6D31637858592B343136377052376D75304B695367594B3631784A4E4F614833596C3847574873684D5873336A356A3250506175354530706C6F7235425341512B52562F35593D; unique_cookie=U_g8tcxzqdf1dowvnoie2xx9cej2ljcr2bz1y*15'
              }
    response = requests.get(url, headers=header, verify=True)
    return HtmlResponse(status=response.status_code, body=response.content,
                        url=response.url)


if __name__ == '__main__':
    url = 'https://gz.zu.anjuke.com/fangyuan/1136577460'
    showHtmlResponse(downloadHttpResponse(url))
