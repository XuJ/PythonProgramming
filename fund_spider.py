# coding: utf-8
import os
from threading import Lock, Thread

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait


# 下面是利用 selenium 抓取html页面的代码

# 初始化函数
def initSpider():
    driver = webdriver.PhantomJS(executable_path=r"D:/phantomjs-2.1.1-windows/bin/phantomjs")
    driver.get("http://fund.eastmoney.com/f10/jjjz_110011.html")  # 要抓取的网页地址

    # 找到"下一页"按钮,就可以得到它前面的一个label,就是总页数
    getPage_text = driver.find_element_by_id("pagebar").find_element_by_xpath(
        "div[@class='pagebtns']/label[text()='下一页']/preceding-sibling::label[1]").get_attribute("innerHTML")
    # 得到总共有多少页
    total_page = int("".join(filter(str.isdigit, getPage_text)))

    # 返回
    return driver, total_page


# 获取html内容
def getData(myrange, driver, lock):
    for x in myrange:
        # 锁住
        lock.acquire()
        tonum = driver.find_element_by_id("pagebar").find_element_by_xpath(
            "div[@class='pagebtns']/input[@class='pnum']")  # 得到 页码文本框
        jumpbtn = driver.find_element_by_id("pagebar").find_element_by_xpath(
            "div[@class='pagebtns']/input[@class='pgo']")  # 跳转到按钮

        tonum.clear()  # 第x页 输入框
        tonum.send_keys(str(x))  # 去第x页
        jumpbtn.click()  # 点击按钮

        # 抓取
        WebDriverWait(driver, 10).until(lambda driver: driver.find_element_by_id("pagebar").find_element_by_xpath(
            "div[@class='pagebtns']/label[@value={0} and @class='cur']".format(x)) is not None)
        print(driver.find_element_by_id("jztable").get_attribute("innerHTML").encode('utf-8'))

        # 保存到项目中
        with open("data/fund/110011/html/{0}.txt".format(x), 'wb') as f:
            f.write(driver.find_element_by_id("jztable").get_attribute("innerHTML").encode('utf-8'))
            f.close()

        # 解锁
        lock.release()


# 开始抓取函数
def beginSpider():
    # 初始化爬虫
    (driver, total_page) = initSpider()
    # 创建锁
    lock = Lock()

    r = range(1, int(total_page) + 1)
    step = 10
    range_list = [r[x:x + step] for x in range(0, len(r), step)]  # 把页码分段
    thread_list = []
    for r in range_list:
        t = Thread(target=getData, args=(r, driver, lock))
        thread_list.append(t)
        t.start()
    for t in thread_list:
        t.join()  # 这一步是需要的,等待线程全部执行完成

    print("抓取完成")


# #################上面代码就完成了 抓取远程网站html内容并保存到项目中的 过程


# 将txt汇总存储成csv文档
def getFundDataByTxt():
    result = pd.DataFrame(columns=['净值日期', '单位净值', '累计净值', '日增长率', '申购状态', '赎回状态', '分红送配'])
    for txt in os.listdir('data/fund/110011/html'):
        with open(os.path.join('data/fund/110011/html', txt), encoding='utf8') as file:
            soup = BeautifulSoup(file, "html.parser")
        fund_codes = soup.find_all("td")

        if len(fund_codes) == 0:
            print(txt)
        else:
            list0 = []
            list1 = []
            list2 = []
            list3 = []
            list4 = []
            list5 = []
            list6 = []
            for i, code in enumerate(fund_codes):
                x = code.get_text().strip()
                if i % 7 == 0:
                    list0.append(x)
                elif i % 7 == 1:
                    list1.append(x)
                elif i % 7 == 2:
                    list2.append(x)
                elif i % 7 == 3:
                    list3.append(x)
                elif i % 7 == 4:
                    list4.append(x)
                elif i % 7 == 5:
                    list5.append(x)
                elif i % 7 == 6:
                    list6.append(x)

            result_sub = pd.DataFrame({
                '净值日期': list0,
                '单位净值': list1,
                '累计净值': list2,
                '日增长率': list3,
                '申购状态': list4,
                '赎回状态': list5,
                '分红送配': list6
                }, columns=['净值日期', '单位净值', '累计净值', '日增长率', '申购状态', '赎回状态', '分红送配'])
            result = result.append(result_sub)
    result.sort_values(by='净值日期', ascending=False, inplace=True)
    result.to_csv('data/fund/110011/data.csv', index=False)


# beginSpider()
getFundDataByTxt()
