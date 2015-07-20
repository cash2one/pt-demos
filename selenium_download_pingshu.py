#! /usr/bin/env python
#coding:utf8

"""
演示 selenium 的使用
1. pip install selenium
2. 根据提示，安装不同浏览器的 webdriver
3. 下载52ps网上的“白眉大侠” 320集。
4. iframe需要特殊处理： http://assertselenium.com/2013/02/22/handling-iframes-using-webdriver/
5.  本来最佳方式是获取所有的320个url，利用xunlei下载，但是52ps有防抓取措施，url短时间内会失效。智能找一个下一个。
"""

import sys
from time import sleep, time, ctime
from urllib2 import urlopen
from argparse import ArgumentParser


from selenium import webdriver




def parse_args():
    parser = ArgumentParser(description='download baimeidaxia from 5tps.com. python xxx.py 60 70 means [61, 62, ..., 70]')
    parser.add_argument("start", metavar='START', type=int, help="the start episode. no-inclusive. ")
    parser.add_argument("end", metavar='END', type=int, help="the end episode(inclusive).")
    
    args = parser.parse_args()
    return args
        
def main(args):
    
    driver = webdriver.Chrome()

    for i in range(args.start, args.end):
        idx = i + 1
        s_time = time()
        
        out_file = '5tps.com_单田芳_白眉大侠_%03d.mp3' % idx
        print "%s -- downloading %s..." % (ctime(s_time), out_file)
        sys.stdout.flush()
        
        
        # 'http://www.5tps.com/down/10678_47_1_1.html'
        # 'http://www.5tps.com/down/10678_47_1_320.html'
        url = 'http://www.5tps.com/down/10678_47_1_%d.html'
        driver.get(url % idx)
        # 包含下载信息的区块, iframe本质就是独立的页面，所以不能直接find_element_by_xx
        # <iframe id="play" name="play" ...
        sleep(1)
        driver.switch_to.frame('play')
        elements = driver.find_elements_by_tag_name('a')
        # <a href="http://dxpse-d.ysx8.net:8000/单田芳/单田芳_白眉大侠320清晰/5tps.com_单田芳_白眉大侠_001.mp3?60023422909419x1420949202x60023885522173-61784622818271489815261?3" id="p3163321532"><font color="blue">点此下载《白眉大侠(全320回)高清版》第1回</font></a>
        ele = elements[0]
        href = ele.get_attribute('href')
        fh = open(out_file.decode('utf8').encode('gbk'), 'wb')
        fh.write(urlopen(href).read())
        fh.close()

        e_time = time()
        delta = e_time - s_time
        print "%s -- download with %.1f seconds for %s" % (ctime(e_time), delta, href)
        sys.stdout.flush()
        
        

if __name__ == "__main__":
    args = parse_args()
    main(args)
