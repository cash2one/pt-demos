#! /usr/bin/env python
#coding:utf8

"""
演示 selenium 的使用
1. pip install selenium
2. 根据提示，安装不同浏览器的 webdriver
"""


from selenium import webdriver

def main():
    
    driver = webdriver.Chrome()
    driver.get("http://item.taobao.com/item.htm?id=42666101172")
    
    # 包含 物流信息的异步加载区块
    element = driver.find_element_by_class_name('tb-location')
    print element.text
    

if __name__ == "__main__":
    main()
