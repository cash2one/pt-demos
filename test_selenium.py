#! /usr/bin/env python
#coding:utf8

"""
��ʾ selenium ��ʹ��
1. pip install selenium
2. ������ʾ����װ��ͬ������� webdriver
"""


from selenium import webdriver

def main():
    
    driver = webdriver.Chrome()
    driver.get("http://item.taobao.com/item.htm?id=42666101172")
    
    # ���� ������Ϣ���첽��������
    element = driver.find_element_by_class_name('tb-location')
    print element.text
    

if __name__ == "__main__":
    main()
