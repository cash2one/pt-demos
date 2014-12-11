#!/usr/bin/env python
#coding:gbk

"""
 Author:  pengtao --<pengtao@baidu.com>
 Purpose: 
     1. 将nsclick根据pid信息隔离出来
 History:
     1. 2013/7/15 
"""


import sys
sys.path.append('./')  # make hce framework happy !

from hceutil import emit
from ubsutils.hcestep import reducer_cat
from ubsutils.parser.recordz import NSAccessLogRecord

rec = None
nbad = 0
#----------------------------------------------------------------------
def mapper(k, v):
    """
    k == None, v is a text line
    """
    global rec, nbad
    try:
        if rec.parseLine(v):
            baiduid = rec.attr("baiduid")
            if baiduid != '-':
                pid = rec.attr("urlfields").get("pid", '0')
                if pid in ["102", "103", "241"]:
                    time = rec.attr('timesz')
                    ip = rec.attr('ip')
                    url = rec.attr('url')
                    refer = rec.attr('referer')
                    emit(baiduid, "\t".join([time, ip, url, refer, pid]))
                else:
                    # print >> sys.stderr, pid
                    pass
                return True
    except ValueError:
        print >> sys.stderr, v
    nbad += 1
    return True
                
    

#----------------------------------------------------------------------
def mapper_setup():
    """"""
    global rec
    rec = NSAccessLogRecord()

#----------------------------------------------------------------------
def mapper_cleanup():
    """"""
    global nbad
    print >> sys.stderr, "nbad=%d" % nbad


def reducer(k, vs):
    """
    """
    for v in vs:
        pid = v.rsplit("\t")[-1]
        suffix = 'Z'
        if pid == '102':
            suffix = 'A'
        elif pid == '103':
            suffix = 'B'
        elif pid == '241':
            suffix = 'C'
        else:
            pass
        emit(k, v+"#"+suffix)
    
# reducer = reducer_cat
