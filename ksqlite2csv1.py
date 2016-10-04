#!/usr/bin/env python
#coding:utf-8


#utility


import sys#for argv()
#from struct import unpack
import os.path


import ksqlitepg
import ksqlitepg2csv



try:

    out=open(sys.argv[1]+'.csv',"wb")

    fr=ksqlitepg.fileReader(sys.argv[1])
    
    for ij in fr.openres:
        print ij

    i=0
    while i < os.path.getsize(sys.argv[1]) // fr.pageSize:  #cikl po stranicam
	res=ksqlitepg.analyzePg(i,fr.f,fr.pageSize,fr)
	ksqlitepg2csv.process_pg(res,out)
	i+=1
	
    out.close()
except Exception as n: 
    print "usage: <program> <file.sqlite>"
    print n
