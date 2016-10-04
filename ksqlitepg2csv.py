#!/usr/bin/env python
#coding:utf-8



"""module for writing data from 'res' structure(which is formed by analyze_pg() from ksqlitepg module) to csv file

exports process_pg(),process_row() functions"""





def performStrForCsv1(s):
    r=[]
    for i in s:
	if i in {',',';','\n','\r'}: r.append('"'+i+'"')
	elif  i=='"': r.append('""')
	else: r.append(i)
    return ''.join(r)

def performStrForCsv2(s):
    r=[]
    for i in s:
	if   i=='"': r.append('""') 
	else: r.append(i)
    return ''.join(r)

def encode_str(s,enc,errCh='?'):#for process_row() function
    if enc=='utf8':
	return s.encode(enc)
    else:
	r=''
	for i in s:
	    try:
		ch=i.encode(enc)
	    except:
		ch=errCh
	    r+=ch
	return r
def process_row(r,out,enc='utf8',errCh='?',csv_delim=','):
    """process_row(r,out) function
    
    writes db table row 'r'(represented in 'res' db row format) to file 'out' open for reading    in csv string format"""
    cnt=0
    for j in r:
        try:
	    if cnt>0: out.write(csv_delim+' ')
	    if j==None:
		out.write('NULL')
	    elif type(j) in {int,long}:
		out.write(str(j))
	    elif type(j)==unicode:
		out.write('"')
		text = encode_str(j,enc,errCh)
		text=performStrForCsv2(text)#!!!!!!!!!!!!!!!!!!!!!!!!!!text.replace('"','""')
		out.write(text)
		out.write('"')
	    elif type(j)==str:
		text=str(  [j]  )
		text=performStrForCsv1(text)
		out.write(  text  )
	    elif type(j)==float:
		out.write(  "%f"%j  )
	    else: out.write(  "[UNKN]%s" % str(type(j))  )
	except Exception as n: out.write("[ERR]%s" % n)
	#
	cnt+=1



def process_pg(r,out,enc='utf8',errCh='?',csv_delim=','):
    """process_pg(r,out) function
    
    writies data from 'res' structure r(which is formed by analyze_pg() from ksqlitepg module) to .csv file out open for reading 
    'res' structure holds all the rows of db page """
    print r.s
    if r.status==1:
	print "  pgType==%s" % r.pgType
	print "  CPAstart==%s" % r.CPAstart
	print "  cellsNum==%s" % r.cellsNum
	print "  SCAstart==%s" % r.SCAstart
	print "  cellpointers:",r.cellpointers
	print "  cells:",r.cells

        for i in r.cells:
	    try:
		process_row(i["paylData"]["row"],out,enc,errCh,csv_delim)
		#cnt=0
		#for j in i["paylData"]["row"]:
		#    try:
		#	if cnt>0: out.write(', ')
		#	if j==None:
		#	    out.write('NULL')
		#	elif type(j) in {int,long}:
		#	    out.write(str(j))
		#	elif type(j)==unicode:
		#	    out.write('"')
		#	    text = j.encode('utf8')
		#	    text=performStrForCsv2(text)#!!!!!!!!!!!!!!!!!!!!!!!!!!text.replace('"','""')
		#	    out.write(text)
		#	    out.write('"')
		#	elif type(j)==str:
		#	    text=str(  [j]  )
		#	    text=performStrForCsv1(text)
		#	    out.write(  text  )
		#	else: out.write(  "[UNKN]%s" % str(type(j))  )
		#    except Exception as n: out.write("[ERR]%s" % n)
		#    #
		#    cnt+=1
		try:
		    i["paylData"]["row"]#проверка наличия элемента с индексом row
		    out.write("\r\n")
		except:pass
	    except:pass
	    

if __name__=="__main__":
    try:

	import sys#for argv()
	#from struct import unpack
	import os.path
	import ksqlitepg



	out=open(sys.argv[1]+'.csv',"wb")

	fr=ksqlitepg.fileReader(sys.argv[1])
    
	for ij in fr.openres:
	    print ij

	i=0
	while i < os.path.getsize(sys.argv[1]) // fr.pageSize:  #cikl po stranicam
	    res=ksqlitepg.analyzePg(i,fr.f,fr.pageSize,fr)
	    process_pg(res,out)
	    i+=1
	
	out.close()
    except Exception as n: 
	print "usage: <program> <file.sqlite>"
	print n
