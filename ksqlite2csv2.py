#!/usr/bin/env python
#coding:utf-8


#utility


import sys#for argv()
#from struct import unpack
import os.path


import ksqlitepg
import ksqlitepg2csv

import os#for mkdir




def process_table1(fr,rootpageN,callback,fragCallb=None):
    print "***>analyzing page rootpageN=",rootpageN
    #global mas
    #mas[rootpageN]=1
    if fragCallb:
	res=ksqlitepg.analyzePg(rootpageN,fr.f,fr.pageSize,fr,True,True)
    else:
	res=ksqlitepg.analyzePg(rootpageN,fr.f,fr.pageSize,fr,True)
    if   res.pgType==5: #Table Interior cell
	pass
	if res.status==1:  
	    for i in res.cells:
		#print ">>1"
		try:
        	    process_table1( fr, i["leftchild"]-1, callback,fragCallb )    #for j in i["paylData"]["row"]:
		except:pass
	    try:
	        process_table1( fr, res.rightmostPointer-1, callback,fragCallb ) 
	    except:pass
	    if fragCallb:#############################################################################################################
		for ij in res.freeFr:
		    try:
			fragCallb(ij)
		    except:pass
        else:print "err1"
    elif res.pgType==13:#Table Leaf cell
	if res.status==1:  
	    for i in res.cells:
		#print ">>2"
		try:
		    rowid=i["rowid"]
		except:rowid=None
		try:
        	    callback(i["paylData"]["row"],rowid)#for j in i["paylData"]["row"]:
		except:pass
	    if fragCallb:
		for ij in res.freeFr:
		    try:
			fragCallb(ij)
		    except:pass
        else:print "err2"

def process_index1(fr,rootpageN,callback,fragCallb=None): ###!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! check -- not tested(!)
    print "***>analyzing page rootpageN=",rootpageN
    #global mas
    #mas[rootpageN]=1
    if fragCallb:
	res=ksqlitepg.analyzePg(rootpageN,fr.f,fr.pageSize,fr,True,True)
    else:
	res=ksqlitepg.analyzePg(rootpageN,fr.f,fr.pageSize,fr,True)
    if   res.pgType==2: #Index Interior cell
	pass
	if res.status==1:  
	    for i in res.cells:
		#print ">>1"
		try:
		    callback(i["paylData"]["row"])#fix: index interior cells store data too(!)
        	    process_index1( fr, i["leftchild"]-1, callback,fragCallb )    #for j in i["paylData"]["row"]:
		except:pass
	    try:
	        process_index1( fr, res.rightmostPointer-1, callback,fragCallb ) 
	    except:pass
	    if fragCallb:#############################################################################################################
		for ij in res.freeFr:
		    try:
			fragCallb(ij)
		    except:pass
        else:print "err1"
    elif res.pgType==10:#Index Leaf cell
	if res.status==1:  
	    for i in res.cells:
		#print ">>2"
		try:
        	    callback(i["paylData"]["row"])#for j in i["paylData"]["row"]:
		except:pass
	    if fragCallb:
		for ij in res.freeFr:
		    try:
			fragCallb(ij)
		    except:pass
        else:print "err2"



def callback1(row,rowid=None):
    print row
#-----------------------------------------------------------------------callback'и для 1го прохода(чтение таблиц, индексов, удал.фрагментов)
ff=None
ffs=None#freeFramgents
#ffrd=None#rows data from freeFramgents(rows data)
ffr=None#rows data from freeFramgents(rows)
rowD0=None#описание типов полей записей текущей таблицы
rowD1=None#описание типов полей записей  таблицы для извлечения удаленных записей
fragm_table=None#имя таблицы для чтения удаленных записей

def callback2(row,rowid=None):#callback for processing row of 'sqlite_master' table
    if   row[0]=='table':
	global fr#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	global ff,ffs,ffr
	global rowD1,rowD0
	global fragm_table
	ff =open("./out/table-"+row[1]+".csv","wb")
	ffs=open("./out/tableFragm-"+row[1]+".csv","wb")

	try:
	    try:
		rowD0=parseTableDef(row[4])
		print "@@@@@@@@@@@",rowD0
		ff.write('; '.join([i[2] for i in rowD0[0]]) + '\r\n')
	    except:pass
	    if row[1]==fragm_table:
		try:
		    rowD1=rowD0		    
		    print '@@@@@@@@@@@@@@@@@@@@@',rowD1		    
		except:pass
		#ffr=open("./out/tableFrRows-"+row[1]+".csv","wb")
		process_table1(fr,row[3]-1,callback3,callback3FragTnoRec)
		#ffr.close()
	    else:
		process_table1(fr,row[3]-1,callback3,callback3FragTnoRec)
	except:pass

	ff.close()
	ffs.close()
    elif row[0]=='index':
	global fr#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	global ff,ffs#,ffr
	ff= open("./out/index-"+row[1]+".csv","wb")
	ffs=open("./out/indexFragm-"+row[1]+".csv","wb")
	#ffr=open("./out/IndexFrRows-"+row[1]+".csv","wb")
	try:
	    process_index1(fr,row[3]-1,callback3I,callback3FragI)
	except:pass
	ff.close()
	ffs.close()
	#ffr.close()

def callback2_1tab(row,rowid=None):#callback for processing row of 'sqlite_master' table
    """
    аналогично callback2() (первый проход), но происходит только чтение существующих записей указанной таблицы
    """
    if   row[0]=='table':
	global fr
	global ff,ffs,ffr
	global rowD1,rowD0
	global fragm_table
	try:
	    try:
		rowD0=parseTableDef(row[4])
		print "@@@@@@@@@@@",rowD0
	    except:pass
	    if row[1]==fragm_table:
		ff=open("./out/table-"+row[1]+".csv","wb")
		#for i in rowD0[0]:
		#    ff.write( i[2] )#####print '******',[i[2] for i in rowD0[0]] #####
		try:
		    rowD1=rowD0
		    print '@@@@@@@@@@@@@@@@@@@@@',rowD1
		    ff.write('; '.join([i[2] for i in rowD0[0]]) + '\r\n')
		except:pass
		process_table1(fr,row[3]-1,callback3)
		ff.close()
	except:pass	


def callback3(row,rowid=None):#callback for processing row of a table(during reading 'sqlite_master' table )
    global ff
    global rowD0
    global out_str_enc,out_errCh,csv_delim
    try:#fix:changing table's integer primary key column value from Payload to Rowid
	if rowD0[1][0]==True:#table has integer primary key column => changing TLeaf Page value(corresponding column) to Rowid value
	    row1=row[:]#creating copy of 'row' variable
	    row1[  rowD0[1][1]  ]   =   rowid 
	else:
	    row1=row
    except: row1=row
    ksqlitepg2csv.process_row(row1,ff,out_str_enc,out_errCh,csv_delim)#ff.write(str(row)+'\n')
    ff.write('\r\n')
def callback3I(row,rowid=None):#callback for processing row of an index (during reading 'sqlite_master' table )
    global ff
    global out_str_enc,out_errCh,csv_delim
    ksqlitepg2csv.process_row(row,ff,out_str_enc,out_errCh,csv_delim)#ff.write(str(row)+'\n')
    ff.write('\r\n')
#def callback3FragT(row):#callback for processing freeFragment of a table(during reading 'sqlite_master' table )
#    global ffs,ffr
#    global rowD1
#    #ksqlitepg2csv.process_row(row,ff)#ff.write(str(row)+'\n')
#    ffs.write(str(row))
#    ffs.write('\r\n')
#    try:
#	for ii in getRowsFromFragm(row[0],rowD1):
#	    ffr.write(str(ii)+'\r\n')
#    except:pass
def callback3FragTnoRec(row):#callback for processing freeFragment of a table(during reading 'sqlite_master' table )
    global ffs,ffr
    global rowD1
    ffs.write(str(row))
    ffs.write('\r\n')


def callback3FragI(row):#callback for processing freeFragment of an index(during reading 'sqlite_master' table )
    global ffs
    ffs.write(str(row))
    ffs.write('\r\n')
#-----------------------------------------------------------------------callback'и для 2го прохода(чтение удаленных записей)
def callback2R(row,rowid=None):#callback for processing row of 'sqlite_master' table
    if   row[0]=='table':
	global fr#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	global ff,ffs,ffr
	global rowD1
	global fragm_table,own_table
	try:
	    if (not own_table)or(row[1]==fragm_table):
		process_table1(fr,row[3]-1,callback3R,callback3fR)
	except:pass
    elif row[0]=='index':
	global fr#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	global ff,ffs,ffr
	global rowD1
	global fragm_table,own_table
	try:
	    if (not own_table):
		process_index1(fr,row[3]-1,callback3R,callback3fR)
	except:pass



def callback3R(row,rowid=None):#callback for processing row of a table(during reading 'sqlite_master' table )
    pass
def callback3fR(row):#callback for processing freeFragment of a table/index(during reading 'sqlite_master' table )
    global ffs,ffr#,ffrd
    global rowD1
    global filter_none
    global out_str_enc,out_errCh,csv_delim
    #
    try:
	if rowD1[1][0]:
	    v=rowD1[1][1]
	else:
	    v=-1
	for ii in getRowsFromFragm(row[0],rowD1[0],v):
	    if (not filter_none) or not are_nones(ii["row"]):
		#ffrd.write(str(ii)+'\r\n')
		ksqlitepg2csv.process_row(ii['row'],ffr,out_str_enc,out_errCh,csv_delim)
		ffr.write('\r\n')
    except:pass

def are_nones(l):#returns True if all list's members are None
    return filter(lambda x: x!=None,l)==[]
"""
def default0():
    out=open(sys.argv[1]+'.csv',"wb")

    fr=ksqlitepg.fileReader(sys.argv[1])
    
    for ij in fr.openres:
        print ij

    i=0
    while (i+1)*fr.pageSize +1 < os.path.getsize(sys.argv[1]) :  #cikl po stranicam
	res=ksqlitepg.analyzePg(i,fr.f,fr.pageSize,fr)
	ksqlitepg2csv.process_pg(res,out)
	i+=1
	
    out.close()
"""

def freelist_trunk(trunkPgN,fr,fragCallb):
    def readPage(fr,pgNum):
	fr.f.seek(pgNum*fr.pgSize)
	return fr.f.read(fr.pgSize) 
    nxt=ksqlitepg.getI(fr.f,4,(trunkPgN-1)*fr.pageSize)
    num=ksqlitepg.getI(fr.f,4)
    for i in range(num):
	pgn=ksqlite.getI(fr.f,4)
	res=ksqlitepg.analyzePg(pgn-1,fr.f,fr.pageSize,fr,True)
	#ksqlitepg2csv.process_pg(res,out)
	if res.status==1:
	    if res.pgType in {5,13,2,10}:#this is b-tree page
		fragCallb(readPage(fr,pgn-1))
    if nxt>0: freelist_trunk(nxt,fr,fragCallb)
#-------------------------------------------------------------------------
def parseTableDef(sqlS):
    """получает на вход sql-строку с запросом сreate table. возвращает [  [[FIELDTYPE1,size1],[FIELDTYPE2,size2]...[FIELDTYPEn,sizeN]], pkD]  
    
    где FIELDTYPE::=[token1 token2 .. tokenN]"""
    res=[]
    pkD=[False]#primary key data: [False] or [True,pkNum]
    inum=0
    ss=sqlS[sqlS.find("(")+1:] . split(",")
    for i in ss:
	inum+=1
	FIRST_CNSTRT_TOKEN=''
	#print "^^1",i
	tp=[[],None,None]
	res.append(tp)
	s=i.lstrip()
	#
	n=s.find(" ")+1#начало названия типа
	n1=n
	#print "^^2",n
	#записываем название поля(1й токен описания типа)
	#if tp[2]==None:
	try:
	    tp[2]=s.split(" ")[0]
	except:pass
	while (n1<len(s)) and (FIRST_CNSTRT_TOKEN=='') and not (s[n1] in {'(',')',  ' ',chr(9),chr(10),chr(13)} ):#цикл по словам из которых состоит тип
	    s1=""
	    while (n1<len(s)) and not (s[n1] in {'(',')',  ' ',chr(9),chr(10),chr(13)}):#чтение слова
		s1+=s[n1]
		n1+=1
	    #записываем токен в список(если это не начало названия constraint'a)
	    if s1.upper() in {'PRIMARY','NOT','UNIQUE','CHECK','DEFAULT','COLLATE',   'ON'}:
		FIRST_CNSTRT_TOKEN=s1.upper()
	    else:
		tp[0].append(s1.upper())
	    #print "^^3"
	    while (n1<len(s)) and (s[n1] in {' ',chr(9),chr(10),chr(13)}):
		n1+=1
	#print "^^4"
	if (n1<len(s)) and (s[n1]=='('):#размер в скобках
	    sz=""
	    n1+=1
	    while not(s[n1] in {',',')'}):
		sz+=s[n1]
		n1+=1
	    try:
		szi=long(sz)
	    except:szi=0
	    tp[1]=szi
	#5(determining if it's an integer primary key column)
	try:
	    #print "@@@@@"
	    if n1<len(s):
		if s[n1]==')':
		    n1+=1
		#с учетом 1го токена
		ts=(s[n1:].strip().split(" "))
		if FIRST_CNSTRT_TOKEN!='':
		    ts=[FIRST_CNSTRT_TOKEN]+ts
		#print "@@@@@",ts
		if (tp[0][0].upper()=="INTEGER") and (ts[0].upper()=='PRIMARY') and (ts[1].upper()=='KEY'):
		    pkD[0]=True
		    pkD.append(inum-1)
	except:pass
	else:
	    pass
    return [res,pkD]
    
#----------------
def find_substr(type_lst,s):
    b=False
    for i in type_lst:
	b=b or (i.find(s)!=-1)
    return b
def checkForRow(rowD,f,check_hs,ipkN=-1):
    """определяет, находится ли заголовок строки бд-таблицы по указанному номеру байта в файле и списку полей таблицы(в формате parseTableDef())

    f -- файл(овый объект)
    rowD -- описание полей таблицы бд(в формате parseTableDef() )
    возвращает значение"""

    global filter_sb
    hsSz=0
    try:
	#print '+++checkForRow-1'
	#####fN=ksqlitepg.getVI(f,0)
	#####hsSz=f.tell()
	#print '+++checkForRow-2'
	n=-1
	for i in range(len(rowD)):
	    n+=1
	    try:
		if n==ipkN:
		    if rowD[i][0][0]=='INTEGER':
			ksqlitepg.getIs(f,1)
			#print "--->ok"
			continue
		fC=ksqlitepg.getVI(f)
		c=rowD[i][0]#младший токен типа
#		if c=='INTEGER':
#		    if not (fC in {0,1,2,3,4,5,6,8,9}):
#			return -1
#		elif (c=='CHAR') or (c=='VARCHAR') or (c=='TEXT'):
#		    if not ( (fC==0) or ((fC>=13) and (fC%2==1)) or ((fC>=12) and (fC%2==0)) and not filter_sb ):
#			return -1
#		elif (c=='FLOAT') or (c=='REAL'):#fix me!!
#		    if not (fC in {1,2,3,4,5,6,7,8,9}):
#			return -1
#		elif c=='NULL':
#		    if fC != 0:
#			return -1
#		elif c=='BLOB':
#		    if not ( (fC==0) or (fC>=12 and fC%2==0) ):
#			return -1
		"""   """
		if find_substr(c,'INT'):
		    if not (fC in {0,1,2,3,4,5,6,8,9}):
			return -1
		elif find_substr(c,'CHAR') or find_substr(c,'TEXT') or find_substr(c,'CLOB'):
		    if not ( (fC==0) or ((fC>=13) and (fC%2==1)) or ((fC>=12) and (fC%2==0)) and not filter_sb ):
			return -1
		elif find_substr(c,'REAL') or find_substr(c,'FLOA') or find_substr(c,'DOUB'):#fix me!!
		    if not (fC in {1,2,3,4,5,6,7,8,9}):
			return -1
		elif c[0]=='NULL':
		    if fC != 0:
			return -1
		elif find_substr(c,'BLOB'):
		    if not ( (fC==0) or (fC>=12 and fC%2==0) ):
			return -1
	    except:pass
	#print '+++checkForRow-3-'
	if check_hs:#проверять по байту размера заголовка(тест-не работает)
	    if 0!=f.tell():
		#print '+++checkForRow-#0',fN,len(rowD)
		return -1
	#print '+++checkForRow-4-'
    except Exception as e:
	#print 'eeeeeeeeeeeeeeeeee==',e
	return -1
    #print '+++checkForRow--------------------OK',f.tell()
    return (f.tell(),0)

def int2vint(i):
    v=i
    r=""
    while v!=0:
	b=v&127
	v=v >> 7
	if r!="":
	    b=b | 128
	r=chr(b)+r
    return r
def getRowsFromFragm(frag,rowD,ipkN=-1):
    res=[]
    global check_hs,filter_ue,fr
    for i in range(len(frag)):
	try:
	    f=ksqlitepg.SasF(frag[i:])
	    r=checkForRow(rowD,f,check_hs,ipkN)
	    if r!=-1:
		#print "++++++++++++++0"
		try:
		    #vint=int2vint(r[0])
		    res.append(ksqlitepg.parsePayload1( fr, frag[(i+r[1]):], r[0]-r[1], filter_ue,ipkN ))
		except:pass
	except:pass
    return res

#-------------------------------
def print_help():
    print "usage: <program> <file.sqlite> [ --fragm-table=<TABLENAME_TO_GET_DELETED_RECORDS>  [ --filter-none ] [ --filter-ue ]  [ --filter-sb ]  ]   [--out-str-enc=<output string values encoding>]  [--csv-delim=<csv delimiting character>]"
try:
    try:
	os.mkdir('./out')
    except:pass
    try:
        for i in os.walk("./out/"):
	    for j in i[2]:
		print j
		os.remove("./out/"+j)
    except:pass

    own_table=False
    check_hs=False
    filter_none=False
    filter_ue=False
    filter_sb=False
    out_str_enc='utf8'
    out_errCh='?'
    csv_delim=','
    for i in sys.argv:
	a=i.split("=")
	if   a[0]=='--fragm-table':
	    fragm_table=a[1]
	elif a[0]=='--help':
	    print_help()
	elif a[0]=='--own-table':#искать значения только в страницах своей таблицы
	    own_table=True
	elif a[0]=='--check-hs':#проверять по байту размера заголовка
	    check_hs=True
	elif a[0]=='--filter-none':#фильтровать записи где все значения None
	    filter_none=True
	elif a[0]=='--filter-ue':#фильтровать записи где ошибки при чтении unicode-строк
	    filter_ue=True
	elif a[0]=='--filter-sb':#фильтровать записи не соотв-е str-str, blob-blob
	    filter_sb=True
	elif a[0]=='--out-str-enc':#кодировка для строк на выходе(по умолчанию utf8).  Для кодировки windows(кирилица): --out-str-enc=cp1251
	    out_str_enc=a[1]
	elif a[0]=='--out-errCh':#символ, который выводится при ошибке перекодировки символа строки в 1-байтовую кодировку
	    out_errCh=a[1]
	elif a[0]=='--csv-delim':#символ разделителя в csv-файле (запятая по умолчанию)
	    csv_delim=a[1]

    fr=ksqlitepg.fileReader(sys.argv[1])

    #pgNum=os.path.getsize(sys.argv[1]) // fr.pageSize
    #mas=[0]*pgNum
    
    #1 pass
    if fragm_table:
	process_table1(fr,0,callback2_1tab)#processing 'sqlite_master' table
    else:
        process_table1(fr,0,callback2)#processing 'sqlite_master' table
    
    #2 pass
    if fragm_table:
	print "processing free fragments.."
	try:
	    #ffrd=open("./out/tableFrRows-"+fragm_table+".dat","wb")
	    ffr =open("./out/tableFrRows-"+fragm_table+".csv","wb")
	    try:
		ffr.write('; '.join([i[2] for i in rowD1[0]]) + '\r\n')
	    except:pass
	    process_table1(fr,0,callback2R)#поиск удаленных записей в обычных таблицах
	    #print "[Processing master table..]"
	    #process_table1(fr,0,callback3R,callback3fR)#поиск удаленных записей в самой master table
	    ffr.close()
	    #ffrd.close()
	except:pass
	if fr.trunkPgN>0:
	    print "processing freelist trunk.."
	    try:
	        #ffrd=open("./out/tableFrRows1-"+fragm_table+".dat","wb")
	        ffr =open("./out/tableFrRows1-"+fragm_table+".csv","wb")
		try:
		    ffr.write('; '.join([i[2] for i in rowD1[0]]) + '\r\n')
		except:pass
	        freelist_trunk(fr.trunkPgN,fr,callback3fR)#processing freelist trunk
	        ffr.close()
	        #ffrd.close()
	    except:pass
    
    #for ii in mas:
    #    if ii==0:
    #        print 'not passed>--',ii

except Exception as n: 
    print_help()
    print n
