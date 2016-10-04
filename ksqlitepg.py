#!/usr/bin/env python
#coding:utf-8


"""module for analysing sqlite database file pages

exports analyze_pg() function, fileReader class"""


import sys#for argv()
from struct import unpack
import os.path



class fileReader:
    def __init__(self,pth):
	#1   =>f,pageSize,U
	f=open(pth,'rb')
	
	self.openres=[]

	f.seek(16)
	self.openres.append( "page size: %s" % unpack(">H",f.read(2))[0] )     # > -- big endian, H -- unsigned short
	f.seek(16)
	self.pageSize=int(unpack(">H",f.read(2))[0])
	if self.pageSize==1: self.pageSize=65536		#--fix:overfl

	f.seek(20)
	self.pgReserved=int(unpack(">B",f.read(1))[0])#Bytes of unused "reserved" space at the end of each page. Usually 0.

	f.seek(32)
	self.openres.append( "first freelist trunk page Num: %s" % unpack(">I",f.read(4))[0] )     # > -- big endian, I -- unsigned int
	f.seek(32)
	self.trunkPgN=int(unpack(">I",f.read(4))[0])


	f.seek(36)
	self.openres.append( "Total number of freelist pages: %s" % unpack(">I",f.read(4))[0] )     # > -- big endian, I -- unsigned int

	f.seek(56)
	self.openres.append( "The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.: %s" %unpack(">I",f.read(4))[0] )     # > -- big endian, I -- unsigned int
	f.seek(56)
	self.enc=int(unpack(">I",f.read(4))[0])
	self.encc={1:'utf-8',2:'utf-16le',3:'utf-16be'}[self.enc]

	f.seek(20)
	unusedSpace=unpack(">B",f.read(1))[0]#unused space in the end of each page

	self.U=self.pageSize-unusedSpace#"usable size of a database page"
	self.f=f
    def __del__(self):
	#3
	try:
	    self.f.close()
	except:pass

#считывание беззнакового числа
def getI(f,siz,pos="n"):#прочитать целое число указанного размера(1,2,4) в байтах из файла(если указана позиция, то с переходом на поз.,иначе с тек.позиции)
    if pos!="n":
	f.seek(pos)
    if   siz==1:
	return int(unpack(">B",f.read(1))[0])
    elif siz==2:
	return int(unpack(">H",f.read(2))[0])
    elif siz==3:
	v=unpack(">BBB",f.read(3))
	return int(  (int(v[0])<<16) | (int(v[1])<<8) | (int(v[2])<<0)  )
    elif siz==4:
	return long(unpack(">I",f.read(4))[0])
    elif siz==6:
	v=unpack(">BBBBBB",f.read(6))
	return long(  (long(v[0])<<40) | (long(v[1])<<32) | (long(v[2])<<24)  |  (long(v[3])<<16) | (long(v[4])<<8) | (long(v[5])<<0) )
    elif siz==8:
	v=unpack(">BBBBBBBB",f.read(8))
	return long(  (long(v[0])<<56) | (long(v[1])<<48) | (long(v[2])<<40) | (long(v[3])<<32)  |  (long(v[4])<<24) | (long(v[5])<<16) | (long(v[6])<<8) | (long(v[7])<<0) )
    return 0

#считывание знакового числа
def getIs(f,siz,pos="n"):#прочитать целое число указанного размера(1,2,4) в байтах из файла(если указана позиция, то с переходом на поз.,иначе с тек.позиции)
    if pos!="n":
	f.seek(pos)
    if   siz==1:
	return int(unpack(">b",f.read(1))[0])
    elif siz==2:
	return int(unpack(">h",f.read(2))[0])
    elif siz==3:
	v=unpack(">BBB",f.read(3))
	v1=int(  (int(v[0])<<16) | (int(v[1])<<8) | (int(v[2])<<0)  )
	if (v[0]&128)==128:#signed
	    v2=(0xffffff-v1)+1
	    return -v2
	else:#unsigned
	    return v1
    elif siz==4:
	return long(unpack(">i",f.read(4))[0])
    elif siz==6:
	v=unpack(">BBBBBB",f.read(6))
	v1=long(  (long(v[0])<<40) | (long(v[1])<<32) | (long(v[2])<<24)  |  (long(v[3])<<16) | (long(v[4])<<8) | (long(v[5])<<0) )
	if (v[0]&128)==128:#signed
	    v2=(0xffffffffffffL-v1)+1
	    return -v2
	else:#unsigned
	    return v1
    elif siz==8:
	v=unpack(">BBBBBBBB",f.read(8))
	v1=long(  (long(v[0])<<56) | (long(v[1])<<48) | (long(v[2])<<40) | (long(v[3])<<32)  |  (long(v[4])<<24) | (long(v[5])<<16) | (long(v[6])<<8) | (long(v[7])<<0) )
	if (v[0]&128)==128:#signed
	    v2=(0xffffffffffffffffL-v1)+1
	    return -v2
	else:#unsigned
	    return v1
    return 0

def getVI(f,pos="n"):#прочитать целое число VARINT из файла(если указана позиция, то с переходом на поз.,иначе с тек.позиции)
    if pos!="n":
	f.seek(pos)
    v=0L
    b=ord(f.read(1))
    v=v | (b & 127)
    while (b & 128)!=0:
	v=v << 7
	b=ord(f.read(1))
	v=v | (b & 127)
    return v

def getS(f,n,pos="n"):#прочитать строку длиной n из файла(если указана позиция, то с переходом на поз.,иначе с тек.позиции)
    try:
	if pos!="n":
	    f.seek(pos)
	return f.read(n)
    except:return "<!--BLOB READ ERROR--!>"
class myUException:pass
def getUS(fr,f,n,pos='n',noUE=False):
    try:
	return unicode(getS(f,n,pos), fr.encc)
    except:
	if noUE:raise myUException
	try:
	    return getS(f,n,pos)
	except:return '<!--STRING READ ERROR--!>'

def getF(f,pos='n'):
    #return "<float:%f>"  %  unpack(">d",f.read(8))
    return unpack(">d",f.read(8)) [0]


#функция для pазбора payload-a
    #SasF -- создает на основе строки объект, совместимый с объектом файлового типа
class SasF:
    def __init__(self,p):
	self.p=p
	self.iii=0
    def seek(self,i):
	self.iii=i
    def read(self,i=None):
	i1=self.iii
	self.iii+=i
	return self.p[i1:(i1+i)]
    def tell(self):
	return self.iii
def parsePayload1(fr,payl,hs,noUE=False,ipkN=-1):
    p=SasF(payl)
    #
    res={}
    res["headerSize"]=hs
    res["fieldTypeNumbers"]=[]  
    n=0  
    while p.tell() < res["headerSize"]:
	if n==ipkN:
	    v=getIs(p,1)
	    res["fieldTypeNumbers"].append(0)
	else:
	    res["fieldTypeNumbers"].append(getVI(p))
	n+=1
    res["row"]=[]
    try:
	for ii in res["fieldTypeNumbers"]:
	    if   ii==0:
		res["row"].append(None)
	    elif ii==1:
		res["row"].append(getIs(p,1))
	    elif ii==2:
		res["row"].append(getIs(p,2))
	    elif ii==3:
		res["row"].append(getIs(p,3))
	    elif ii==4:
		res["row"].append(getIs(p,4))
	    elif ii==5:
		res["row"].append(getIs(p,6))
	    elif ii==6:
		res["row"].append(getIs(p,8))
	    elif ii==7:
		res["row"].append(getF(p))#fixed
	    elif ii==8:
		res["row"].append(0)
	    elif ii==9:
		res["row"].append(1)
	    elif (ii>=12)and((ii%2)==0):
		res["row"].append(getS(p,(ii-12)//2))
	    elif (ii>=13)and((ii%2)==1):
		res["row"].append(getUS(fr,p,(ii-13)//2,'n',noUE))
    except myUException:
	if noUE:raise
    except:pass
    return res

def parsePayload0(fr,payl,noUE=False):
    p=SasF(payl)
    #
    res={}
    res["headerSize"]=getVI(p,0)
    res["fieldTypeNumbers"]=[]    
    while p.tell() < res["headerSize"]:
	res["fieldTypeNumbers"].append(getVI(p))
    res["row"]=[]
    try:
	for ii in res["fieldTypeNumbers"]:
	    if   ii==0:
		res["row"].append(None)
	    elif ii==1:
		res["row"].append(getIs(p,1))
	    elif ii==2:
		res["row"].append(getIs(p,2))
	    elif ii==3:
		res["row"].append(getIs(p,3))
	    elif ii==4:
		res["row"].append(getIs(p,4))
	    elif ii==5:
		res["row"].append(getIs(p,6))
	    elif ii==6:
		res["row"].append(getIs(p,8))
	    elif ii==7:
		res["row"].append(getF(p))#fixed
	    elif ii==8:
		res["row"].append(0)
	    elif ii==9:
		res["row"].append(1)
	    elif (ii>=12)and((ii%2)==0):
		res["row"].append(getS(p,(ii-12)//2))
	    elif (ii>=13)and((ii%2)==1):
		res["row"].append(getUS(fr,p,(ii-13)//2,'n',noUE))
    except myUException:
	if noUE:raise
    except:pass
    return res
def parsePayload(fr,cell,payl):
    cell["paylData"]=parsePayload0(fr,payl)
#
def getCellPayloadSize(pgType,P):#P -- is payload size
    def isIndex(p):
	return p in {2,10}

    global fr#access to global variable
    U=fr.U

    if isIndex(pgType):
	X=((U-12)*64//255)-23
    else:
	X=U-35
    
    M = ((U-12)*32//255)-23
    
    K = M+((P-M)%(U-4))
    
    if   (P>X) and (K<=X):
	return K
    elif (P>X) and (K>X):
	return M
    else:
        return P
#вспомогательные функции для разбора ячеек
def totalBnum(cell,  f):
    cell["totalBnum"]=getVI(f)
def rowid(cell,  f):
    cell["rowid"]=getVI(f)
def payload(cell,  f, plSize):
    cell["payload"]=f.read(plSize)
def owerflow(cell, f,owerfl, pcsz,pgSize):
    def read_owerflow(pg,f,bnum,pd,pgSize):#(!)bnum is not used -- not necessary
	#print ">>>>>>>>>>owerfl_r",pg
	nxt=getI(f,4,pg*pgSize)
	pd+=f.read(pgSize-4)
	if nxt>0:
	    pd+=read_owerflow(nxt-1,f,bnum,"",pgSize)
	return pd
	
    try:#fix:owerflow cannot exist(!)
	cell["owerflow"]=getI(f,4)
    except:pass
    if owerfl==True:
	#print ">>>>>>>>>>o1",pcsz<cell["totalBnum"],pcsz,cell["totalBnum"]
	pass
	if pcsz<cell["totalBnum"]:
	    #print ">>>>>>>>>>o2"
	    nb=cell["totalBnum"]-pcsz#число байт payload'a в owerflow(за пределами cell)
	    try:
		#print len(cell["payload"]), cell["owerflow"]
		cell["payload"]  =  read_owerflow(cell["owerflow"]-1,f,nb,cell["payload"],pgSize)
		#print len(cell["payload"])
	    except:pass
def leftchild(cell, f):
    cell["leftchild"]=getI(f,4)
def intkey(cell,  f):
    cell["key"]=getVI(f)
#функции для разбора ячеек
def parseIIntCell(cp,  pgNum,f,fr,pgSize ,pgType,owerfl):
    cell={}
    f.seek(pgNum*pgSize+cp)
    leftchild(cell, f)
    totalBnum(cell, f)
    cell["cellPaylSz"]=getCellPayloadSize(pgType,cell["totalBnum"])
    payload(cell,f,  cell["cellPaylSz"] )
    owerflow(cell,f,owerfl,cell["cellPaylSz"],pgSize)
    parsePayload(fr,cell,cell["payload"])
    return cell
def parseTIntCell(cp,  pgNum,f,fr,pgSize ,pgType,owerfl):
    cell={}
    f.seek(pgNum*pgSize+cp)
    leftchild(cell, f)
    intkey(cell,  f)
    return cell
def parseILeafCell(cp,  pgNum,f,fr,pgSize ,pgType,owerfl):
    cell={}
    f.seek(pgNum*pgSize+cp)
    totalBnum(cell, f)
    cell["cellPaylSz"]=getCellPayloadSize(pgType,cell["totalBnum"])
    payload(cell,f,  cell["cellPaylSz"] )
    owerflow(cell,f,owerfl,cell["cellPaylSz"],pgSize)
    parsePayload(fr,cell,cell["payload"])
    return cell
def parseTLeafCell(cp,  pgNum,f,fr,pgSize ,pgType,owerfl):
    cell={}
    f.seek(pgNum*pgSize+cp)
    totalBnum(cell, f)
    rowid(cell, f)
    cell["cellPaylSz"]=getCellPayloadSize(pgType,cell["totalBnum"])
    payload(cell,f, cell["cellPaylSz"]  )
    owerflow(cell,f,owerfl,cell["cellPaylSz"],pgSize)
    parsePayload(fr,cell,cell["payload"])
    return cell


def analyzePg(pgNum,f,pgSize,fr1,owerfl=False,getFreeFragm=False):
    class res:
	pass
    r=res()
    global fr
    fr=fr1
    try:
	print "->>[0]"
	#reading page header
	if pgNum==0:
	    PgHstart=100
	else:
	    PgHstart=0
	f.seek(pgNum*pgSize+PgHstart)
	pgType=  getI(f,1)
	if not pgType in{2,5,10,13}:raise NameError("not a page: WRONG PAGE TYPE VAL %i"%pgType)
	if pgType in {2,5}:
	    CPAstart=PgHstart+12
	else:
	    CPAstart=PgHstart+8
	getI(f,2)
	cellsNum=getI(f,2)
	SCAstart=getI(f,2)
	if SCAstart==0: SCAstart=65536	#--fix:overfl
	getI(f,1)
	if pgType in{2,5}:
	    rightmostPointer=getI(f,4)
	else:
	    rightmostPointer=None
	#reading cell pointer array =>cpo
	cpo=[]
	f.seek(pgNum*pgSize+CPAstart)
	for ii in range(cellsNum):
	    cpo.append(getI(f,2))
	#reading cells
	cells=[]
	freeFr=[]
	funcs={2:parseIIntCell,5:parseTIntCell,10:parseILeafCell,13:parseTLeafCell}
	cpo1=cpo[:]
	cpo1.sort()
	for cp in cpo1:
	    #try:
	    cells.append(  funcs[pgType](cp,  pgNum,f,fr1,pgSize ,pgType,  owerfl)  )
	    #except:pass
	#
	#reading free fragments from page(cells area)
	#
	print "->>[1]",getFreeFragm
	try:
	    if getFreeFragm==True:
		#between cells
		try:
		    print "->>[2]"
		    for ii in range(cellsNum-1):
			print "->>",cpo1[ii]+cells[ii]["cellPaylSz"]-(cpo1[ii+1]-1)
		        if cpo1[ii]+cells[ii]["cellPaylSz"]<cpo1[ii+1]-1:
			    f.seek(pgNum*pgSize+cpo1[ii]+cells[ii]["cellPaylSz"])
			    freeFr.append(    [   f.read(  cpo1[ii+1]-(cpo1[ii]+cells[ii]["cellPaylSz"])  )   ]     )
			    print "****************>",pgNum*pgSize+cpo1[ii]+cells[ii]["cellPaylSz"],pgNum*pgSize+cpo1[ii+1],"--between"
		except:print "===============EXCEPTION1"
		#after cells and before reserved space in the end of page
		try:
		    print "->>[3]"
		    if cellsNum>0:
		    	v1=cells[cellsNum-1]["cellPaylSz"]
			v2=pgSize-fr1.pgReserved
			print "->>",cpo1[cellsNum-1]+v1-(v2-1)
		    	if cpo1[cellsNum-1]+v1<v2-1:
		            f.seek(pgNum*pgSize+cpo1[cellsNum-1]+v1)
		            freeFr.append(    [f.read(  v2-cpo1[cellsNum-1] -v1 )]    )
			    print "****************>",pgNum*pgSize+cpo1[cellsNum-1]+v1,pgNum*pgSize+v2,"--after"
		except:print "===============EXCEPTION2"
		#before cells
		print "->>[4]"
		v1=CPAstart+cellsNum*2#start of unallocated area
		if cellsNum>0:#end of unallocated area
		    v2=cpo1[0]
		else:
		    v2=pgSize-fr1.pgReserved
		print "->>",v1-(v2-1)
		if v1<v2-1:
		    #print v1,v2
		    f.seek(pgNum*pgSize+v1)
		    freeFr.append(    [f.read(  v2-v1 )]    )
		    print "****************>",pgNum*pgSize+v1,pgNum*pgSize+v2,"--before"
		    
	except: print "<----------!!!!!!!!!!!!!!!!!!!!!!!!!!!-------->"

    except Exception as n:
	try:
	    r.status=0
	    r.s="<page %s: is not Pg! %s>" % (pgNum,n)
	except:pass
    except:
	r.status=0
	r.s="<page %s: UNKNOWN ERROR!>" % pgNum
    else:
	r.status=1
	r.s="<page %s:>" % pgNum
	r.pgType=pgType
	r.CPAstart=CPAstart
	r.cellsNum=cellsNum
	r.SCAstart=SCAstart
	r.cellpointers=cpo
	r.cells=cells
	r.rightmostPointer=rightmostPointer
	r.freeFr=freeFr
    return r

def process1(r):
    print r.s
    if r.status==1:
	print "  pgType==%s" % r.pgType
	print "  CPAstart==%s" % r.CPAstart
	print "  cellsNum==%s" % r.cellsNum
	print "  SCAstart==%s" % r.SCAstart
	print "  cellpointers:",r.cellpointers
	print "  rightmostPointer:",r.rightmostPointer
	print "  cells:",r.cells
	print "  freeFr:",r.freeFr

#2

if __name__=="__main__":
    try:
	fr=fileReader(sys.argv[1])
    
	for ij in fr.openres:
	    print ij

	i=0
	while i < os.path.getsize(sys.argv[1]) // fr.pageSize :  #cikl po stranicam
	    res=analyzePg(i,fr.f,fr.pageSize,fr,False,True)
	    process1(res)
	    i+=1
    except Exception as n: 
	print "usage: <program> <file.sqlite>"
	print n
