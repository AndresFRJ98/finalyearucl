#Indexing process.

# def main():
from contextlib import ExitStack
from itertools import zip_longest
import heapq
import os
import codecs
from operator import itemgetter
import ast
import bz2
import time
from bs4 import BeautifulSoup as B
import sqlite3
import traceback
import sys



class Indexer:
    def __init__(self):
        self.path = 'a'
        self.countertoken = {}
        self.sumlength = 0
        self.doc_count = 1
        self.block_count = 0
        self.done = False
        self.doc_length = {}
        self.collection_size = 0
        self.dpath = 'smallextract'
        self.flist = []
    
    def create_sqlite(self):

        try:
            sqliteConnection = sqlite3.connect('wikipedia.db')
            sqlite_create_table_query = '''CREATE TABLE Wikipedia (
                                        id INTEGER PRIMARY KEY,
                                        title TEXT NOT NULL,
                                        content TEXT NOT NULL);'''

            cursor = sqliteConnection.cursor()
            print("Successfully Connected to SQLite")
            cursor.execute(sqlite_create_table_query)
            sqliteConnection.commit()
            print("SQLite table created")

            cursor.close()

        except sqlite3.Error as error:
            print("Error while creating a sqlite table", error)
        finally:
            if (sqliteConnection):
                sqliteConnection.close()
                print("sqlite connection is closed")
        
    def insert_db(self, id, title, content):
        try:
            sqliteConnection = sqlite3.connect('wikipedia.db')
            cursor = sqliteConnection.cursor()
            # print("Successfully Connected to SQLite")

            data_tuple = (id, title, content)
            sqlite_insert_query = """INSERT INTO Wikipedia
                                (id, title, content)  VALUES  (?,?,?)"""

            cursor.execute(sqlite_insert_query, data_tuple)
            sqliteConnection.commit()
            # print("Record inserted successfully into Wikipedia table ")
            cursor.close()

        except sqlite3.Error as error:
            print("Failed to insert data into sqlite table")
        finally:
            # if (sqliteConnection):
            sqliteConnection.close()
            # print("The SQLite connection is closed")

    def set_path(self, path):
        self.dpath = path
    
    def set_size(self, n):
        self.collection_size = n
        return

    def counter(self, tokens,id):
        #Token,Document pair 
        
        #Record document length

        if id not in self.doc_length.keys():
            self.doc_length[id] = len(tokens)
        else:
            self.doc_length[id] += len(tokens)
        
        self.sumlength += len(tokens)

        for token in tokens:

            if token not in self.countertoken.keys():
                #New token
                # self.countertoken[token] = ([1], [id])
                self.countertoken[token] = {}
                self.countertoken[token][id] = 1
            elif id not in self.countertoken[token].keys():
                #New doc for this token
                self.countertoken[token][id] = 1  
            else:
                self.countertoken[token][id] += 1  
            
    def index(self):
        #Sort TermID - DocID pairs
        #Collect those with same ID into posting list (simply termIDs)
        #Write to disk
        # index = {}

        # for key in self.countertoken:

        #     index[key] = self.countertoken[key]
        
        ordered = sorted(self.countertoken)
        #Store alphabetically. 
        # with open("idxfile{}.txt".format(self.block_count),"w") as f:
        if not os.path.isdir('temp'):
            os.mkdir('temp')

        f = codecs.open("temp/idxfile{}.txt".format(self.block_count),'w',encoding='utf-8')
        for key in ordered: 
            f.write('{}'.format(key))
            for docid in self.countertoken[key].keys():
                f.write(':')
                f.write('{};{}'.format(docid, self.countertoken[key][docid]))
    
            f.write('\n')
        
        
        self.countertoken = {}

    def  write_lengths(self):
        #Write length of current document

        f = open('lengths.txt','a')
        for key in self.doc_length:
            f.write('{} {}\n'.format(key, self.doc_length[key]))
        self.doc_length = {}
        return
    
    def write_avg(self):
        f = open('avg.txt','w')
        f.write('{}\n'.format(self.collection_size))
        f.write(str(self.sumlength / self.collection_size))
        f.close()
        return 
    
    def navigate(self):
        # os.chdir(self.dpath)

        #Build file list
        self.flist = [f for f in os.listdir(self.dpath)]

    def parseblock(self):
        
        print('PARSING BLOCK: {} '.format(self.block_count))
        path = self.flist[self.block_count]
        dumps = [d for d in os.listdir('{}/{}'.format(self.dpath,path))]
        start = time.time()
        dn = len(dumps)
        di = 0
        for dump in dumps:

            sys.stdout.write('\r')
            # # the exact output you're looking for:
            sys.stdout.write("[{:{}}] {:.1f}%".format("="*di, dn-1, (100/(dn-1)*di)))
            sys.stdout.flush()
            # # time.sleep(0.25)

            with bz2.open('{}/{}/{}'.format(self.dpath,path,dump), 'r') as article:
                #Load all block
                wikixml = B(article.read().decode('utf-8'), features='lxml')
                #Parse through each document. 
                documents = wikixml.findAll('doc')
                self.doc_count += len(documents)
                # n = len(documents)
                # i = 0
                # print(len(documents))
                for document in documents:
                    # sys.stdout.write('\r')
                    # # the exact output you're looking for:
                    # sys.stdout.write("[{:{}}] {:.1f}%".format("="*i, n-1, (100/(n-1)*i)))
                    # sys.stdout.flush()
                    # time.sleep(0.25)

                    content = document.get_text() #STRING OF TEXT

                    #TOKENIZE
                    tokens = content.split()

                    #Build dictionary
                    self.counter(tokens, document['id'])
                    #Save to sqlite db
                    self.insert_db(document['id'],document['title'],content)
            di+=1

        self.write_lengths()
        print()
        print("Done parsing block")
        end = time.time()
        print("Took {} seconds".format(end-start))
        #Update counters
        self.block_count += 1
        # self.doc_count += len(flist)
        print("Doc count is {}".format(self.doc_count))
        if self.block_count == len(self.flist):
            print("DONE PARSING CORPUS")
            self.collection_size = self.doc_count
            self.done = True
            # exit()
    
    def mergeblocks(self):
        #Build file list
        flist = []
        for i in range(1,self.block_count+1):
            flist.append(codecs.open("temp/idxfile{}.txt".format(i),encoding='utf-8'))
        
        if not os.path.isdir('temp'):
            os.mkdir('temp')
        # f = codecs.open("idxfile{}.txt".format(self.block_count),'w',encoding='utf-8')

        idxf = codecs.open("temp/sortedidxfile.txt".format(i),'w',encoding='utf-8')
        # with open ("temp/sortedidxfile.txt".format(i),'w') as idxf :
        tokens = [ ((line.split(":",1)[0], line) for line in f) for f in flist]
        merged = heapq.merge(*tokens)
        ungenerated = list(map(itemgetter(-1), merged))

        idxf.writelines(ungenerated)

        # f.close()
        #Sorted files
        sortidx = codecs.open('temp/sortedidxfile.txt',encoding='utf-8')

        head = sortidx.readline()
        index = codecs.open('index.txt', 'w+',encoding='utf-8')

        if head:
            head = head.rstrip()
            prev = head.split(':')[0]
            # print("Writing {}".format(head))
            index.write('{}'.format(head))

        while True:
            head = sortidx.readline()
            head = head.rstrip()

            if len(head) == 0:
                break

            if head.split(':')[0] == prev.split(':')[0]: #Same token, merge results

                new = (head.split(':')[1])
                index.write(':{}'.format(new))

            else:
                index.write('\n')
                index.write('{}'.format((head)))

            prev = head



#Begin indexing
# documents = open('test.txt',"r")
indexer = Indexer()
start = time.time()
#Navigate to dump directory (extracted)
indexer.navigate()
indexer.create_sqlite()
while (True):
    #Block
    indexer.parseblock()
    indexer.index()
    if indexer.done:
        break
end = time.time()
print("Done indexing blocks. Took {} seconds".format(end-start))
indexer.write_avg()
#Merge blocks. 
print("Beginning merge...")
indexer.mergeblocks()

