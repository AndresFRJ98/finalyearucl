#BM 25 scores

#Term Frequency in document D

#D is document length

#AVG length
import numpy as np
import codecs
import time


f = open('avg.txt')
doc_avg = 0
totaldocs = 0
i = 0
for line in f:
    if  i == 0:
        totaldocs = int(line)
        i+=1
    else:
        doc_avg = float(line)
f.close()
print("Average length of a document  : {}".format(doc_avg))
print("Number of documents :  {}".format(totaldocs))
#IDF
def idf(q, posting):
    #Get number for docs with query
    global totaldocs

    docswithq = len(posting)
    num = totaldocs - docswithq + 0.5
    denom = docswithq + 0.5

    idf = np.log(num / denom)

    if idf < 0:
        return 0
    else:
        return idf

def idx_lookup(query):
    idx = codecs.open("index.txt",encoding='utf-8')
    postings = None
    for line in idx:
        # print(line)
        #Process line
        line = line.rstrip()
        line = line.split(":")
        if query == line[0]:
            #Match
            postings = line[1:]
    return postings
def getlength(doc):
    #Return document legnth
    f = open("lengths.txt")
    for line in f:
        line = line.split()
        if line[0] == doc:
            #Match
            return int(line[1])
    return None
def score(doc, term, posting):
    #Score of a document for a given query

    #Split query into tokens
    # clean_query = query.split()
    doclength = getlength(doc)
    global doc_avg
    k = 1.2
    b = 0.75

    idfq = idf(term,posting)
    fqid = 0
    for document in posting:
        if document.split(";")[0] == doc:
            #This document
            fqid = int(document.split(";")[1])
            break
    num = (fqid * (k + 1))
    denom = fqid + k * ( 1 - b + b * (doclength / doc_avg))
    return idfq * ( num / denom )
    
def main():

    #Get parsed query
    query = "Anarchism"
    query = query.split()
    docstorank = {}

    start = time.time()

    for term in query:
        #Find documents containing this term. 
        #Lookup on index
        postings = idx_lookup(term)

        if postings != None:
            #Get List of documents to consider
            # postingsdoc 
            for pair in postings:
                doc = pair.split(";")[0]
                if doc not in docstorank.keys():
                    docstorank[doc] = 0
                
                #Compute score for this term
                docstorank[doc] += score(doc, term, postings)

    print("Search took {} seconds".format(time.time() - start))
    print("SCORES:")
    
    docstorank = sorted(docstorank.items(), key=lambda x: x[1], reverse=True)    
    for key in docstorank:
        print("Article {} has Score of {} ".format(key[0],key[1]))
    
if __name__ == "__main__":
    main()