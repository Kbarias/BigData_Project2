import os
import sys
import math

import findspark
findspark.init()

import csv
from pyspark import SparkContext

sc = SparkContext(master="local[4]")

def fileParser():
    key_values = []
    path='C:\\Users\\IVETTE ORTIZ\\Downloads\\project2_test.txt'
    f = open(path)
    csv_f = csv.reader(f)
    for row in csv_f:
        doc_content = ( int(row[0].split(' ',1)[0] ), row[0].split(' ',1)[1])  #split each line into docid and contents
        key_values.append(doc_content)
    f.close()
    return key_values

#creates associative array for terms and count in doc
def mapperOne(content):
    total_words = 0
    term_count = {}
    array = []
    terms = content.split(' ')
    for word in terms:
        if(word != ''):
            total_words += 1
            term_count[word] = term_count.get(word, 0) + 1
    for pair in term_count.items():
        array.append(pair)
    return term_count

#calculates tf of term in doc
def f(firstmap):
    array = []
    total_terms_in_doc = sum(firstmap.values())
    for pair in firstmap.items():
        tup = (pair[0], (pair[1]/ total_terms_in_doc))
        array.append(tup)
    return array

def idf(list_of_docs, num):
    array = []
    num_docs = len(list_of_docs)
    for tf in list_of_docs:
        tup = (tf[0], (tf[1] * math.log10(num/num_docs) ) )
        array.append(tup)
    return array

array = fileParser()

corpus_docs = len(array)

docs = sc.parallelize(array)
#returing a new rdd with <key = docid, value = [term_1:count,...]>
firstmap = docs.map(lambda line: (line[0], mapperOne(line[1])))

#returning a new rdd with <key = term, value = (docid, tf)>
secmap = firstmap.flatMapValues(f).map(lambda line: (line[1][0], (line[0], line[1][1])) )

#combiner to return a new rdd with <key = term, value = [(docid, tf), ...]
combined = secmap.groupByKey().map(lambda x : (x[0], idf(list(x[1]) , corpus_docs) ) )



#print(firstmap.first())
#print(firstmap.collect())
#print(secmap.collect())
print(combined.collect())

# lines=sc.textFile(path)
sc.stop()