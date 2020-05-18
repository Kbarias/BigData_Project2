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

    path='C:\\Users\\IVETTE ORTIZ\\Downloads\\project2_egfr.txt'

    f = open(path)

    csv_f = csv.reader(f)

    i = 1

    for row in csv_f:

        doc_content = ( i, row[0].split(' ',1)[1]) #( int(row[0].split(' ',1)[0] ), row[0].split(' ',1)[1])  #split each line into docid and contents

        key_values.append(doc_content)

        i+=1

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

def tf(firstmap):

    array = []

    total_terms_in_doc = sum(firstmap.values())

    for pair in firstmap.items():

        tup = (pair[0], (pair[1]/ total_terms_in_doc))

        array.append(tup)

    return array



#calculates tf-idf for each document in the term's associative array

def idf(list_of_docs, num):

    array = []

    num_docs = len(list_of_docs)

    for tf in list_of_docs:

        tup = (tf[0], (tf[1] * math.log10(num/num_docs) ) )

        array.append(tup)

    return array



array = fileParser()



corpus_docs = len(array)



#part ONE

docs = sc.parallelize(array)



#returing a new rdd with <key = docid, value = [term_1:count,...]>

firstmap = docs.map(lambda line: (line[0], mapperOne(line[1])))



#returning a new rdd with <key = term, value = (docid, tf)>

secmap = firstmap.flatMapValues(tf).map(lambda line: (line[1][0], (line[0], line[1][1])) )



#combiner to return a new rdd with <key = term, value = [(docid, tf-idf), ...]

combined = secmap.groupByKey().map(lambda x : (x[0], idf(list(x[1]) , corpus_docs) ) )



#rdd filtered with only terms with 'gene_xxx_gene' as key

filtered_terms = combined.filter(lambda x : (x[0].startswith("gene_") and x[0].endswith("_gene") ))



#print rdd of filtered terms with associative array of tf-idf

#print(filtered_terms.collect())



#list of filtered terms

terms = filtered_terms.map(lambda x : (x[0])).collect()

def p(filtered_terms):return filtered_terms


#returning a new rdd with <key = docid, value = (term , tf-idf)>

doc_idf = filtered_terms.flatMapValues(p).map(lambda line: (line[1][0], (line[0], line[1][1])) )


#returning a new rdd with <key = docid, value = [(term, tf-idf),...]>

doc_term_array = doc_idf.groupByKey().map(lambda x : (x[0], list( x[1]) ) )



def createMatrixRows(line, words):
    row = [0] * (len(words) + 1)
    row[0] = line[0] #first column is docid
    dicti = {}
    for tup in line[1]:
        dicti[tup[0]] = tup[1]
    i = 0
    for item in words:
        if(item in dicti):
            row[i + 1] = (item, dicti[item] )
        i+=1
    return row



#returning a new rdd with each item being a row to the matrix, doc x terms
matrix_data = doc_term_array.map(lambda x : createMatrixRows(x, terms) ).collect()


sc.stop()
