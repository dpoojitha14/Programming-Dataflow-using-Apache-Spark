from pyspark import SparkContext, SparkConf
from operator import add
from os import listdir
from os.path import isfile,join
import sys
import csv
import re
import datetime


APP_NAME = " HelloWorld of Big Data"


def main(sc, filename, lemma_dict):

    pairs_list = []

    onlyfiles = [f for f in listdir(filename) if isfile(join(filename, f))]
    #print onlyfiles
    #print len(onlyfiles)
    for index in range(120):
        #print index
        if(index < len(onlyfiles)):
            path = "".join([filename,"/",onlyfiles[index]])
            with open(path, "rb") as input_file:
                data = input_file.readlines()

                # pairs_list = []

                for l in range(len(data)):
                    line = data[l]

                    m = re.search('<(.+?)>', line)
                    chapter = ""
                    if m:
                        chapter = m.group(1)
                        # print chapter
                    chapter = "".join(["[", chapter, "]"])

                    length = len(chapter) + 3
                    remaining = line[length:]

                    wordList = re.sub("[^\w]", " ", remaining).split()

                    # print wordList

                    for i in range(len(wordList)):
                        term1 = wordList[i]
                        if (i + 1 < len(wordList)):
                            term2 = wordList[i + 1]
                            lemma1 = []
                            lemma2 = []

                            if (lemmas_dict.has_key(term1)):
                                lemma1 = lemmas_dict.get(term1)
                            if (lemmas_dict.has_key(term2)):
                                lemma2 = lemmas_dict.get(term2)

                            if (len(lemma1) == 0 and len(lemma2) == 0):
                                key = "".join(["(", term1, " , ", term2, ")"])
                                pairs_list.append((key, chapter))

                            else:
                                if (len(lemma1) == 0):
                                    for j in range(len(lemma2)):
                                        key = "".join(["(", term1, " , ", lemma2[j], ")"])
                                        pairs_list.append((key, chapter))

                                if (len(lemma2) == 0):
                                    for j in range(len(lemma1)):
                                        key = "".join(["(", lemma1[j], " , ", term2, ")"])
                                        pairs_list.append((key, chapter))

                                if (len(lemma1) > 0 and len(lemma2) > 0):
                                    for j in range(len(lemma1)):
                                        for k in range(len(lemma2)):
                                            key = "".join(["(", lemma1[j], " , ", lemma2[k], ")"])
                                            pairs_list.append((key, chapter))

    pairsRDD = sc.parallelize(pairs_list)
    print type(pairs_list[0][0])
    print type(pairs_list[0][1])
    pairscount = pairsRDD.aggregateByKey("", lambda s, d: s + d, lambda s1, s2: s1 + " " + s2)
    ans = pairscount.collect()

    # print ans

    with open("output_bigrams_120.txt", "a") as output:
        for wc in ans:
            pair = "".join([wc[0], "[", wc[1], "]", "\n"])
            output.write(pair)


if __name__ == "__main__":
    # Configure Spark
    start = datetime.datetime.now()
    
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    filename = sys.argv[1]

    # Obtain the lemmatizer
    lemmas_dict = dict()
    with open("new_lemmatizer.csv", "rb") as f:
        reader = csv.reader(f)
        for row in reader:
            lemma_list = row
            length = len(lemma_list)
            value = []
            for i in range(length):
                if i == 0:
                    key = lemma_list[i]
                else:
                    if lemma_list[i] != "":
                        value.append(lemma_list[i])
            lemmas_dict[key] = value

    # Execute Main functionality


    main(sc, filename, lemmas_dict)
    finish = datetime.datetime.now()
    delta = finish - start
    print "Time taken: ",
    print delta