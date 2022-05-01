import pyspark

import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer,PorterStemmer
from nltk.corpus import stopwords
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import csv
import numpy as np
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml import *
from pyspark.ml.feature import *
import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

from os import listdir
from os.path import isfile, join
import os

import time

import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('vader_lexicon')

#Prepare spark session
spark = SparkSession.builder.appName('spark-yarn').getOrCreate()

#Get the list of tickers with news data
onlyfiles = [f for f in os.listdir('.') if os.path.isfile(f)]

#Dont include the script in the file names
onlyfiles.remove('PysparkSent.py')
onlyfiles = sorted(onlyfiles)

for m in range(len(onlyfiles)):
    ticker = onlyfiles[m][:-4]

    # Create dataframe of the csv file    
    df = spark.read.csv("hdfs://pi0:9000/NEWS/" + ticker + ".csv")
    df2=df.select(['_c2','_c3'])
    
    # Set stopwords dictionary
    stop_words = set(stopwords.words('english'))
    
    # Gather news titles to collection
    headlines_rdd = df2.select('_c3').rdd.flatMap(lambda x: x)
    headlines_rdd.collect()
    
    header= headlines_rdd.first()
    data_rev_col = headlines_rdd.filter(lambda row: row != header)
    
    # Set all news titles to lowercase
    lowerCase_rdd = data_rev_col.map(lambda x: x.lower())
    lowerCase_rdd.collect()
    
    def sent_TokenizeFunct(x):
        return nltk.sent_tokenize(x)
    
    # Tokenize all news titles
    sentenceTokenizeRDD = lowerCase_rdd.map(sent_TokenizeFunct)
    sentenceTokenizeRDD.collect()
    
    def word_TokenizeFunct(x):
        splittedwordTokenize = [word for line in x for word in line.split()]
        return splittedwordTokenize
    
    wordTokenizeRDD = sentenceTokenizeRDD.map(word_TokenizeFunct)
    wordTokenizeRDD.collect()
    
    def removeStopWordsFunct(x):
        filteredSentence = [w for w in x if not w in stop_words]
        return filteredSentence
    
    # Remove all stopwords
    stopwordRDD = wordTokenizeRDD.map(removeStopWordsFunct)
    stopwordRDD.collect()
    
    def removeStopWordsFunct2(x):
        stop_words = set(['asa:','asa','-'])
        filteredSentence = [w for w in x if not w in stop_words]
        return filteredSentence
    
    # Remove custom stopwords list
    stopwordRDD2 = stopwordRDD.map(removeStopWordsFunct2)
    stopwordRDD2.collect()
    
    # Tokenized collection with removed stopwords
    collection_values = wordTokenizeRDD.collect()
    
    data_list = []
    
    # Save all dates in collection
    dates_rdd = df2.select('_c2').rdd.flatMap(lambda x: x)
    dates_rdd.collect()
    
    # Create analyzer
    analyzer = SentimentIntensityAnalyzer()

    # Function to formate the datetime to correct syntax    
    def dateformater(x):
        stringcompl = ""
        
        for z in range(13):
            stringcompl+=x[z]
        
        stringcompl= stringcompl + ":00:00+00:00"
        print(stringcompl)
        print(x)
        print(x[11]+x[12]+":00:00+00:00")
        return(stringcompl)
    
    # Go through news titles and do sentiment analysis on the text
    for x in range(len(wordTokenizeRDD.collect())):
        add = []
        filtered_sentence = (" ").join(collection_values[x])
        result = analyzer.polarity_scores(filtered_sentence)
        add.append(ticker)
        add.append(dateformater(dates_rdd.collect()[x+1]))
        #add.append(result['neg'])
        #add.append(result['neu'])
        #add.append(result['pos'])
        add.append(result['compound'])
        print(filtered_sentence)
        
        data_list.append(add)
    
    # Add rolling mean to the sentiment analysis
    dftest = pd.DataFrame(data_list)
    dftest['3'] = dftest[2].rolling(window=5).mean()
    
    dftest['3'][0] = dftest[2][0]
    dftest['3'][1] = (dftest[2][0]+dftest[2][1])/2
    dftest['3'][2] = (dftest[2][0]+dftest[2][1]+dftest[2][2])/3
    dftest['3'][3] = (dftest[2][0]+dftest[2][1]+dftest[2][2]+dftest[2][3])/4
    
    dftest['3'] = dftest['3'].round(4)
    
    dftest = dftest[[0, 1, '3']]
    
    dftest.drop_duplicates(inplace=True)
    dftest.drop_duplicates(subset=[1, '3'], inplace=True)
    print(len(dftest))
    
    # Fix datetime column
    dftest['Date']= pd.to_datetime(dftest[1])
    dftest = dftest.resample('h', on='Date').mean().dropna(how='all')
    dftest['3'] = dftest['3'].round(4)
    dftest['Ticker']=ticker
    
    dftest['Dates']=dftest.index
    dftest.reset_index(drop=True)
    dftest = dftest[['Ticker', 'Dates', '3']]
    dftest.reset_index(drop=True)
    
    dftest.to_csv('Sentiment.csv', index=False, columns=None, header=False, mode='a')
















