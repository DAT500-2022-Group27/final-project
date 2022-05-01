# final-project
DAT500 Group 27 Final Project, 2022

Here you will find our implementation for the Time Series Aggregation and Modeling for Many Stocks in Parallel project. This project was part of our evaluation and project work in DAT500-1 22V Data intensive systems. This implementation are using a combination of Hadoop with MRJob and Spark to be able to chew through big datasets and predict future stock prices on stocks at Oslo Børs Exchange. This was a group project by Adam J. Becker and Sondre Tennø.

Github link: https://github.com/DAT500-2022-Group27/final-project

## Dataset

We are using two different datasets for this project, one tick dataset, gathered from an API, and news data gathered from another API.
The code for downloading these datsets are included here on github, but the datasets are also uploaded so you won't have to download them yourself through the API, though the TICK dataset is only a sample, given github restrictions for filesize.
Both datasets are cleaned and aggregated by the hour.

TICK dataset(TICKS_SAMPLE.zip):
  - Zipped: 68.3MB
  - Unzipped: 764MB
  - Complete non-sampled unzipped: 10.5GB

News dataset(NEWS.zip):
  - Zipped: 35MB
  - Unzipped: 250MB


## Code
All the code in this project are uploaded in the code folder.
Here there are three new folders, separating code made in Spark, MRJob and regular python.

  - Spark files are used for doing sentiment analysis and machine learning to forecast future stock prices.

  - MRJob files are used for preprocessing mainly.

  - Python files are used mainly for downloading datasets and validating the data.


## Environment

Your environment is expected to be on a cluster with Hadoop and Spark installed.
You must also have python 3.6+ and the libraries that are imported in the codes to run this project.

## Paper

To contemplate the code, we have written a paper regarding our implementation of predicting stock prices by using Spark and Hadoop. What approaches we have chosen and why we did certain things.
The paper can be found in this github repository as 'Group27.pdf'.
