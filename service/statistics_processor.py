from pyspark.sql import *


ss = SparkSession \
    .builder \
    .appName("English Premier League match statistics") \
    .getOrCreate()

#First look at our schema
raw_statistics_df = ss.read.json("../data/*.json")
raw_statistics_df.printSchema()

#Columns to delete from our DF
columns_to_drop = ["BSH", "BSD", "BSA", "BWH", "BWD", "BWA", "GBH", "GBD", "GBA", "IWH", "IWD", "IWA", "LBH", "LBD",
                   "SOH", "SOD", "SOA", "SBH", "SBD", "SBA", "SJH", "SJD", "SJA", "SYH", "SYD", "SYA", "VCH", "VCD",
                   "VCA", "B365H", "B365D", "B365A", "Bb1X2", "BbMxH", "BbAvH", "BbMxD", "BbAvD", "BbMxA", "BbAvA",
                   "BbOU", "BbAv>2.5", "BbMx<2.5", "BbAv<2.5", "BbAH", "BbAHh", "AHh", "BbMxAHH", "BbAvAHH", "BbMxAHA",
                   "BbAvAHA", "BbMx>2.5", "ABP", "HBP", "WHH", "WHD", "WHA", "PSH", "PSD", "PSCH", "PSCD", "PSCA",
                   "PSA", "LBA"]

#Keeping match statistics columns only and renaming them
statistics = raw_statistics_df\
    .drop(*columns_to_drop)\
    .withColumnRenamed("FTHG", "FT_homeGoals")\
    .withColumnRenamed("FTAG", "FT_awayGoals")\
    .withColumnRenamed("FTR", "FT_result")\
    .withColumnRenamed("HTHG", "HT_homeGoals")\
    .withColumnRenamed("HTAG", "HT_awayGoals")\
    .withColumnRenamed("HTR", "HT_result")\
    .withColumnRenamed("HS", "home_shots")\
    .withColumnRenamed("AS", "away_shots")\
    .withColumnRenamed("AST", "away_shotsOT")\
    .withColumnRenamed("HST", "home_shotsOT")\
    .withColumnRenamed("HHW", "home_woodwork")\
    .withColumnRenamed("AHW", "away_woodwork")\
    .withColumnRenamed("HC", "home_corner")\
    .withColumnRenamed("AC", "away_corners")\
    .withColumnRenamed("HF", "home_fouls_commited")\
    .withColumnRenamed("AF", "away_fouls_commited")\
    .withColumnRenamed("HFKC", "home_FK_conceded")\
    .withColumnRenamed("AFKC", "away_FK_conceded")\
    .withColumnRenamed("HO", "home_offside")\
    .withColumnRenamed("AO", "away_offside")\
    .withColumnRenamed("HY", "home_yellow")\
    .withColumnRenamed("AY", "away_yellow")\
    .withColumnRenamed("HR", "home_red")\
    .withColumnRenamed("AR", "away_red")
statistics.printSchema()

'''
Referees statistics and analysis
'''
referees = statistics\
    .select("Referee")\
    .distinct()\
    .count()
print(referees)

most_referred_games = statistics\
    .groupBy("Referee")\
    .count()\
    .withColumnRenamed("count", "matches")\
    .orderBy("matches", ascending = False)\
    .limit(40)\
    .show()

least_referred_games = statistics\
    .groupBy("Referee")\
    .count()\
    .withColumnRenamed("count", "matches")\
    .orderBy("matches")\
    .limit(10)\
    .show()

'''
Teams statistics
'''
