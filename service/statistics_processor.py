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
statistics.limit(5).show()

'''
TEAMS STATISTICS
'''
#
#Games played
#
home_matches = statistics\
    .groupBy("HomeTeam")\
    .count()\
    .withColumnRenamed("HomeTeam", "Team")\
    .withColumnRenamed("count", "Home_matches")

away_matches = statistics\
    .groupBy("AwayTeam")\
    .count()\
    .withColumnRenamed("AwayTeam", "Team")\
    .withColumnRenamed("count", "Away_matches")

total_matches = home_matches\
    .join(away_matches, "Team", "inner")\
    .withColumn("matches_played", home_matches.Home_matches + away_matches.Away_matches)\
    .drop("Home_matches")\
    .drop("Away_matches")
total_matches.printSchema()

#
#Top 5 Wins
#
home_wins = statistics\
    .where(statistics.FT_result == "H")\
    .groupBy("HomeTeam")\
    .count()\
    .withColumnRenamed("count", "home_wins")\
    .withColumnRenamed("HomeTeam", "Team")
home_wins.printSchema()

away_wins = statistics\
    .where(statistics.FT_result == "A")\
    .groupBy("AwayTeam")\
    .count()\
    .withColumnRenamed("count", "away_wins")\
    .withColumnRenamed("AwayTeam", "Team")
away_wins.printSchema()

total_wins = home_wins\
    .join(away_wins, "Team", "inner")\
    .withColumn("total_wins", home_wins.home_wins + away_wins.away_wins)
total_wins.printSchema()

total_wins = total_wins\
    .join(total_matches, "Team", "inner")\
    .withColumn("Win %", (total_wins.total*100)/total_matches.matches_played)\
    .orderBy("total_wins", ascending=False)\
    .show()

#
#Most scored goals
#
home_team_goals = statistics\
    .select("HomeTeam", "FT_homeGoals")\
    .groupBy("HomeTeam")\
    .sum("FT_homeGoals")\
    .withColumnRenamed("HomeTeam", "Team")\
    .withColumnRenamed("sum(FT_homeGoals)", "home_goals")\
    .orderBy("home_goals", ascending = False)
home_team_goals.printSchema()

away_team_goals = statistics\
    .select("AwayTeam", "FT_awayGoals")\
    .groupBy("AwayTeam")\
    .sum("FT_awayGoals")\
    .withColumnRenamed("AwayTeam", "Team")\
    .withColumnRenamed("sum(FT_awayGoals)", "away_goals")\
    .orderBy("away_goals", ascending = False)
away_team_goals.printSchema()

























'''
Referees statistics and analysis


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

