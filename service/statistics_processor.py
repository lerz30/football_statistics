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
# Wins statistics
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

wins_stats = home_wins\
    .join(away_wins, "Team", "inner")\
    .withColumn("total_wins", home_wins.home_wins + away_wins.away_wins)
wins_stats.printSchema()

wins_stats = total_matches\
    .join(wins_stats, "Team", "inner")\
    .withColumn("Win %", (wins_stats.total_wins*100)/total_matches.matches_played)
wins_stats.printSchema()

#
# Draws statistics
#
home_draws = statistics\
    .where(statistics.FT_result == "D")\
    .groupBy("HomeTeam")\
    .count()\
    .withColumnRenamed("count", "home_draws")\
    .withColumnRenamed("HomeTeam", "Team")
home_draws.printSchema()

away_draws = statistics\
    .where(statistics.FT_result == "D")\
    .groupBy("AwayTeam")\
    .count()\
    .withColumnRenamed("count", "away_draws")\
    .withColumnRenamed("AwayTeam", "Team")
away_draws.printSchema()

draw_stats = home_draws\
    .join(away_draws, "Team", "inner")\
    .withColumn("total_draws", home_draws.home_draws + away_draws.away_draws)
draw_stats.printSchema()

#Output table
games_table = wins_stats\
    .join(draw_stats, "Team", "inner")\
    .withColumn("Draw %", (draw_stats.total_draws*100)/wins_stats.matches_played)\
    .orderBy("total_wins", ascending=False)\
    .show()



#
# Goals statistics
#
home_team_goals = statistics\
    .select("HomeTeam", "FT_homeGoals", "FT_awayGoals")\
    .groupBy("HomeTeam")\
    .sum("FT_homeGoals", "FT_awayGoals")\
    .withColumnRenamed("HomeTeam", "Team")\
    .withColumnRenamed("sum(FT_homeGoals)", "home_scored")\
    .withColumnRenamed("sum(FT_awayGoals)", "home_received")
home_team_goals.printSchema()

away_team_goals = statistics\
    .select("AwayTeam", "FT_homeGoals", "FT_awayGoals")\
    .groupBy("AwayTeam")\
    .sum("FT_homeGoals", "FT_awayGoals")\
    .withColumnRenamed("AwayTeam", "Team")\
    .withColumnRenamed("sum(FT_homeGoals)", "away_received")\
    .withColumnRenamed("sum(FT_awayGoals)", "away_scored")
away_team_goals.printSchema()

goals_stats = home_team_goals\
    .join(away_team_goals, "Team", "inner")\
    .withColumn("total_scored", home_team_goals.home_scored + away_team_goals.away_scored)\
    .withColumn("total_received", home_team_goals.home_received + away_team_goals.away_received)
goals_stats.printSchema()

goals_stats = goals_stats\
    .withColumn("goal_dif", goals_stats.total_scored - goals_stats.total_received)\
    .orderBy("goal_dif", ascending = False)\
    .show()

























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

