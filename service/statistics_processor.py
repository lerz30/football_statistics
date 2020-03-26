from pyspark.sql import *
import ui.user_interface

def create_spark_session():
   return SparkSession \
        .builder \
        .appName("English Premier League match statistics") \
        .getOrCreate()


def read_raw_data(ss, league):
    # First look at our schema
    raw_statistics_df = ss.read.json("../data/" + league + "/*.json")
    raw_statistics_df.printSchema()

    # Columns to delete from our DF
    columns_to_drop = ["BSH", "BSD", "BSA", "BWH", "BWD", "BWA", "GBH", "GBD", "GBA", "IWH", "IWD", "IWA", "LBH", "LBD",
                       "SOH", "SOD", "SOA", "SBH", "SBD", "SBA", "SJH", "SJD", "SJA", "SYH", "SYD", "SYA", "VCH", "VCD",
                       "VCA", "B365H", "B365D", "B365A", "Bb1X2", "BbMxH", "BbAvH", "BbMxD", "BbAvD", "BbMxA", "BbAvA",
                       "BbOU", "BbAv>2.5", "BbMx<2.5", "BbAv<2.5", "BbAH", "BbAHh", "AHh", "BbMxAHH", "BbAvAHH",
                       "BbMxAHA", "BbAvAHA", "BbMx>2.5", "ABP", "HBP", "WHH", "WHD", "WHA", "PSH", "PSD", "PSCH",
                       "PSCD", "PSCA", "PSA", "LBA"]

    # Keeping match statistics columns only and renaming them
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
    return statistics


def played_games_df(statistics):
    home_matches = statistics \
        .groupBy("HomeTeam") \
        .count() \
        .withColumnRenamed("HomeTeam", "Team") \
        .withColumnRenamed("count", "Home_matches")

    away_matches = statistics \
        .groupBy("AwayTeam") \
        .count() \
        .withColumnRenamed("AwayTeam", "Team") \
        .withColumnRenamed("count", "Away_matches")

    return home_matches \
        .join(away_matches, "Team", "inner") \
        .withColumn("matches_played", home_matches.Home_matches + away_matches.Away_matches) \
        .drop("Home_matches") \
        .drop("Away_matches")


def won_games_df(statistics):
    total_matches = played_games_df(statistics)

    home_wins = statistics \
        .where(statistics.FT_result == "H") \
        .groupBy("HomeTeam") \
        .count() \
        .withColumnRenamed("count", "home_wins") \
        .withColumnRenamed("HomeTeam", "Team")

    away_wins = statistics \
        .where(statistics.FT_result == "A") \
        .groupBy("AwayTeam") \
        .count() \
        .withColumnRenamed("count", "away_wins") \
        .withColumnRenamed("AwayTeam", "Team")

    return total_matches \
        .join(home_wins, "Team", "inner") \
        .join(away_wins, "Team", "inner") \
        .withColumn("total_wins", home_wins.home_wins + away_wins.away_wins) \
        .withColumn("Win %", ((home_wins.home_wins + away_wins.away_wins)*100)/total_matches.matches_played)


def tied_games_df(statistics):
    home_draws = statistics \
        .where(statistics.FT_result == "D") \
        .groupBy("HomeTeam") \
        .count() \
        .withColumnRenamed("count", "home_draws") \
        .withColumnRenamed("HomeTeam", "Team")

    away_draws = statistics \
        .where(statistics.FT_result == "D") \
        .groupBy("AwayTeam") \
        .count() \
        .withColumnRenamed("count", "away_draws") \
        .withColumnRenamed("AwayTeam", "Team")

    return home_draws \
        .join(away_draws, "Team", "inner") \
        .withColumn("total_draws", home_draws.home_draws + away_draws.away_draws)


def generate_matches_statistics(statistics, team, league):
    wins_stats = won_games_df(statistics)
    draw_stats = tied_games_df(statistics)

    if team != "*":
        wins_stats = wins_stats.where(wins_stats.Team == team)
        draw_stats = draw_stats.where(draw_stats.Team == team)

    result_df = wins_stats\
        .join(draw_stats, "Team", "inner")\
        .withColumn("Draw %", (draw_stats.total_draws*100)/wins_stats.matches_played)\
        .orderBy("total_wins", ascending=False)
    result_df.limit(10).show()
    result_df.coalesce(1).write.format('csv').save("../tmp/" + league + "/games_stats.csv", header='true')



def calculate_goals_stats(statistics):
    home_team_goals = statistics \
        .select("HomeTeam", "FT_homeGoals", "FT_awayGoals") \
        .groupBy("HomeTeam") \
        .sum("FT_homeGoals", "FT_awayGoals") \
        .withColumnRenamed("HomeTeam", "Team") \
        .withColumnRenamed("sum(FT_homeGoals)", "home_scored") \
        .withColumnRenamed("sum(FT_awayGoals)", "home_received")

    away_team_goals = statistics \
        .select("AwayTeam", "FT_homeGoals", "FT_awayGoals") \
        .groupBy("AwayTeam") \
        .sum("FT_homeGoals", "FT_awayGoals") \
        .withColumnRenamed("AwayTeam", "Team") \
        .withColumnRenamed("sum(FT_homeGoals)", "away_received") \
        .withColumnRenamed("sum(FT_awayGoals)", "away_scored")

    return home_team_goals \
        .join(away_team_goals, "Team", "inner") \
        .withColumn("total_scored", home_team_goals.home_scored + away_team_goals.away_scored) \
        .withColumn("total_received", home_team_goals.home_received + away_team_goals.away_received) \
        .withColumn("goal_diff", (home_team_goals.home_scored + away_team_goals.away_scored) -
                    (home_team_goals.home_received + away_team_goals.away_received))\
        .orderBy("goal_diff", ascending=False)


def calculate_shots_stats(statistics):
    home_shots_stats_df = statistics \
        .select("HomeTeam", "home_shots", "home_shotsOT") \
        .groupBy("HomeTeam") \
        .sum("home_shots", "home_shotsOT") \
        .withColumnRenamed("sum(home_shots)", "total_home_shots") \
        .withColumnRenamed("sum(home_shotsOT)", "total_home_shotsOT") \
        .withColumnRenamed("HomeTeam", "Team")

    away_total_shots_df = statistics \
        .select("AwayTeam", "away_shots", "away_shotsOT") \
        .groupBy("AwayTeam") \
        .sum("away_shots", "away_shotsOT") \
        .withColumnRenamed("sum(away_shots)", "total_away_shots") \
        .withColumnRenamed("sum(away_shotsOT)", "total_away_shotsOT") \
        .withColumnRenamed("AwayTeam", "Team")

    return home_shots_stats_df.join(away_total_shots_df, "Team", "inner") \
        .withColumn("total_shots", home_shots_stats_df.total_home_shots + away_total_shots_df.total_away_shots) \
        .withColumn("total_shotsOT", home_shots_stats_df.total_home_shotsOT + away_total_shots_df.total_away_shotsOT) \
        .drop("total_home_shots", "total_home_shotsOT", "total_away_shots", "total_away_shotsOT")


def generate_goals_statistics(statistics, team, league):
    goals_stats_df = calculate_goals_stats(statistics)
    shots_stats_df = calculate_shots_stats(statistics)

    if team != "*":
        goals_stats_df = goals_stats_df.where(goals_stats_df.Team == team)
        shots_stats_df = shots_stats_df.where(shots_stats_df.Team == team)

    goals_stats_df.limit(10).show()
    goals_stats_df.coalesce(1).write.format('csv').save("../tmp/" + league + "/goals_stats.csv", header='true')

    result_df = shots_stats_df.join(goals_stats_df, "Team", "inner")\
        .withColumn("shots_accuracy", (goals_stats_df.total_scored*100)/shots_stats_df.total_shots)\
        .drop("home_scored", "home_received", "away_received", "away_scored", "total_received", "goal_diff")\
        .orderBy("shots_accuracy", ascending=False)
    result_df.limit(10).show()
    result_df.coalesce(1).write.format('csv').save("../tmp/" + league + "/shots_stats.csv", header='true')


ss = create_spark_session()

league = ui.user_interface.get_league()
if league is not None:
    statistics_df = read_raw_data(ss, league)
    statistics_df.limit(5).show()

    team = ui.user_interface.get_team(statistics_df.select("HomeTeam").distinct().orderBy("HomeTeam").collect())
    if team is not None:
        generate_matches_statistics(statistics_df, team, league)
        generate_goals_statistics(statistics_df, team, league)
