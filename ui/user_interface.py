def get_league():
    leagues = ["bundesliga", "epl", "la_liga", "ligue-1"]
    print("Choose a league:\n")
    for league in leagues:
        print(league)
    league_input = input()
    if league_input not in leagues:
        return None
    return league_input


def get_team(teams):
    list_of_teams = []
    print("Type de name of any of the following teams or * to get all team's statistics: \n")
    for team in teams:
        print(team["HomeTeam"])
        list_of_teams.append(team["HomeTeam"])
    user_input = input()
    if user_input not in list_of_teams:
        if user_input != "*":
            return None
    return user_input

