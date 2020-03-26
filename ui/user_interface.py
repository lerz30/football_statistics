def get_league():
    return (str(input("Choose a league: \n")))


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

