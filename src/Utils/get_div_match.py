team_divs = {
    'TAM': 'NS',
    'ATL': 'NS',
    'NOR': 'NS',
    'CAR': 'NS',
    'DAL': 'NE',
    'PHI': 'NE',
    'WAS': 'NE',
    'NYG': 'NE',
    'ARI': 'NW',
    'SFO': 'NW',
    'LAR': 'NW',
    'SEA': 'NW',
    'PIT': 'AN',
    'CLE': 'AN',
    'CIN': 'AN',
    'BAL': 'AN',
    'IND': 'AS',
    'TEN': 'AS',
    'HOU': 'AS',
    'JAX': 'AS',
    'DEN': 'AW',
    'KAN': 'AW',
    'LAC': 'AW',
    'OAK': 'AW',
    'LVR': 'AW', # vegas
    'CHI': 'NN',
    'MIN': 'NN',
    'DET': 'NN',
    'GNB': 'NN',
    'NWE': 'AE',
    'MIA': 'AE',
    'BUF': 'AE',
    'NYJ': 'AE'
}

def get_div_match(away_team, home_team):
    away_div = team_divs.get(away_team)
    home_div = team_divs.get(home_team)

    if away_div == home_div:
        return 1.0
    else:
        return 0.0