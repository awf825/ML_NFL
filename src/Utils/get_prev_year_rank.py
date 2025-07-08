ranks = {
    2018: {
        'CLE': 32,
        'NYG': 31,
        'IND': 30,
        'HOU': 29,
        'DEN': 28,
        'NYJ': 27,
        'TAM': 26,
        'CHI': 25,
        'SFO': 24,
        'OAK': 23,
        'MIA': 22,
        'CIN': 21,
        'WAS': 20,
        'GNB': 19,
        'ARI': 18,
        'BAL': 17,
        'BUF': 16,
        'LAC': 15,
        'SEA': 14,
        'DAL': 13,
        'DET': 12,
        'TEN': 11,
        'LAR': 10,
        'CAR': 9,
        'KAN': 8,
        'ATL': 7,
        'NOR': 6,
        'PIT': 5,
        'JAX': 4,
        'MIN': 3,
        'NWE': 2,
        'PHI': 1
    },
    2019: {},
    2020: {},
    2021: {},
    2022: {},
    2023: {},
    2024: {},
    2025: {},
}

def get_prev_year_rank(year, team):
    ranks_by_year = ranks.get(year)
    rank_by_team = ranks_by_year.get(team)
    return rank_by_team