# expansions and relocations
# '79 St. Louis Cardinals, '88 Phoenix Cardinals, '94 Arizona Cardinals
#    STL before 1988 should be ARI!!! PHO is ARI
# '79 Oakland Raiders, '82 Los Angeles Raiders, '95 Oakland Raiders, '20 Las Vegas Raaiders
#   79-81 OAK, 82-94 RAI, 95-19 OAK, 20- LVR
# '79 Houston Oilers, '97 Tennessee Oilers; '02 HOU is now texans, by this point, TEN is already titans
#   79-96, HOU is TEN; 97-01 TEN is TEN; -02 HOU is HOU, TEN is TEN
# '79-94 LAR Rams, RAM 95-15 STL, -16 LAR
#   STL after 95 is LAR (effectively after 1988)
# Baltimore and indy
# '79-83 BAL -> IND
#  Cardinals
#       '79 St. Louis Cardinals, '88 Phoenix Cardinals, '94 Arizona Cardinals
#  Oilers/Titans
#       '79 Houston Oilers, '97 Tennessee Oilers; 
#       '02 HOU is now texans, by this point, TEN is already titans
#        79-96, HOU is TEN; 97-01 TEN is TEN; -02 HOU is HOU, TEN is TEN
#  Rams
#       '79-94 LAR Rams, RAM 95-15 STL, -16 LAR
#       STL after 95 is LAR (effectively after 1988)
# Baltimore and indy
#       96- BAL -> BAL

team_abbrev_index = {
    'ATL': 'Atlanta Falcons',
    'BUF': 'Buffalo Bills',
    'CHI': 'Chicago Bears',
    'CIN': 'Cincinnati Bengals',
    'CLE': 'Cleveland Browns',
    'DAL': 'Dallas Cowboys',
    'DEN': 'Denver Broncos',
    'DET': 'Detroit Lions',
    'GNB': 'Green Bay Packers',
    'IND': 'Indianapolis Colts',
    'KAN': 'Kansas City Chiefs',
    'LVR': 'Las Vegas Raiders',
    'OAK': 'Las Vegas Raiders',
    'STL': 'Los Angeles Rams', # ST Louis Rams
    'LAR': 'Los Angeles Rams',
    'MIA': 'Miami Dolphins',
    'MIN': 'Minnesota Vikings',
    'NWE': 'New England Patriots',
    'NOR': 'New Orleans Saints',
    'NYG': 'New York Giants',
    'NYJ': 'New York Jets',
    'PHI': 'Philadelphia Eagles',
    'ARI': 'Arizona Cardinals',
    'PIT': 'Pittsburgh Steelers',
    'LAC': 'Los Angeles Chargers',
    'SDG': 'Los Angeles Chargers',
    'SFO': 'San Francisco 49ers',
    'SEA': 'Seattle Seahawks',
    'TAM': 'Tampa Bay Buccaneers',
    'WAS': 'Washington Commanders',
    'CAR': 'Carolina Panthers',
    'JAX': 'Jacksonville Jaguars',
    'BAL': 'Baltimore Ravens', # Baltimore Ravens
    'TEN': 'Tennessee Titans', # Houston Oilers
    'HOU': 'Houston Texans'
}

# 1988 - 1996

# Phoenix Cardinals is directed to ARI
# No Baltimore
# Any STL is directed to LAR
# Oakland Raiders
# Houston Oilers is directed to TEN
team_abbrev_index_1996 = {
    'ARI': 'Arizona Cardinals',
    'ATL': 'Atlanta Falcons',
    'BAL': 'Baltimore Ravens', # Ravens inaugural season 1996
    'BUF': 'Buffalo Bills',
    'CAR': 'Carolina Panthers',
    'CHI': 'Chicago Bears',
    'CIN': 'Cincinnati Bengals',
    'CLE': 'Cleveland Browns',
    'DAL': 'Dallas Cowboys',
    'DEN': 'Denver Broncos',
    'DET': 'Detroit Lions',
    'GNB': 'Green Bay Packers',
    'HOU': 'Tennessee Titans', # Houston Oilers
    'IND': 'Indianapolis Colts',
    'JAX': 'Jacksonville Jaguars',
    'KAN': 'Kansas City Chiefs',
    'LAC': 'Los Angeles Chargers',
    'MIA': 'Miami Dolphins',
    'MIN': 'Minnesota Vikings',
    'NWE': 'New England Patriots',
    'NOR': 'New Orleans Saints',
    'NYG': 'New York Giants',
    'NYJ': 'New York Jets',
    'OAK': 'Las Vegas Raiders',
    'PHI': 'Philadelphia Eagles',
    'PHO': 'Arizona Cardinals', # Pheonix Cardinals
    'PIT': 'Pittsburgh Steelers',
    'RAI': 'Las Vegas Raiders',
    'RAM': 'Los Angeles Rams',
    'SDG': 'Los Angeles Chargers',
    'SEA': 'Seattle Seahawks',
    'SFO': 'San Francisco 49ers',
    'STL': 'Los Angeles Rams', # ST Louis Rams
    'TAM': 'Tampa Bay Buccaneers',
    'WAS': 'Washington Commanders'
}

# 1979-1987
team_abbrev_index_1987 = {
    'ARI': 'Arizona Cardinals',
    'ATL': 'Atlanta Falcons',
    'BAL': 'Indianapolis Colts', # Baltimore Colts
    'BUF': 'Buffalo Bills',
    'CAR': 'Carolina Panthers',
    'CHI': 'Chicago Bears',
    'CIN': 'Cincinnati Bengals',
    'CLE': 'Cleveland Browns',
    'DAL': 'Dallas Cowboys',
    'DEN': 'Denver Broncos',
    'DET': 'Detroit Lions',
    'GNB': 'Green Bay Packers',
    'HOU': 'Tennessee Titans', # Houston Oilers
    'IND': 'Indianapolis Colts',
    'JAX': 'Jacksonville Jaguars',
    'KAN': 'Kansas City Chiefs',
    'LAC': 'Los Angeles Chargers',
    'MIA': 'Miami Dolphins',
    'MIN': 'Minnesota Vikings',
    'NWE': 'New England Patriots',
    'NOR': 'New Orleans Saints',
    'NYG': 'New York Giants',
    'NYJ': 'New York Jets',
    'OAK': 'Las Vegas Raiders',
    'PHI': 'Philadelphia Eagles',
    'PHO': 'Arizona Cardinals', # Pheonix Cardinals
    'PIT': 'Pittsburgh Steelers',
    'RAI': 'Las Vegas Raiders',
    'RAM': 'Los Angeles Rams',
    'SDG': 'Los Angeles Chargers',
    'SEA': 'Seattle Seahawks',
    'SFO': 'San Francisco 49ers',
    'STL': 'Arizona Cardinals', # ST Louis Cardinals
    'TAM': 'Tampa Bay Buccaneers',
    'WAS': 'Washington Commanders'
}

# team_abbrev_index = {
#     'ATL': 'Atlanta Falcons',
#     'BUF': 'Buffalo Bills',
#     'CHI': 'Chicago Bears',
#     'CIN': 'Cincinnati Bengals',
#     'CLE': 'Cleveland Browns',
#     'DAL': 'Dallas Cowboys',
#     'DEN': 'Denver Broncos',
#     'DET': 'Detroit Lions',
#     'GNB': 'Green Bay Packers',
#     'TEN': 'Tennessee Titans',
#     'IND': 'Indianapolis Colts',
#     'KAN': 'Kansas City Chiefs',
#     'LVR': 'Las Vegas Raiders',
#     'OAK': 'Las Vegas Raiders',
#     'LAR': 'Los Angeles Rams',
#     'STL': 'Los Angeles Rams',
#     'MIA': 'Miami Dolphins',
#     'MIN': 'Minnesota Vikings',
#     'NWE': 'New England Patriots',
#     'NOR': 'New Orleans Saints',
#     'NYG': 'New York Giants',
#     'NYJ': 'New York Jets',
#     'PHI': 'Philadelphia Eagles',
#     'ARI': 'Arizona Cardinals',
#     'PIT': 'Pittsburgh Steelers',
#     'LAC': 'Los Angeles Chargers',
#     'SDG': 'Los Angeles Chargers',
#     'SFO': 'San Francisco 49ers',
#     'SEA': 'Seattle Seahawks',
#     'TAM': 'Tampa Bay Buccaneers',
#     'WAS': 'Washington Commanders',
#     'CAR': 'Carolina Panthers',
#     'JAX': 'Jacksonville Jaguars',
#     'BAL': 'Baltimore Ravens',
#     'HOU': 'Houston Texans'
# }