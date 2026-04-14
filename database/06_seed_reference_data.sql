\c arsenalfc_analytics

INSERT INTO gold.dim_season (season_name, start_year, end_year) VALUES
    ('2024-2025', 2024, 2025),
    ('2025-2026', 2025, 2026)
ON CONFLICT (season_name) DO NOTHING;

INSERT INTO gold.dim_competition (competition_name, competition_code, country, tier) VALUES
    ('Premier League','PL','England',1),
    ('UEFA Champions League', 'UCL', 'Europe', 1),
    ('FA Cup', 'FAC', 'England', 1),
    ('EFL Cup', 'EFL', 'England', 2),
    ('Community Shield', 'CS', 'England', 3)
ON CONFLICT (competition_name) DO NOTHING;


INSERT INTO gold.dim_game_state (state_name, state_code, description) VALUES
    ('Leading', 'W', 'Team is ahead in score'),
    ('Drawing', 'D', 'Score is level'),
    ('Trailing', 'L', 'Team is behind in score')
ON CONFLICT (state_name) DO NOTHING;


INSERT INTO gold.dim_team (team_name, team_short_name, fbref_team_id) VALUES
    ('Arsenal', 'ARS', '18bb7c10')
ON CONFLICT (team_name) DO NOTHING;


INSERT INTO gold.dim_team (team_name, team_short_name) VALUES
    ('Manchester City', 'MCI'),
    ('Liverpool', 'LIV'),
    ('Chelsea', 'CHE'),
    ('Manchester United', 'MUN'),
    ('Tottenham Hotspur', 'TOT'),
    ('Newcastle United', 'NEW'),
    ('Aston Villa', 'AVL'),
    ('Brighton & Hove Albion', 'BHA'),
    ('West Ham United', 'WHU'),
    ('Fulham', 'FUL'),
    ('Brentford', 'BRE'),
    ('Crystal Palace', 'CRY'),
    ('Everton', 'EVE'),
    ('Nottingham Forest', 'NFO'),
    ('Wolverhampton Wanderers', 'WOL'),
    ('Bournemouth', 'BOU'),
    ('Leicester City', 'LEI'),
    ('Ipswich Town', 'IPS'),
    ('Southampton', 'SOU'),
    ('Luton Town', 'LUT')
ON CONFLICT (team_name) DO NOTHING;