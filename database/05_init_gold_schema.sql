\c arsenalfc_analytics

-- Dimension: Season
CREATE TABLE IF NOT EXISTS gold.dim_season (
    season_id SERIAL PRIMARY KEY,
    season_name VARCHAR(20) UNIQUE NOT NULL, 
    start_year INTEGER NOT NULL,
    end_year INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- Dimension: Competition
CREATE TABLE IF NOT EXISTS gold.dim_competition (
    competition_id SERIAL PRIMARY KEY,
    competition_name VARCHAR(100) UNIQUE NOT NULL, 
    competition_code VARCHAR(10), 
    country VARCHAR(50),
    tier INTEGER, 
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- Dimension: Team
CREATE TABLE IF NOT EXISTS gold.dim_team(
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) UNIQUE NOT NULL,
    team_short_name VARCHAR(50),
    fbref_team_id VARCHAR(50),
    understat_team_id VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Player
CREATE TABLE IF NOT EXISTS gold.dim_player(
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(200) NOT NULL,
    fbref_player_id VARCHAR(50),
    understat_player_id VARCHAR(50),
    position VARCHAR(20),
    nationality VARCHAR(50),
    birth_date DATE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_name, fbref_player_id)
);

-- Dimension: Game State
CREATE TABLE IF NOT EXISTS gold.dim_game_state (
    game_state_id SERIAL PRIMARY KEY,
    state_name VARCHAR(20) UNIQUE NOT NULL, 
    state_code VARCHAR(10),
    description TEXT
);

-- Dimension: Match
CREATE TABLE IF NOT EXISTS gold.dim_match(
    match_id VARCHAR(50) PRIMARY KEY,
    season_id INTEGER NOT NULL REFERENCES gold.dim_season(season_id),
    competition_id INTEGER NOT NULL REFERENCES gold.dim_competition(competition_id),
    match_date DATE NOT NULL,
    kickoff_time TIME,
    home_team_id INTEGER NOT NULL REFERENCES gold.dim_team(team_id),
    away_team_id INTEGER NOT NULL REFERENCES gold.dim_team(team_id),

    home_score INTEGER,
    away_score INTEGER,
    -- Match details
    venue VARCHAR(200),
    attendance INTEGER,
    referee VARCHAR(100),
    home_formation VARCHAR(20),
    away_formation VARCHAR(20),
    match_status VARCHAR(20),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- FACT TABLES
CREATE TABLE IF NOT EXISTS gold.fact_team_match_performance (
    team_match_id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL REFERENCES gold.dim_match(match_id),
    team_id INTEGER NOT NULL REFERENCES gold.dim_team(team_id),
    opponent_id INTEGER NOT NULL REFERENCES gold.dim_team(team_id),
    is_home BOOLEAN NOT NULL,
    goals_for INTEGER,
    goals_against INTEGER,
    result VARCHAR(1),
    xg_for DECIMAL(5,2),
    xg_against DECIMAL(5,2),
    npxg_for DECIMAL(5,2),
    npxg_against DECIMAL(5,2),
    possession_pct DECIMAL(5,2),
    passes_completed INTEGER,
    passes_attempted INTEGER,
    pass_completion_pct DECIMAL(5,2),
    progressive_passes INTEGER,
    shots INTEGER,
    shots_on_target INTEGER,
    shots_on_target_pct DECIMAL(5,2),
    progressive_carries INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(match_id, team_id)
);

CREATE TABLE IF NOT EXISTS gold.fact_player_match_performance(
    player_match_id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL REFERENCES gold.dim_match(match_id),
    player_id INTEGER NOT NULL REFERENCES gold.dim_player(player_id),
    team_id INTEGER NOT NULL REFERENCES gold.dim_team(team_id),
    minutes_played INTEGER,
    started BOOLEAN,
    position VARCHAR(20),
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    xg DECIMAL(5,2),
    npxg DECIMAL(5,2),
    xa DECIMAL(5,2),
    xag DECIMAL(5,2), 
    npxg_plus_xa DECIMAL(5,2),
    passes_completed INTEGER,
    passes_attempted INTEGER,
    pass_completion_pct DECIMAL(5,2),
    progressive_passes INTEGER,
    key_passes INTEGER,
    passes_into_final_third INTEGER,
    passes_into_penalty_area INTEGER,
    progressive_carries INTEGER,
    carries_into_final_third INTEGER,
    carries_into_penalty_area INTEGER,
    dribbles_completed INTEGER,
    dribbles_attempted INTEGER,
    dribbles_success_pct DECIMAL(5,2),
    sca INTEGER,
    gca INTEGER,
    tackles INTEGER,
    interceptions INTEGER,
    blocks INTEGER,
    clearances INTEGER,
    touches INTEGER,
    minutes_leading INTEGER DEFAULT 0,
    minutes_drawing INTEGER DEFAULT 0,
    minutes_trailing INTEGER DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(match_id, player_id)

);


CREATE TABLE IF NOT EXISTS gold.fact_match_events (
    event_id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL REFERENCES gold.dim_match(match_id),
    player_id INTEGER REFERENCES gold.dim_player(player_id),
    team_id INTEGER NOT NULL REFERENCES gold.dim_team(team_id),
    event_type VARCHAR(50) NOT NULL,
    minute INTEGER NOT NULL,
    second INTEGER,
    x_coord DECIMAL(5,2),
    y_coord DECIMAL(5,2),
    outcome VARCHAR(50),
    xg_value DECIMAL(5,4),
    xt_value DECIMAL(5,4),
    result_type VARCHAR(50),
    situation VARCHAR(50),
    shot_type VARCHAR(50),
    assist_type VARCHAR(50),
    score_diff INTEGER,
    game_state_id INTEGER REFERENCES gold.dim_game_state(game_state_id),
    source_system VARCHAR(20),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(match_id, player_id, event_type, minute, x_coord, y_coord)
);

CREATE INDEX IF NOT EXISTS idx_dim_match_season ON gold.dim_match(season_id);
CREATE INDEX IF NOT EXISTS idx_dim_match_competition ON gold.dim_match(competition_id);
CREATE INDEX IF NOT EXISTS idx_dim_match_date ON gold.dim_match(match_date DESC);
CREATE INDEX IF NOT EXISTS idx_dim_match_home_team ON gold.dim_match(home_team_id);
CREATE INDEX IF NOT EXISTS idx_dim_match_away_team ON gold.dim_match(away_team_id);

CREATE INDEX IF NOT EXISTS idx_fact_team_match_match ON gold.fact_team_match_performance(match_id);
CREATE INDEX IF NOT EXISTS idx_fact_team_match_team ON gold.fact_team_match_performance(team_id);

CREATE INDEX IF NOT EXISTS idx_fact_player_match_match ON gold.fact_player_match_performance(match_id);
CREATE INDEX IF NOT EXISTS idx_fact_player_match_player ON gold.fact_player_match_performance(player_id);
CREATE INDEX IF NOT EXISTS idx_fact_player_match_team ON gold.fact_player_match_performance(team_id);

CREATE INDEX IF NOT EXISTS idx_fact_events_match ON gold.fact_match_events(match_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_player ON gold.fact_match_events(player_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_team ON gold.fact_match_events(team_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_type ON gold.fact_match_events(event_type);
CREATE INDEX IF NOT EXISTS idx_fact_events_minute ON gold.fact_match_events(match_id, minute);


GRANT ALL ON ALL TABLES IN SCHEMA gold TO analytics_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA gold TO analytics_user;