\c arsenalfc_analytics

CREATE TABLE IF NOT EXISTS silver.stg_matches(

    match_id VARCHAR(50) PRIMARY KEY,
    season VARCHAR(20) NOT NULL,
    competition VARCHAR(100) NOT NULL,
    match_date DATE NOT NULL,
    kickoff_time TIME,

    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    home_team_id VARCHAR(50),
    away_team_id VARCHAR(50),

    home_score INTEGER,
    away_score INTEGER,

    home_formation VARCHAR(20),
    away_formation VARCHAR(20),

    venue VARCHAR(200),
    attendance INTEGER,
    referee VARCHAR(100),

    match_status VARCHAR(20),
    source_system VARCHAR(20) DEFAULT 'fbref',

    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.stg_player_stats(
    player_stat_id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL,
    player_name VARCHAR(200) NOT NULL,
    player_id VARCHAR(50),
    team VARCHAR(100) NOT NULL,
    position VARCHAR(20),

    minutes_played INTEGER,
    started BOOLEAN,

    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,

    -- Expected goals
    xg DECIMAL(5,2),
    npxg DECIMAL(5,2), 
    xa DECIMAL(5,2),
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

    -- Shot/Goal creating actions
    sca INTEGER,
    gca INTEGER,

    -- Defensive
    tackles INTEGER,
    interceptions INTEGER,
    blocks INTEGER,
    clearances INTEGER,

    -- Possession
    touches INTEGER,

    -- Source
    source_system VARCHAR(20) DEFAULT 'fbref',
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(match_id, player_id)
);

CREATE TABLE IF NOT EXISTS silver.stg_shot_events(
    shot_id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL,

    player_name VARCHAR(200),
    player_id VARCHAR(50),
    team VARCHAR(100) NOT NULL,

    -- Shot details
    minute INTEGER NOT NULL,
    result VARCHAR(20),
    situation VARCHAR(50),
    shot_type VARCHAR(50),

    x_coord DECIMAL(5,2),
    y_coord DECIMAL(5,2),

    xg DECIMAL(5,4),

    assisted_by VARCHAR(200),
    last_action VARCHAR(50),
    
    source_system VARCHAR(20) DEFAULT 'understat',
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(match_id, player_id, minute, x_coord, y_coord)
);


CREATE TABLE IF NOT EXISTS silver.stg_team_stats(
    team_stat_id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL,
    team VARCHAR(100) NOT NULL,
    opponent VARCHAR(100) NOT NULL,

    goals_for INTEGER,
    goals_against INTEGER,

    -- Expected goals
    xg_for DECIMAL(5,2),
    xg_against DECIMAL(5,2),

    possession_pct DECIMAL(5,2),

    passes_completed INTEGER,
    passes_attempted INTEGER,
    pass_completion_pct DECIMAL(5,2),
    progressive_passes INTEGER,

    shots INTEGER,
    shots_on_target INTEGER,

    -- Progressive carries
    progressive_carries INTEGER,

    source_system VARCHAR(20) DEFAULT 'fbref',
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(match_id,team)

);

CREATE INDEX IF NOT EXISTS idx_stg_matches_date ON silver.stg_matches(match_date DESC);
CREATE INDEX IF NOT EXISTS idx_stg_matches_season ON silver.stg_matches(season);
CREATE INDEX IF NOT EXISTS idx_stg_player_stats_match ON silver.stg_player_stats(match_id);
CREATE INDEX IF NOT EXISTS idx_stg_player_stats_player ON silver.stg_player_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_stg_shot_events_match ON silver.stg_shot_events(match_id);
CREATE INDEX IF NOT EXISTS idx_stg_shot_events_player ON silver.stg_shot_events(player_id);
CREATE INDEX IF NOT EXISTS idx_stg_team_stats_match ON silver.stg_team_stats(match_id);

GRANT ALL ON ALL TABLES IN SCHEMA silver TO analytics_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA silver TO analytics_user;
