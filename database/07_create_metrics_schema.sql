\c arsenalfc_analytics

CREATE TABLE IF NOT EXISTS metrics.player_rolling_xg(
    player_rolling_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES gold.dim_player(player_id),
    season_id INTEGER NOT NULL REFERENCES gold.dim_season(season_id),
    competition_id INTEGER REFERENCES gold.dim_competition(competition_id),

    last_match_date DATE NOT NULL,
    window_size INTEGER NOT NULL, 

    matches_played INTEGER,
    minutes_played INTEGER,

    rolling_xg DECIMAL(6,2),
    rolling_npxg DECIMAL(6,2),
    rolling_xa DECIMAL(6,2),
    rolling_goals INTEGER,
    rolling_assists INTEGER,

    xg_per_90 DECIMAL(5,2),
    npxg_per_90 DECIMAL(5,2),
    xa_per_90 DECIMAL(5,2),
    goals_per_90 DECIMAL(5,2),
    assists_per_90 DECIMAL(5,2),

    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(player_id, season_id, last_match_date, window_size, competition_id)
);

CREATE TABLE IF NOT EXISTS metrics.match_xg_flow (
    flow_id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) NOT NULL REFERENCES gold.dim_match(match_id),
    minute INTEGER NOT NULL,
    home_cumulative_xg DECIMAL(5,2),
    away_cumulative_xg DECIMAL(5,2),
    home_score INTEGER,
    away_score INTEGER,
    home_shots INTEGER DEFAULT 0,
    away_shots INTEGER DEFAULT 0,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(match_id, minute)

);

CREATE TABLE IF NOT EXISTS metrics.team_game_state_metrics(
    team_game_state_id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES gold.dim_team(team_id),
    season_id INTEGER NOT NULL REFERENCES gold.dim_season(season_id),
    game_state_id INTEGER NOT NULL REFERENCES gold.dim_game_state(game_state_id),
    competition_id INTEGER REFERENCES gold.dim_competition(competition_id),

    total_minutes INTEGER,
    shots INTEGER,
    shots_on_target INTEGER,
    xg DECIMAL(6,2),
    goals INTEGER,
    passes_completed INTEGER,
    passes_attempted INTEGER,
    pass_completion_pct DECIMAL(5,2),
    progressive_passes INTEGER,

    shots_per_90 DECIMAL(5,2),
    xg_per_90 DECIMAL(5,2),
    goals_per_90 DECIMAL(5,2),

    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, season_id, game_state_id, competition_id)

);


CREATE TABLE IF NOT EXISTS metrics.season_team_summary (
    summary_id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL REFERENCES gold.dim_team(team_id),
    season_id INTEGER NOT NULL REFERENCES gold.dim_season(season_id),
    competition_id INTEGER REFERENCES gold.dim_competition(competition_id),

    matches_played INTEGER,
    wins INTEGER,
    draws INTEGER,
    losses INTEGER,
    points INTEGER,

    goals_for INTEGER,
    goals_against INTEGER,
    goal_difference INTEGER,

    xg_for DECIMAL(6,2),
    xg_against DECIMAL(6,2),
    xg_difference DECIMAL(6,2),

    goals_vs_xg DECIMAL(6,2)
    goals_conceded_vs_xga DECIMAL(6,2),
    xg_per_match DECIMAL(5,2),
    xga_per_match DECIMAL(5,2),
    possession_avg DECIMAL(5,2),

    home_matches INTEGER,
    home_wins INTEGER,
    home_xg DECIMAL(6,2),
    away_matches INTEGER,
    away_wins INTEGER,
    away_xg DECIMAL(6,2),
    last_match_date DATE,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, season_id, competition_id)

);

CREATE TABLE IF NOT EXISTS metrics.season_player_summary (
    summary_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES gold.dim_player(player_id),
    team_id INTEGER NOT NULL REFERENCES gold.dim_team(team_id),
    season_id INTEGER NOT NULL REFERENCES gold.dim_season(season_id),
    competition_id INTEGER REFERENCES gold.dim_competition(competition_id),

    matches_played INTEGER,
    matches_started INTEGER,
    minutes_played INTEGER,

    goals INTEGER,
    assists INTEGER,
    xg DECIMAL(6,2),
    xa DECIMAL(6,2),
    xag DECIMAL(6,2),

    goals_per_90 DECIMAL(5,2),
    assists_per_90 DECIMAL(5,2),
    xg_per_90 DECIMAL(5,2),
    xa_per_90 DECIMAL(5,2),

    goals_vs_xg DECIMAL(5,2),
    conversion_rate DECIMAL(5,2),

    key_passes INTEGER,
    key_passes_per_90 DECIMAL(5,2),
    sca INTEGER,
    sca_per_90 DECIMAL(5,2),

    progressive_passes INTEGER,
    progressive_carries INTEGER,
    progressive_actions_per_90 DECIMAL(5,2),

    dribbles_completed INTEGER,
    dribbles_success_pct DECIMAL(5,2),

    last_match_date DATE,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(player_id, team_id, season_id, competition_id)
);

CREATE TABLE IF NOT EXISTS metrics.xt_grid(
    grid_id SERIAL PRIMARY KEY,
    x_bin INTEGER NOT NULL,
    y_bin INTEGER NOT NULL,

    move_to_shot_prob DECIMAL(6,4), 
    shot_to_goal_prob DECIMAL(6,4),
    xt_value DECIMAL(6,4),

    season_id INTEGER REFERENCES gold.dim_season(season_id),
    competition_id INTEGER REFERENCES gold.dim_competition(competition_id),

    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(x_bin, y_bin, season_id, competition_id)

);

CREATE INDEX IF NOT EXISTS  idx_player_rolling_xg_player ON metrics.player_rolling_xg(player_id);
CREATE INDEX IF NOT EXISTS idx_player_rolling_xg_season ON metrics.player_rolling_xg(season_id);
CREATE INDEX IF NOT EXISTS idx_match_xg_flow_match ON metrics.match_xg_flow(match_id);
CREATE INDEX IF NOT EXISTS idx_team_game_state_team ON metrics.team_game_state_metrics(team_id);
CREATE INDEX IF NOT EXISTS idx_team_game_state_season ON metrics.team_game_state_metrics(season_id);
CREATE INDEX IF NOT EXISTS idx_season_team_summary_team ON metrics.season_team_summary(team_id);
CREATE INDEX IF NOT EXISTS idx_season_team_summary_season ON metrics.season_team_summary(season_id);
CREATE INDEX IF NOT EXISTS idx_season_player_summary_player ON metrics.season_player_summary(player_id);
CREATE INDEX IF NOT EXISTS idx_season_player_summary_season ON metrics.season_player_summary(season_id);
CREATE INDEX IF NOT EXISTS idx_xt_grid_coords ON metrics.xt_grid(x_bin, y_bin);


GRANT ALL ON ALL TABLES IN SCHEMA metrics TO analytics_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA metrics TO analytics_user;
