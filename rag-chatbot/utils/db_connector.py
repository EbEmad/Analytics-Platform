import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict,Any,List

class DatabaseConnector:
    def __init__(self):
        self.conn_params={
            "host":os.getenv('POSTGRES_HOST', 'localhost'),
            'port':os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'arsenalfc_analytics'),
            'user': os.getenv('POSTGRES_USER', 'analytics_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'analytics_pass')
        }
    
    def get_connection(self):
        return psycopg2.connect(**self.conn_params)
    

    def fetch_all_matches(self, team_name: str = None) -> List[Dict[str, Any]]:
        query="""
            SELECT
                m.match_date,
            m.season,
            m.team_name,
            m.opponent,
            m.venue,
            m.result,
            m.team_goals as arsenal_goals,
            m.opponent_goals,
            m.team_xg as arsenal_xg,
            m.opponent_xg,

            -- Shot statistics from silver.shot_events
            COALESCE(shot_stats.total_shots, 0) as total_shots,
            COALESCE(shot_stats.shots_on_target, 0) as shots_on_target,
            COALESCE(shot_stats.goals, 0) as goals,
            COALESCE(shot_stats.avg_shot_xg, 0) as avg_shot_xg,
            COALESCE(shot_stats.big_chances, 0) as big_chances,
            shot_stats.scorers
            

            FROM metrics.team_matches m
            LEFT JOIN LATERAL (
            SELECT
                COUNT(*) as total_shots,
                COUNT(*) FILTER (WHERE s.result IN ('Goal', 'SavedShot', 'ShotOnPost')) as shots_on_target,
                COUNT(*) FILTER (WHERE s.result = 'Goal') as goals,
                ROUND(AVG(s.xg)::numeric, 3) as avg_shot_xg,
                COUNT(*) FILTER (WHERE s.xg >= 0.35) as big_chances,
                STRING_AGG(DISTINCT s.player_name, ', ') FILTER (WHERE s.result = 'Goal') as scorers
            FROM silver.shot_events s
            WHERE s.match_url = m.match_url
              AND s.team = m.team_name
        ) shot_stats ON true
        WHERE 1=1

        """
        params=[]
        if team_name:
            query+= " AND m.team_name = %s"
            params.append(team_name)
        query+=" ORDER BY m.match_date DESC"

        with self.get_connection()as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params if params else None)
                return cursor.fetchall()
    
    def fetch_player_stats(self, season: str = None, team_name: str = None) -> List[Dict[str, Any]]:
        query = """
        SELECT
            player_name,
            team as team_name,
            season,
            total_shots,
            goals,
            total_xg,
            conversion_rate,
            big_chances,
            big_chances_scored,
            matches_played
        FROM metrics.player_season_stats
        WHERE 1=1
        """
        params = []
        if season:
            query += " AND season = %s"
            params.append(season)
        if team_name:
            query += " AND team = %s"
            params.append(team_name)
            
        query += " ORDER BY goals DESC, total_xg DESC"


        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params if params else None)
                return cursor.fetchall()
        
    def fetch_all_player_stats(self) -> List[Dict[str, Any]]:
        query = """
        SELECT
            player_name,
            team as team_name,
            season,
            total_shots,
            goals,
            total_xg,
            conversion_rate,
            big_chances,
            big_chances_scored,
            matches_played
        FROM metrics.player_season_stats
        WHERE goals > 0
        ORDER BY season DESC, goals DESC
        """

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                return cursor.fetchall()
    
    def fetch_opponent_analysis(self) -> List[Dict[str, Any]]:
        query = """
        SELECT
            opponent,
            matches_played,
            wins,
            draws,
            losses,
            goals_for,
            goals_against,
            avg_xg_for,
            avg_xg_against,
            win_rate
        FROM metrics.opponent_comparison
        ORDER BY matches_played DESC, win_rate DESC
        """

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                return cursor.fetchall()


