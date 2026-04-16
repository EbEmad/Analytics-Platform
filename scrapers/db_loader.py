import psycopg2
from psycopg2.extras import Json, execute_values
from contextlib import contextmanager
import logging
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

from config import config

logger = logging.getLogger(__name__)


class DatabaseLoader:

    def __init__(self, connection_string: Optional[str] = None):

        self.connection_string = connection_string or config.db_connection_string

    @contextmanager
    def get_connection(self):
        conn=None

        try:
            conn=psycopg2.connect(self.connection_string)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def save_fbref_raw(
        self,
        match_id: str,
        raw_data: Dict[str, Any],
        match_url: str,
        scrape_run_id: Optional[str] = None
    ) -> bool:
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = """
                        INSERT INTO bronze.fbref_raw
                            (match_id, match_url, raw_data, scrape_run_id, scraped_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (match_id)
                        DO UPDATE SET
                            raw_data = EXCLUDED.raw_data,
                            match_url = EXCLUDED.match_url,
                            scrape_run_id = EXCLUDED.scrape_run_id,
                            scraped_at = EXCLUDED.scraped_at,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING id
                    """

                    cur.execute(query, (
                        match_id,
                        match_url,
                        Json(raw_data),
                        scrape_run_id,
                        datetime.utcnow(),
                        datetime.utcnow()
                    ))

                    result = cur.fetchone()
                    logger.info(f"Saved FBref data for match {match_id} (ID: {result[0]})")

            return True

        except Exception as e:
            logger.error(f"Failed to save FBref data: {e}")
            return False
    
    def save_understat_raw(
        self,
        match_id: str,
        raw_shots: Dict[str, Any],
        match_url: str,
        scrape_run_id: Optional[str] = None,
        team_name: Optional[str] = None
    ) -> bool:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = """
                        INSERT INTO bronze.understat_raw
                            (match_id, match_url, raw_shots, scrape_run_id, scraped_at, team_name)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (match_id)
                        DO UPDATE SET
                            raw_shots = EXCLUDED.raw_shots,
                            match_url = EXCLUDED.match_url,
                            scraped_at = EXCLUDED.scraped_at,
                            team_name = COALESCE(EXCLUDED.team_name, bronze.understat_raw.team_name),
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING id
                    """

                    cur.execute(query, (
                        match_id,
                        match_url,
                        Json(raw_shots),
                        scrape_run_id,
                        datetime.utcnow(),
                        team_name
                    ))

                    result = cur.fetchone()
                    logger.info(f"Saved Understat data for match {match_id} (ID: {result[0]}, Team: {team_name})")

            return True

        except Exception as e:
            logger.error(f"Failed to save Understat data: {e}")
            return False
    
    def create_scrape_run(self,run_id:str,match_id:str,scrape_type:str,dag_run_id:Optional[str]=None)->bool:

        try:
            with self.get_connection() as conn:
                with conn.cursor()as cur:
                    query = """
                        INSERT INTO bronze.scrape_runs
                            (run_id, dag_run_id, match_id, scrape_type, status, started_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id) DO NOTHING
                    """

                    cur.execute(query,(
                        run_id,
                        dag_run_id,
                        match_id,
                        scrape_type,
                        'running',
                        datetime.utcnow()
                    ))
            logger.info(f"Created scrape run: {run_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to create scrape run: {e}")
            return False
    

    def update_scrape_run(self,run_id:str,status:str,records_scraped:Optional[int]=None,error_message:Optional[str]=None)->bool:

        try:
            with self.get_connection()as conn:
                with conn.cursor() as cur:
                    query = """
                        UPDATE bronze.scrape_runs
                        SET status = %s,
                            records_scraped = %s,
                            error_message = %s,
                            completed_at = %s
                        WHERE run_id = %s
                    """
                    cur.execute(query,(
                        status,
                        records_scraped,
                        error_message,
                        datetime.utcnow(),
                        run_id
                    ))

            logger.info(f"Updated scrape run {run_id}:{status}")
            return True
        except Exception as e:
            logger.error(f"Failed to update scrape run: {e}")
            return False
    
    def get_latest_scrape_for_match(self, match_id: str, scrape_type: str) -> Optional[Dict]:

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = """
                        SELECT run_id, status, scraped_at, records_scraped
                        FROM bronze.scrape_runs
                        WHERE match_id = %s
                          AND scrape_type = %s
                          AND status = 'success'
                        ORDER BY completed_at DESC
                        LIMIT 1
                    """

                    cur.execute(query,(match_id,scrape_type))
                    row=cur.fetchone()


                    if row:
                        return {
                            'run_id': row[0],
                            'status': row[1],
                            'scraped_at': row[2],
                            'records_scraped': row[3]
                        }
            return None
        except Exception as e:
            logger.error(f"Failed to get latest scrape: {e}")
            return None


    def check_match_exists(self, match_id: str) -> bool:

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query="SELECT 1 FROM bronze.fbref_raw WHERE match_id = %s LIMIT 1"
                    cur.execute(query,(match_id,))
                    return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Failed to check match existence: {e}")
            return False

    def save_match_reference(
        self,
        match_url: str,
        home_team: str,
        away_team: str,
        match_date: str,
        season: str,
        team_name: str
    ) -> bool:
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query="""
                        INSERT INTO bronze.match_reference
                        (match_url, home_team, away_team, match_date, season, team_name)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (match_url, team_name)
                        DO UPDATE SET
                            home_team = EXCLUDED.home_team,
                            away_team = EXCLUDED.away_team,
                            match_date = EXCLUDED.match_date,
                            season = EXCLUDED.season
                        """
                    
                    cur.execute(query, (
                        match_url,
                        home_team,
                        away_team,
                        match_date,
                        season,
                        team_name
                    ))

                    conn.commit()
                    logger.info(f"Saved match reference for {home_team} vs {away_team} (Team: {team_name})")
            return True
        except Exception as e:
            logger.error(f"Failed to save match reference: {e}")
            return False


    def get_existing_matches_for_team(self, team_name: str, season_prefix: str) -> set:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query="""SELECT DISTINCT match_id FROM bronze.understat_raw WHERE team_name = %s
                        AND match_id LIKE %s  """
                    
                    cur.execute(query,(team_name,f"{season_prefix}%"))
                    return set(row[0] for row in cur.fetchall())
        except Exception as e:
            logger.error(f"Failed to get existing matches: {e}")
            return set()

    def save_fbref_lineups(self,match_url:str,lineup_data: Dict[str, Any],match_id: Optional[str] = None,scrape_run_id: Optional[str] = None)->bool:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query="""
                        INSERT INTO bronze.fbref_lineups
                        (match_id, match_url, raw_lineups, scrape_run_id, scraped_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (match_url)
                        DO UPDATE SET
                            raw_lineups = EXCLUDED.raw_lineups,
                            match_id = EXCLUDED.match_id,
                            scrape_run_id = EXCLUDED.scrape_run_id,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING id
                        """
                    
                    cur.execute(query,(
                        match_id,
                        match_url,
                        Json(lineup_data),
                        scrape_run_id,
                        datetime.utcnow(),
                        datetime.utcnow()
                    ))
                    result = cur.fetchone()
                    logger.info(f"Saved FBref lineups (ID: {result[0]})")
            return True
        except Exception as e:
            logger.error(f"Failed to save FBref lineups: {e}")
            return False
                    
