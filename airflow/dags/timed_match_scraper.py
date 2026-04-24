from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import sys
import uuid
import logging

sys.path.insert(0, '/opt/airflow/scrapers')

from playwright_scraper import UnderstatPlaywrightScraper
from db_loader import DatabaseLoader

logger = logging.getLogger(__name__)

MONITORED_TEAMS = {
    'Arsenal': 'Arsenal',
    'Manchester_United': 'Manchester United',
    'Manchester_City': 'Manchester City',
    'Liverpool': 'Liverpool'
}

def get_current_season():
    now = datetime.now()
    return str(now.year) if now.month >= 8 else str(now.year - 1)

def convert_season_display(season_year):
    year = int(season_year)
    return f"{year}-{str(year + 1)[-2:]}"

def check_for_recent_matches(**context):

    scraper = UnderstatPlaywrightScraper()
    season = get_current_season()
    matches_to_scrape = []

    for team_key, team_display in MONITORED_TEAMS.items():
        logger.info(f"Checking {team_display} for recent completed matches")

        try:
            fixtures = scraper.scrape_season_fixtures(season, team_key)
            played = [f for f in fixtures if f.get('is_result', False)]

            loader=DatabaseLoader()
            existing_urls=set()

            with loader.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT match_url FROM bronze.understat_raw WHERE team_name = %s", (team_display,))
                    existing_urls = set(row[0] for row in cur.fetchall())
            
            new_matches = [m for m in played if m.get('match_url') not in existing_urls]

            for match in new_matches:
                match['team_key'] = team_key
                match['team_display'] = team_display
                matches_to_scrape.append(match)
                logger.info(f"Found new match: {match.get('home_team')} vs {match.get('away_team')} ({team_display})")
        except Exception as e:
            logger.error(f"Error checking {team_display}: {e}")
            continue
    
    if matches_to_scrape:
        context['task_instance'].xcom_push(key='matches_to_scrape', value=matches_to_scrape)
        return 'scrape_matches'
    else:
        logger.info("No new matches to scrape - all up to date!")
        return 'skip_scraping'

def scrape_new_matches(**context):
    ti = context['task_instance']
    matches = ti.xcom_pull(key='matches_to_scrape', task_ids='check_recent_matches') or []

    if not matches:
        return {"scraped": 0}
    
    scraper = UnderstatPlaywrightScraper()
    loader = DatabaseLoader()
    season_display = convert_season_display(get_current_season())

    scraped = 0
    errors = 0

    for match in matches:
        home = match.get('home_team', '')
        away = match.get('away_team', '')
        date = match.get('match_date', '')
        match_url = match.get('match_url', '')
        team_display = match.get('team_display', '')
        team_key = match.get('team_key', '')
        
        logger.info(f"Scraping: {home} vs {away} on {date} ({team_display})")
        
        try:
            match_data = scraper.scrape_match_shots(match_url, home_team=home, away_team=away, match_date=date)
            
            if not match_data or not match_data.get('shots'):
                logger.warning(f"No data for {home} vs {away}")
                errors += 1
                continue
            
            run_id = f"epl_timed_{team_key}_{uuid.uuid4().hex[:8]}"
            match_id = str(match_data.get('match_id', match.get('match_id')))
            
            loader.create_scrape_run(run_id, match_id, 'understat', None)
            loader.save_match_reference(match_url, home, away, date, season_display, team_display)
            loader.save_understat_raw(match_id, match_data, match_url, run_id, team_name=team_display)
            loader.update_scrape_run(run_id, 'success', len(match_data['shots']))
            
            logger.info(f"✓ {home} vs {away}: {len(match_data['shots'])} shots, xG {match_data.get('home_xg', 0):.2f}-{match_data.get('away_xg', 0):.2f}")
            scraped += 1
            
        except Exception as e:
            logger.error(f"Error scraping {home} vs {away}: {e}")
            errors += 1
    
    logger.info(f"Completed: {scraped} matches scraped, {errors} errors")
    return {"scraped": scraped, "errors": errors}

default_args = {
    'owner': 'epl_analytics',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'epl_timed_match_scraper',
    default_args=default_args,
    description='Scrapes EPL matches 2 hours after typical kickoff times - Arsenal, Man United, Man City, Liverpool',
    schedule_interval='30 17,19,22,0 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['epl', 'arsenal', 'manchester_united', 'manchester_city', 'liverpool', 'timed', 'production'],
)as dag:
    dag.doc_md = """
    ## EPL Timed Match Scraper
    
    Runs at times aligned with typical EPL match completions:
    - 17:30 UTC - After early kickoffs (12:30 UK)
    - 19:30 UTC - After standard 3pm kickoffs
    - 22:00 UTC - After evening kickoffs (5:30pm UK)
    - 00:30 UTC - After late evening games (8pm UK)
    
    **Teams**: Arsenal, Manchester United, Manchester City, Liverpool
    """

    check_task = BranchPythonOperator(
        task_id='check_recent_matches',
        python_callable=check_for_recent_matches,
    )
    
    scrape_task = PythonOperator(
        task_id='scrape_matches',
        python_callable=scrape_new_matches,
    )
    
    skip_task = EmptyOperator(
        task_id='skip_scraping',
    )
    
    done_task = EmptyOperator(
        task_id='done',
        trigger_rule='none_failed_min_one_success',
    )
    
    check_task >> [scrape_task, skip_task] >> done_task
    
