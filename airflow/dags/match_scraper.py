from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import uuid
import logging

sys.path.insert(0, '/opt/airflow/scrapers')

from playwright_scraper import UnderstatPlaywrightScraper
from db_loader import DatabaseLoader

logger=logging.getLogger(__name__)

MONITORED_TEAMS = ['Arsenal', 'Manchester_United', 'Manchester_City', 'Liverpool']

def get_current_season():
    now=datetime.now()
    return str(now.year) if now.month >= 8 else str(now.year - 1)

def convert_season_display(season_year):
    year=int(season_year)
    return f"{year}-{str(year + 1)[-2:]}"


def  check_and_scrape_team_matches(team_name, **context):
    scraper = UnderstatPlaywrightScraper()
    loader = DatabaseLoader()

    season = get_current_season()
    season_display=convert_season_display(season)
    team_display=team_name.replace('_', ' ')

    logger.info(f"Checking {team_display} matches for {season_display}")
    try:
        fixtures=scraper.scrape_season_fixtures(season,team_name)
        if not fixtures:
            return {"team":team_display,"status":"no_fixtures","scraped":0}
        
        played_matches=[f for f in fixtures if f.get('is_result',False)]

        if not played_matches:
              return {"team": team_display, "status": "no_completed", "scraped": 0}
        existing_urls=set()
        with loader.get_connection() as conn:
            with conn.cursor()as cur:
                cur.execute("SELECT DISTINCT match_url FROM bronze.understat_raw WHERE team_name = %s", (team_display,))
                existing_urls = set(row[0] for row in cur.fetchall())
        new_matches = [m for m in played_matches if m.get('match_url') not in existing_urls]

        if not new_matches:
            logger.info(f"ALL {team_display} matches up to date!")
            return {"team":team_display,"status":"up_to_date","scraped":0}
        scraped_count=0
        for match in new_matches:
            home,away=match.get('home_team',''),match.get('away_team','')
            date,match_url=match.get('match_date',''),match.get('match_url','')

            try:
                match_data = scraper.scrape_match_shots(match_url, home_team=home, away_team=away, match_date=date)
                if not match_data or not match_data.get('shots'):
                    continue
                run_id=f"smart_{team_name}_{uuid.uuid4().hex[:8]}"
                match_id=str(match_data.get('match_id',match.get('match_id')))
                loader.create_scrape_run(run_id, match_id, 'understat', None)
                loader.save_match_reference(match_url,home,away,date,season_display,team_display)
                loader.save_understat_raw(match_id,match_data,match_url,run_id,team_name=team_display)
                loader.update_scrape_run(run_id, 'success', len(match_data['shots']))

                logger.info(f" {home} vs {away}: {len(match_data['shots'])} shots")
                scraped_count+=1
            except Exception as e:
                logger.error(f"Error: {e}")
                continue
        return {"team":team_display,"status":"success","scraped":scraped_count}
    
    except Exception as e:
        return {"team":team_display,"status":"error","error":str(e)}
    


def scrape_arsenal(**context):
    return check_and_scrape_team_matches('Arsenal',**context)

def  scrape_manchester_united(**context):
    return check_and_scrape_team_matches('Manchester_United',**context)

def scrape_manchester_city(**context):
    return check_and_scrape_team_matches('Manchester_City',**context)
def scrape_liverpool(**context):
    return check_and_scrape_team_matches('Liverpool',**context)

def summarize(**context):
    ti=context['task_instance']
    arsenal = ti.xcom_pull(task_ids='scrape_arsenal') or {}
    manutd = ti.xcom_pull(task_ids='scrape_manchester_united') or {}
    mancity = ti.xcom_pull(task_ids='scrape_manchester_city') or {}
    liverpool = ti.xcom_pull(task_ids='scrape_liverpool') or {}

    total = (arsenal.get('scraped', 0) + manutd.get('scraped', 0) + 
             mancity.get('scraped', 0) + liverpool.get('scraped', 0))
    logger.info(f"Total scraped: {total}")
    return {"total": total}

default_args={
    'owner':'analytics',
    'depends_on_past':False,
    'retries':2,
    'retry_delay':timedelta(minutes=10),
}
with DAG(
    'smart_match_scraper',
    default_args=default_args,
    description='Multi-team  scraper for Arsenal, Man United, Man City, Liverpool - runs every 2 hours',
    schedule_interval='0 */2 * * *',
    start_date=datetime(2026,4,15),
    catchup=False,
    tags=['arsenal', 'manchester_united', 'manchester_city', 'liverpool', 'multi-team'],

) as dag:
    scrape_arsenal_task = PythonOperator(task_id='scrape_arsenal', python_callable=scrape_arsenal)
    scrape_manutd_task = PythonOperator(task_id='scrape_manchester_united', python_callable=scrape_manchester_united)
    scrape_mancity_task = PythonOperator(task_id='scrape_manchester_city', python_callable=scrape_manchester_city)
    scrape_liverpool_task = PythonOperator(task_id='scrape_liverpool', python_callable=scrape_liverpool)
    summarize_task = PythonOperator(task_id='summarize', python_callable=summarize)

    [scrape_arsenal_task, scrape_manutd_task, scrape_mancity_task, scrape_liverpool_task] >> summarize_task

