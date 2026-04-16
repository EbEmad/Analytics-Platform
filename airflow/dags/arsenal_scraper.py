import sys
sys.path.insert(0, '/opt/airflow/scrapers')
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from playwright_scraper import UnderstatPlaywrightScraper
from fbref_scraper import FBrefScraper
from db_loader import DatabaseLoader
from airflow import DAG
import uuid
import logging
import time


logger=logging.getLogger(__name__)

def get_current_season():
    now = datetime.now()
    return str(now.year) if now.month >= 8 else str(now.year - 1)

def scrape_arsenal(**context):
    u_scraper=UnderstatPlaywrightScraper()
    f_scraper=FBrefScraper()
    loader=DatabaseLoader()

    season=get_current_season()
    logger.info(f"Starting comprehensive scrape for Arsenal - Season {season}")

    try:
        u_fixtures = u_scraper.scrape_season_fixtures(season, 'Arsenal')
        u_played = {f['match_date']: f for f in u_fixtures if f.get('is_result')}
    except Exception as e:
        logger.error(f"Failed to fetch Understat fixtures: {e}")
        u_played = {}
    
    try:
        f_season = f"{season}-{int(season)+1}"
        f_fixtures = f_scraper.scrape_fixtures(f_season)
        f_played = {f['match_date']: f for f in f_fixtures if f.get('match_status') == 'finished'}

    except Exception as e:
        logger.error(f"Failed to fetch FBref fixtures: {e}")
        f_played={}
    
    existing_urls = set()
    with loader.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT match_url FROM bronze.understat_raw WHERE team_name = 'Arsenal'")
            existing_urls = set(row[0] for row in cur.fetchall())
    
    new_dates = [d for d in u_played if u_played[d]['match_url'] not in existing_urls]
    if not new_dates:
        logger.info("No new Arsenal matches found to scrape.")
        return {"scraped":0}
    
    scraped_count = 0
    for date in sorted(new_dates):
        u_match = u_played[date]
        f_match = f_played.get(date)

        home = u_match.get('home_team')
        away = u_match.get('away_team')

        match_id=u_match.get('match_id')
        logger.info(f"Processing match: {date} | {home} vs {away}")
        run_id = f"comp_{uuid.uuid4().hex[:8]}"

        try:
            logger.info(f"Scraping Understat shots for {match_id}...")
            u_data = u_scraper.scrape_match_shots(u_match['match_url'], home_team=home, away_team=away, match_date=date)
            if u_data:
                loader.create_scrape_run(run_id + "_u", match_id, 'understat', context['dag_run'].run_id)
                loader.save_understat_raw(match_id, u_data, u_match['match_url'], run_id + "_u", team_name='Arsenal')
                loader.update_scrape_run(run_id + "_u", 'success', len(u_data.get('shots', [])))
        except Exception as e:
            logger.error(f"Understat scrape failed for {date}: {e}")

        
        if f_match and f_match.get('match_report_url'):
            try:
                report_url = f_match['match_report_url']
                logger.info(f"Scraping FBref stats from {report_url}...")

                f_stats=f_scraper.scrape_match_stats(report_url)

                if f_stats:
                    loader.create_scrape_run(run_id + "_fs", match_id, 'fbref', context['dag_run'].run_id)
                    loader.save_fbref_raw(match_id, f_stats, report_url, run_id + "_fs")
                    loader.update_scrape_run(run_id + "_fs", 'success', 1)
                time.sleep(3)

                logger.info(f"Scraping FBref lineups...")
                f_lineups=f_scraper.scrape_match_lineups(report_url)

                if f_lineups:
                    loader.save_fbref_lineups(report_url, f_lineups, match_id=match_id, scrape_run_id=run_id + "_fl")
            except Exception as e:
                logger.error(f"FBref scrape failed for {date}: {e}")
        else:
            logger.warning(f"No FBref match report found for date {date}")

        scraped_count+=1
        time.sleep(2)
    
    return {"scraped":scraped_count}

default_args = {
    'owner': 'arsenal_analytics',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    'arsenal_comprehensive_match_scraper',
    default_args=default_args,
    description='Automated comprehensive scraper for Arsenal using Understat and FBRef',
    schedule_interval='0 */4 * * *',
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=['arsenal', 'comprehensive', 'understat', 'fbref'],
) as dag:
    scrape_task = PythonOperator(
        task_id='scrape_arsenal_all_sources',
        python_callable=scrape_arsenal,
        provide_context=True,
    )
    scrape_task




