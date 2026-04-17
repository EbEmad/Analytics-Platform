from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
import sys
import logging

sys.path.insert(0, '/opt/airflow/scrapers')

from playwright_scraper import UnderstatPlaywrightScraper

logger=logging.getLogger(__name__)


MONITORED_TEAMS = {
    'Arsenal': 'Arsenal',
    'Manchester_United': 'Manchester United',
    'Manchester_City': 'Manchester City',
    'Liverpool': 'Liverpool'
}

def get_current_season():
    now = datetime.now()
    return str(now.year) if now.month >= 8 else str(now.year - 1)

def fetch_upcoming_matches(**context):
    scraper=UnderstatPlaywrightScraper()
    season=get_current_season()

    all_upcoming=[]

    for team_key, team_display in MONITORED_TEAMS:
        logger.info(f"Fetching fixtures for {team_display}")

        try:
            fixtures=scraper.scrape_season_fixtures(season, team_key)
            upcoming = [f for f in fixtures if not f.get('is_result', False)]

            for match in upcoming:
                match['team_key']=team_key
                match['team_display']=team_display
                all_upcoming.append(match)
            logger.info(f"Found {len(upcoming)} upcoming matches for {team_display}")
        except Exception as e:
            logger.error(f"Error fetching {team_display} fixtures: {e}")
            continue
    unique_matches={}
    for match in all_upcoming:
        match_id=match.get('match_id')
        if match_id and match_id not in unique_matches:
            unique_matches[match_id]=match
    
    upcoming_list=list(unique_matches.values())
    upcoming_list.sort(key=lambda x: x.get('match_date', ''))

    logger.info(f"Total unique upcoming matches: {len(upcoming_list)}")

    context['task_instance'].xcom_push(key='upcoming_matches',value=upcoming_list[:30])

    return upcoming_list


def schedule_scrape_runs(**context):
    ti=context['task_instance']
    upcoming = ti.xcom_pull(key='upcoming_matches', task_ids='fetch_upcoming') or []

    if not upcoming:
        logger.warning("No upcoming matches to schedule")
        return {"scheduled": 0}
    scheduled_count=0
    for match in upcoming[:20]:
        match_date = match.get('match_date', '')
        match_datetime_str = match.get('date', '')
        home = match.get('home_team', '')
        away = match.get('away_team', '')
        team = match.get('team_display', '')

        try:
            if match_datetime_str and len(match_datetime_str) > 10:
                match_dt = datetime.strptime(match_datetime_str[:19], '%Y-%m-%d %H:%M:%S')
            elif match_date:
                match_dt = datetime.strptime(match_date, '%Y-%m-%d')
                match_dt = match_dt.replace(hour=15, minute=0)
            else:
                continue
        except Exception as e:
            logger.warning(f"Could not parse date for {home} vs {away}: {e}")
            continue

        scrape_time=match_dt+timedelta(hours=4)
        if scrape_time<= datetime.now():
            continue

        logger.info(f"Match: {home} vs {away} ({team})")
        logger.info(f"  Kickoff: {match_dt.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"  Scrape scheduled: {scrape_time.strftime('%Y-%m-%d %H:%M')}")
        
        scheduled_count += 1
    
    logger.info(f"Scheduled {scheduled_count} scraping runs")
    return {"scheduled": scheduled_count}

def print_schedule(**context):
    ti = context['task_instance']
    upcoming = ti.xcom_pull(key='upcoming_matches', task_ids='fetch_upcoming') or []

    logger.info("=" * 80)
    logger.info("UPCOMING EPL MATCHES - SCRAPE SCHEDULE")
    logger.info("Teams: Arsenal, Man United, Man City, Liverpool")
    logger.info("=" * 80)

    for i, match in enumerate(upcoming[:20], 1):
        home = match.get('home_team', '')
        away = match.get('away_team', '')
        date = match.get('match_date', '')
        team = match.get('team_display', '')
    
    try:
        if match.get('date') and len(match.get('date',''))>10:
            kickoff=datetime(match['date'][:19],'%Y-%m-%d %H:%M:%S')
        else:
            kickoff = datetime.strptime(date, '%Y-%m-%d').replace(hour=15, minute=0)
        scrape_at=kickoff+timedelta(hours=4)
        scrape_str=scrape_at.strftime('%Y-%m-%d %H:%M')
    except:
        scrape_str = "TBD"
    logger.info(f"{i:2}. {date} | {home:20} vs {away:20} | Team: {team:15} | Scrape: {scrape_str}")

    return {"printed": len(upcoming[:20])}

default_args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'match_scheduler',
    default_args=default_args,
    description='Fetches EPL match schedule for Arsenal, Man United, Man City, Liverpool',
    schedule_interval='0 6 * * *',
    start_date=datetime(2026,1,6),
    catchup=False,
    tags=['epl', 'scheduler', 'arsenal', 'manchester_united', 'manchester_city', 'liverpool'],

)as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_upcoming',
        python_callable=fetch_upcoming_matches,
    )
    
    schedule_task = PythonOperator(
        task_id='schedule_runs',
        python_callable=schedule_scrape_runs,
    )
    
    print_task = PythonOperator(
        task_id='print_schedule',
        python_callable=print_schedule,
    )
    
    fetch_task >> schedule_task >> print_task








