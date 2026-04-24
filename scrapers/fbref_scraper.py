import logging
import time
import random
from typing import Dict, List, Optional, Any
from datetime import datetime
from contextlib import contextmanager
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from pyvirtualdisplay import Display

from config import config
from utils import (
    safe_extract_text,
    safe_extract_int,
    safe_extract_float,
    clean_player_name,
    generate_match_id,
    ScraperException,
    DataValidationException
)

logger = logging.getLogger(__name__)


class FBrefScraper:
    """Scraper for FBref using headed Playwright + Xvfb virtual display"""

    def __init__(self):
        self.base_url = config.FBREF_BASE_URL
        self.arsenal_id = config.ARSENAL_FBREF_ID
        self.user_agent = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

    @contextmanager
    def _get_browser_page(self):
        """Get a headed browser page running inside Xvfb virtual display"""
        display = Display(visible=0, size=(1920, 1080))
        display.start()
        logger.info("Virtual display started")

        pw = sync_playwright().start()
        browser = pw.chromium.launch(
            headless=False,  
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--window-size=1920,1080",
            ]
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=self.user_agent,
        )
        
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            window.chrome = { runtime: {} };
        """)
        page = context.new_page()
        try:
            yield page
        finally:
            page.close()
            context.close()
            browser.close()
            pw.stop()
            display.stop()
            logger.info("Virtual display stopped")

    def _fetch_page(self, page, url: str) -> BeautifulSoup:
        """Navigate to a URL and return parsed content, handling Cloudflare"""
        logger.info(f"Navigating to: {url}")

        delay = random.uniform(3, 6)
        time.sleep(delay)

        page.goto(url, wait_until='domcontentloaded', timeout=60000)
        page.wait_for_timeout(2000)
        title = page.title()
        logger.info(f"Initial page title: {title}")

        if "Just a moment" in title:
            logger.info("Cloudflare challenge detected, waiting for resolution...")
            try:
                
                page.wait_for_function(
                    "() => !document.title.includes('Just a moment')",
                    timeout=30000
                )
                logger.info(f"Challenge resolved! New title: {page.title()}")
            except Exception:
                logger.warning("Challenge not resolved in 30s, waiting more...")
                page.wait_for_timeout(10000)

        page.wait_for_timeout(2000)
        content = page.content()
        title = page.title()

        if "Just a moment" in title:
            logger.error(f"Still on Cloudflare page. Title: {title}")
            raise ScraperException(f"FBref blocked by Cloudflare for {url}")

        logger.info(f"Page loaded successfully. Title: {title}, Length: {len(content)}")
        return BeautifulSoup(content, 'lxml')



    def scrape_fixtures(self, season: str = "2024-2025") -> List[Dict[str, Any]]:
        """Scrape Arsenal's fixtures for the season"""
        url = f"{self.base_url}/en/squads/{self.arsenal_id}/{season}/Arsenal-Stats"

        with self._get_browser_page() as page:
            soup = self._fetch_page(page, url)

        fixtures = []

        possible_ids = ['matchlogs_for', 'matchlogs_for_all', 'matchlogs_for_c9', 'matchlogs_for_combined']
        table = None
        for tid in possible_ids:
            table = soup.find('table', {'id': tid})
            if table:
                logger.info(f"Found fixtures table: {tid}")
                break

        if not table:
            all_tables = soup.find_all('table')
            logger.warning(f"No table by ID. Found: {[t.get('id') for t in all_tables]}")
            for t in all_tables:
                text = t.get_text().lower()
                if 'opponent' in text and ('comp' in text or 'date' in text):
                    table = t
                    logger.info("Found table by content fallback")
                    break

        if not table:
            logger.error("Could not find fixtures table")
            return fixtures

        tbody = table.find('tbody')
        if not tbody:
            return fixtures

        for row in tbody.find_all('tr'):
            if row.get('class') and 'thead' in row.get('class'):
                continue
            try:
                fixture = self._parse_fixture_row(row, season)
                if fixture:
                    fixtures.append(fixture)
            except Exception as e:
                logger.debug(f"Row parse error: {e}")
                continue

        logger.info(f"Scraped {len(fixtures)} fixtures for Arsenal {season}")
        return fixtures

    def _parse_fixture_row(self, row, season: str) -> Optional[Dict[str, Any]]:
        """Parse a single fixture row"""
        cells = row.find_all(['th', 'td'])
        if len(cells) < 10:
            return None

        date_str = safe_extract_text(cells[1], '')
        time_str = safe_extract_text(cells[2], '')
        competition = safe_extract_text(cells[3], '')
        venue = safe_extract_text(cells[6], '')
        result = safe_extract_text(cells[7], '')
        opponent = safe_extract_text(cells[10], 'a', '')
        gf_str = safe_extract_text(cells[8], '')
        ga_str = safe_extract_text(cells[9], '')

        try:
            match_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            return None

        kickoff_time = None
        if time_str:
            try:
                kickoff_time = datetime.strptime(time_str, '%H:%M').time()
            except ValueError:
                pass

        match_status = 'finished' if result else 'scheduled'
        is_home = venue.lower() == 'home'
        home_team = 'Arsenal' if is_home else opponent
        away_team = opponent if is_home else 'Arsenal'
        home_score = safe_extract_int(gf_str) if is_home else safe_extract_int(ga_str)
        away_score = safe_extract_int(ga_str) if is_home else safe_extract_int(gf_str)

        match_report_link = None
        if len(cells) > 11:
            report_cell = cells[11]
            link = report_cell.find('a', string='Match Report') or report_cell.find('a', text='Match Report')
            if link and link.get('href'):
                match_report_link = self.base_url + link['href']

        return {
            'match_id': generate_match_id(home_team, away_team, str(match_date)),
            'season': season,
            'competition': competition,
            'match_date': str(match_date),
            'kickoff_time': str(kickoff_time) if kickoff_time else None,
            'home_team': home_team,
            'away_team': away_team,
            'home_score': home_score if match_status == 'finished' else None,
            'away_score': away_score if match_status == 'finished' else None,
            'match_status': match_status,
            'venue': venue,
            'match_report_url': match_report_link,
        }

   

    def scrape_match_stats(self, match_report_url: str) -> Dict[str, Any]:
        """Scrape detailed match statistics"""
        logger.info(f"Scraping match stats: {match_report_url}")
        with self._get_browser_page() as page:
            soup = self._fetch_page(page, match_report_url)

        match_data = {
            'match_url': match_report_url,
            'scraped_at': datetime.utcnow().isoformat(),
            'match_metadata': self._extract_match_metadata(soup),
            'team_stats': self._extract_team_stats(soup),
            'player_stats': self._extract_player_stats(soup),
        }
        self._validate_match_data(match_data)
        return match_data

    def _extract_match_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        metadata = {}
        scorebox = soup.find('div', {'class': 'scorebox'})
        if not scorebox:
            return metadata
        teams = scorebox.find_all('div', recursive=False)
        if len(teams) >= 2:
            h = teams[0]
            metadata['home_team'] = h.find('strong').get_text(strip=True) if h.find('strong') else None
            hs = h.find('div', {'class': 'score'})
            metadata['home_score'] = safe_extract_int(hs.get_text(strip=True)) if hs else None
            a = teams[1]
            metadata['away_team'] = a.find('strong').get_text(strip=True) if a.find('strong') else None
            aws = a.find('div', {'class': 'score'})
            metadata['away_score'] = safe_extract_int(aws.get_text(strip=True)) if aws else None
        meta_div = scorebox.find('div', {'class': 'scorebox_meta'})
        if meta_div:
            for d in meta_div.find_all('div'):
                t = d.get_text(strip=True)
                if 'Venue:' in t:
                    metadata['venue'] = t.replace('Venue:', '').strip()
                elif 'Attendance:' in t:
                    metadata['attendance'] = safe_extract_int(t.replace('Attendance:', '').strip())
                elif 'Referee:' in t:
                    metadata['referee'] = t.replace('Referee:', '').strip()
        return metadata

    def _extract_team_stats(self, soup: BeautifulSoup) -> Dict[str, Any]:
        team_stats = {'home': {}, 'away': {}}
        div = soup.find('div', {'id': 'team_stats'})
        if not div:
            return team_stats
        for row in div.find_all('tr'):
            hdr = row.find('th')
            if not hdr:
                continue
            name = hdr.get_text(strip=True).lower()
            cells = row.find_all('td')
            if len(cells) >= 2:
                hv, av = cells[0].get_text(strip=True), cells[1].get_text(strip=True)
                if 'possession' in name:
                    team_stats['home']['possession'] = safe_extract_float(hv.replace('%', ''))
                    team_stats['away']['possession'] = safe_extract_float(av.replace('%', ''))
                elif 'xg' in name and 'xa' not in name:
                    team_stats['home']['xg'] = safe_extract_float(hv)
                    team_stats['away']['xg'] = safe_extract_float(av)
                elif 'shots' in name and 'on target' not in name:
                    team_stats['home']['shots'] = safe_extract_int(hv)
                    team_stats['away']['shots'] = safe_extract_int(av)
                elif 'on target' in name:
                    team_stats['home']['shots_on_target'] = safe_extract_int(hv)
                    team_stats['away']['shots_on_target'] = safe_extract_int(av)
        return team_stats

    def _extract_player_stats(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        player_map = {}
        prefixes = [('stats_', 'summary'), ('passing_', 'passing'), ('defense_', 'defense'),
                     ('possession_', 'possession'), ('gca_', 'gca')]
        for prefix, _ in prefixes:
            for table in soup.find_all('table', id=lambda x: x and x.startswith(prefix)):
                tid = table.get('id', '')
                team = 'home' if 'home' in tid else 'away'
                tbody = table.find('tbody')
                if not tbody:
                    continue
                for row in tbody.find_all('tr'):
                    if 'thead' in row.get('class', []):
                        continue
                    pc = row.find('th', {'data-stat': 'player'})
                    if not pc:
                        continue
                    pname = clean_player_name(pc.get_text(strip=True))
                    key = f"{pname}_{team}"
                    if key not in player_map:
                        player_map[key] = {'player_name': pname, 'team': team}
                    for cell in row.find_all(['th', 'td']):
                        stat = cell.get('data-stat')
                        if stat and stat != 'player':
                            player_map[key][stat] = cell.get_text(strip=True)
        return list(player_map.values())

    def _validate_match_data(self, data: Dict[str, Any]) -> None:
        meta = data.get('match_metadata', {})
        if not meta.get('home_team') or not meta.get('away_team'):
            raise DataValidationException("Missing team names")

    

    def scrape_match_lineups(self, match_report_url: str) -> Dict[str, Any]:
        logger.info(f"Scraping lineups: {match_report_url}")
        with self._get_browser_page() as page:
            soup = self._fetch_page(page, match_report_url)

        meta = self._extract_match_metadata(soup)
        tables = soup.find_all('table', {'class': 'lineup'})
        return {
            'match_url': match_report_url,
            'scraped_at': datetime.utcnow().isoformat(),
            'home_team': meta.get('home_team'),
            'away_team': meta.get('away_team'),
            'home_lineup': self._parse_lineup_table(tables[0], 'home') if len(tables) >= 1 else [],
            'away_lineup': self._parse_lineup_table(tables[1], 'away') if len(tables) >= 2 else [],
        }

    def _parse_lineup_table(self, table, side: str) -> List[Dict[str, str]]:
        lineup = []
        tbody = table.find('tbody')
        if not tbody:
            return lineup
        for row in tbody.find_all('tr'):
            try:
                pc = row.find('th', {'data-stat': 'player'})
                if not pc:
                    continue
                name = clean_player_name(pc.get_text(strip=True))
                pos_cell = row.find('td', {'data-stat': 'position'})
                pos = pos_cell.get_text(strip=True) if pos_cell else 'Unknown'
                num_cell = row.find('th', {'data-stat': 'jersey_number'})
                lineup.append({
                    'player_name': name,
                    'position': pos,
                    'position_category': self._normalize_position(pos),
                    'jersey_number': num_cell.get_text(strip=True) if num_cell else '',
                    'team_side': side,
                })
            except Exception:
                continue
        return lineup


    def scrape_match_logs(self, season: str = "2024-2025", log_type: str = "passing") -> List[Dict[str, Any]]:
        url = f"{self.base_url}/en/squads/{self.arsenal_id}/{season}/matchlogs/c9/{log_type}/Arsenal-Match-Logs-Premier-League"
        with self._get_browser_page() as page:
            soup = self._fetch_page(page, url)

        logs = []
        table = soup.find('table', {'id': f'matchlogs_{log_type}'})
        if not table or not table.find('tbody'):
            return logs
        for row in table.find('tbody').find_all('tr'):
            if row.get('class') and 'thead' in row.get('class'):
                continue
            try:
                cells = row.find_all(['th', 'td'])
                if len(cells) < 5:
                    continue
                dc = cells[0]
                logs.append({
                    'match_date': safe_extract_text(dc.find('a')) if dc.find('a') else safe_extract_text(dc),
                    'opponent': safe_extract_text(cells[1].find('a')) if cells[1].find('a') else safe_extract_text(cells[1]),
                    'venue': safe_extract_text(cells[2]).upper(),
                    'result': safe_extract_text(cells[3]),
                    'score': safe_extract_text(cells[4]),
                    'match_url': f"{self.base_url}{dc.find('a')['href']}" if dc.find('a') else None,
                    'log_type': log_type,
                    'season': season,
                })
            except Exception:
                continue
        return logs

    def _normalize_position(self, pos: str) -> str:
        pos = pos.upper()
        if 'GK' in pos: return 'GK'
        if any(x in pos for x in ['CB', 'LB', 'RB', 'WB', 'DF', 'FB']): return 'DEF'
        if any(x in pos for x in ['CM', 'DM', 'AM', 'MF', 'CDM', 'CAM']): return 'MID'
        if any(x in pos for x in ['FW', 'LW', 'RW', 'CF', 'ST', 'W', 'F']): return 'FWD'
        return 'UNKNOWN'
