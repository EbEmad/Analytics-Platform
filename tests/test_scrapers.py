from scrapers import UnderstatPlaywrightScraper
import pytest
import os
import sys

class TestUnderstatScraper:
    @pytest.fixture
    def scraper(self):
        return UnderstatPlaywrightScraper(headless=True)
    

    def test_scraper_initialization(self, scraper):
        assert self.scraper is not None
        assert hasattr(self.scraper,'scrape_match_shots')
        assert hasattr(self.scraper,'scrape_season_fixtures')
    
    def test_fixture_data_structure(self, scraper):
        fixtures = scraper.scrape_season_fixtures('2024')

        assert len(fixtures)>0 ,"Should return at least one fixture"

        fixture=fixtures[0]
        required_keys = ['match_url', 'match_date', 'home_team', 'away_team', 'is_result']
        for key in required_keys:
            assert key in fixture,f"Fixture missing required key: {key}"
        

        assert isinstance(fixture['match_url'], str)
        assert isinstance(fixture['match_date'], str)
        assert isinstance(fixture['home_team'], str)
        assert isinstance(fixture['away_team'], str)
        assert isinstance(fixture['is_result'], bool)
    

    def test_match_data_structure(self, scraper):
        fixtures = scraper.scrape_season_fixtures('2024')
        played_matches = [f for f in fixtures if f['is_result']]

        assert len(played_matches)>0,"Should have played matches"

        match_url=played_matches[0]['match_url']
        match_data = scraper.scrape_match_shots(match_url)

        required_keys = ['match_id', 'match_url', 'home_team', 'away_team',
                        'home_goals', 'away_goals', 'home_xg', 'away_xg',
                        'match_date', 'shots']
        
        for key in required_keys:
            assert key in match_data, f"Match data missing required key: {key}"
        
        assert isinstance(match_data['shots'], list)
        assert len(match_data['shots']) > 0, "Should have at least one shot"

        shot = match_data['shots'][0]
        shot_keys = ['player_name', 'minute', 'result', 'x_coord', 'y_coord',
                    'xg', 'situation', 'shot_type', 'h_a']

        for key in shot_keys:
            assert key in shot, f"Shot missing required key: {key}"
    def test_xg_values_valid(self, scraper):
        fixtures = scraper.scrape_season_fixtures('2024')
        played_matches = [f for f in fixtures if f['is_result']]

        match_url = played_matches[0]['match_url']
        match_data = scraper.scrape_match_shots(match_url)

        # Check match-level xG
        assert 0 <= match_data['home_xg'] <= 10, "Home xG should be reasonable"
        assert 0 <= match_data['away_xg'] <= 10, "Away xG should be reasonable"

        # Check shot-level xG
        for shot in match_data['shots']:
            assert 0 <= shot['xg'] <= 1, f"Shot xG should be 0-1, got {shot['xg']}"
    def test_coordinates_valid(self, scraper):
        """Test shot coordinates are within valid range"""
        fixtures = scraper.scrape_season_fixtures('2024')
        played_matches = [f for f in fixtures if f['is_result']]

        match_url = played_matches[0]['match_url']
        match_data = scraper.scrape_match_shots(match_url)

        for shot in match_data['shots']:
            assert 0 <= shot['x_coord'] <= 1, f"X coord should be 0-1, got {shot['x_coord']}"
            assert 0 <= shot['y_coord'] <= 1, f"Y coord should be 0-1, got {shot['y_coord']}"

    def test_match_id_format(self, scraper):
        """Test match ID follows expected format"""
        fixtures = scraper.scrape_season_fixtures('2024')
        played_matches = [f for f in fixtures if f['is_result']]

        match_url = played_matches[0]['match_url']
        match_data = scraper.scrape_match_shots(match_url)

        # Match ID should be extracted from URL
        assert match_data['match_id'].isdigit() or '-' in match_data['match_id']
        assert len(match_data['match_id']) > 3
    
class TestDataQuality:
    """Test data quality and consistency"""

    @pytest.fixture
    def scraper(self):
        return UnderstatPlaywrightScraper(headless=True)

    def test_goals_match_result_shots(self, scraper):
        """Test that goals in metadata match Goal results in shots"""
        fixtures = scraper.scrape_season_fixtures('2024')
        played_matches = [f for f in fixtures if f['is_result']][:3]  # Test 3 matches

        for fixture in played_matches:
            match_data = scraper.scrape_match_shots(fixture['match_url'])

            # Count goals from shots
            home_goal_shots = [s for s in match_data['shots']
                             if s['h_a'] == 'h' and s['result'] == 'Goal']
            away_goal_shots = [s for s in match_data['shots']
                             if s['h_a'] == 'a' and s['result'] == 'Goal']

            # Verify they match metadata
            assert len(home_goal_shots) == match_data['home_goals'], \
                f"Home goals mismatch for {fixture['match_url']}"
            assert len(away_goal_shots) == match_data['away_goals'], \
                f"Away goals mismatch for {fixture['match_url']}"

    def test_player_names_not_empty(self, scraper):
        """Test that player names are populated"""
        fixtures = scraper.scrape_season_fixtures('2024')
        played_matches = [f for f in fixtures if f['is_result']]

        match_url = played_matches[0]['match_url']
        match_data = scraper.scrape_match_shots(match_url)

        for shot in match_data['shots']:
            assert shot['player_name'], "Player name should not be empty"
            assert len(shot['player_name']) > 1, "Player name should be valid"



