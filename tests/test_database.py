import pytest
import psycopg2
import os



class TestDatabaseConnection:
    def test_connection(self,db_params):
        conn=psycopg2.connect(**db_params)
        assert conn is not None
        conn.close()
    
    def test_schemas_exist(self, db_params):
        conn=psycopg2.connect(**db_params)
        cur=conn.cursor()

        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name IN ('bronze', 'silver', 'gold', 'metrics')
            """)
        schemas = [row[0] for row in cur.fetchall()]
        assert 'bronze' in schemas
        assert 'silver' in schemas
        assert 'gold' in schemas
        assert 'metrics' in schemas
        cur.close()
        conn.close()

class TestBronzeLayer:
    
    def test_bronze_tables_exist(self, db_conn):
        cur=db_conn.cursor()
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'bronze'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]
        required_tables = ['understat_raw', 'fbref_raw', 'match_reference', 'scrape_runs']
        for table in required_tables:
            assert table in tables, f"Bronze table {table} not found"
        
        cur.close()
    
    def test_understat_raw_columns(self,db_conn):
        cur = db_conn.cursor()
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = 'understat_raw'
            ORDER BY ordinal_position
        """)
        columns={row[0]: row[1] for row in cur.fetchall()}
        required_columns = {
            'id': 'integer',
            'match_id': 'character varying',
            'match_url': 'text',
            'raw_shots': 'jsonb',
            'scrape_run_id': 'character varying',
            'scraped_at': 'timestamp without time zone'
        }
        
        for col_name, col_type in required_columns.items():
            assert col_name in columns,f"Column {col_name} not found"
        cur.close()


# class TestSilverLayer:
    
#     def test_silver_views_exist(self, db_conn):
#         cur = db_conn.cursor()
#         cur.execute("""
#         SELECT table_name
#         FROM information_schema.views
#         WHERE table_schema = 'silver'
#         ORDER BY table_name
#         """)
#         views = [row[0] for row in cur.fetchall()]
#         required_views = ['shot_events', 'match_summary']
#         for view in required_views:
#             assert view in views, f"Silver view {view} not found"
        
#         cur.close()
#     def test_shot_events_structure(self, db_conn):
#         cur = db_conn.cursor()

#         cur.execute("""
#             SELECT column_name
#             FROM information_schema.columns
#             WHERE table_schema = 'silver'
#               AND table_name = 'shot_events'
#             ORDER BY ordinal_position
#         """)
#         columns = [row[0] for row in cur.fetchall()]
#         required_columns = [
#             'match_id', 'match_url', 'match_date', 'season',
#             'home_team', 'away_team', 'player_name', 'minute',
#             'result', 'xg', 'x_coord', 'y_coord', 'shot_type',
#             'situation', 'home_away'
#         ]

#         for col in required_columns:
#             assert col in columns,f"Column {col} not found in shot_event"
        
#         cur.close()

    


# class TestMetricsLayer:
    
#     def test_metrics_views_exist(self, db_conn):
#         cur = db_conn.cursor()

#         cur.execute("""
#             SELECT table_name
#             FROM information_schema.views
#             WHERE table_schema = 'metrics'
#             ORDER BY table_name
#         """)

#         views = [row[0] for row in cur.fetchall()]
#         required_views = [
#             'arsenal_matches',
#             'arsenal_season_summary',
#             'player_advanced_stats',
#             'shot_quality_analysis'
#         ]

#         for view in required_views:
#             assert view in views, f"Metrics view {view} not found"

#         cur.close()

class TestDataIntegrity:
    def test_xg_values_in_range(self, db_conn):
        cur = db_conn.cursor()

        cur.execute("""
            SELECT COUNT(*)
            FROM silver.shot_events
            WHERE xg < 0 OR xg > 1
        """)
        cur.execute("""
        SELECT COUNT(*)
            FROM silver.shot_events
            WHERE xg < 0 OR xg > 1
        """)
        invalid_xg = cur.fetchone()[0]
        assert invalid_xg == 0, f"Found {invalid_xg} shots with invalid xG"

        cur.close()
    
    def test_coordinates_in_range(self, db_conn):
        cur=db_conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM silver.shot_events
            WHERE x_coord < 0 OR x_coord > 1
               OR y_coord < 0 OR y_coord > 1
        """)
        invalid_coords = cur.fetchone()[0]
        assert invalid_coords == 0, f"Found {invalid_coords} shots with invalid coordinates"

        cur.close()
    
    def test_no_null_player_names(self, db_conn):
        cur=db_conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM silver.shot_events
            WHERE player_name IS NULL OR player_name = ''
        """)
        null_players = cur.fetchone()[0]
        assert null_players == 0, f"Found {null_players} shots with null player names"

        cur.close()
