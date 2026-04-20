from .db_connector import DatabaseConnector
from typing import Dict, Any, Optional, Tuple
import re

class QuestionHandler:
    TEAM_ALIASES={
        'arsenal': 'Arsenal',
        'gunners': 'Arsenal',
        'afc': 'Arsenal',
        'liverpool': 'Liverpool',
        'reds': 'Liverpool',
        'lfc': 'Liverpool',
        'manchester city': 'Manchester City',
        'man city': 'Manchester City',
        'city': 'Manchester City',
        'mcfc': 'Manchester City',
        'manchester united': 'Manchester United',
        'man united': 'Manchester United',
        'man utd': 'Manchester United',
        'united': 'Manchester United',
        'mufc': 'Manchester United',
    }

    AGGREGATE_PATTERNS=[
        (r'top\s*scorer', 'top_scorer'),
        (r'most\s*goals', 'top_scorer'),
        (r'leading\s*scorer', 'top_scorer'),
        (r'who\s*(has\s*)?scored?\s*(the\s*)?most', 'top_scorer'),
        (r'best\s*scorer', 'top_scorer'),
        (r'goal\s*scoring\s*leader', 'top_scorer'),
        (r'total\s*goals', 'team_goals'),
        (r'how\s*many\s*goals', 'team_goals'),
        (r'goals?\s*scored', 'team_goals'),
        (r'best\s*conversion', 'best_conversion'),
        (r'highest\s*conversion', 'best_conversion'),
        (r'most\s*shots', 'most_shots'),
        (r'season\s*stats', 'season_summary'),
        (r'season\s*summary', 'season_summary'),
        (r'how\s*(many|much)\s*(matches?|games?)', 'match_count'),
        (r'win\s*rate', 'win_rate'),
        (r'wins?\s*and\s*losses', 'win_rate'),
    ]

    SEASON_PATTERNS=[
        (r'202[3-9]-2[0-9]', None),  # Match season format like 2024-25
        (r'this\s*season', '2025-26'),
        (r'current\s*season', '2025-26'),
        (r'last\s*season', '2024-25'),
        (r'previous\s*season', '2024-25'),
    ]

    def __init__(self, db: DatabaseConnector):
        self.db = db
    
    def extract_team(self, question: str) -> Optional[str]:
        question_lower = question.lower()
        for alias, team in self.TEAM_ALIASES.items():
            if alias in question_lower:
                return team
        return None
    
    def extract_season(self, question: str) -> Optional[str]:
        question_lower = question.lower()

        for pattern, default_season in self.SEASON_PATTERNS:
            match=re.search(pattern,question_lower)
            if match:
                if default_season:
                    return default_season
                return match.group(0)
        return '2025-26'
    
    def classify_question(self, question: str) -> Tuple[Optional[str], Dict[str, Any]]:

        question_lower = question.lower()
        for pattern,q_type in self.AGGREGATE_PATTERNS:
            if re.search(pattern,question_lower):
                team=self.extract_team(question)
                season=self.extract_season(question)
                return q_type,{'team':team,'season':season}
        return None,{}

    def handle_aggregate_question(self, question: str) -> Optional[Dict[str, Any]]:
        q_type, params = self.classify_question(question)
        
        if not q_type:
            return None
        
        team = params.get('team')
        season = params.get('season', '2025-26')

        try:
            if q_type == 'top_scorer':
                return self._get_top_scorers(team, season)
            elif q_type == 'team_goals':
                return self._get_team_goals(team, season)
            elif q_type == 'best_conversion':
                return self._get_best_conversion(team, season)
            elif q_type == 'most_shots':
                return self._get_most_shots(team, season)
            elif q_type == 'season_summary':
                return self._get_season_summary(team, season)
            elif q_type == 'match_count':
                return self._get_match_count(team, season)
            elif q_type == 'win_rate':
                return self._get_win_rate(team, season)
        
        except Exception as e:
            return None
        
        return None
    
    def _get_top_scorers(self, team: Optional[str], season: str) -> Dict[str, Any]:

        query="""
        SELECT player_name, team, season, goals, total_shots, total_xg, 
               conversion_rate, matches_played
        FROM metrics.player_season_stats
        WHERE season = %s
        """

        params = [season]

        if team:
            query+=" AND team = %s"
            params.append(team)
        query+=" ORDER BY goals DESC LIMIT 10"
        
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        if not results:
            return {
                'type': 'aggregate',
                'query_type': 'top_scorer',
                'answer': f"No scoring data found for {team or 'any team'} in {season} season.",
                'data': [],
                'sources': []
            }
        
        team_str = team if team else "all teams"
        top = results[0]

        answer = f"**Top Scorers for {team_str} ({season} season):**\n\n"

        for i, player in enumerate(results[:5], 1):
            answer += f"{i}. **{player['player_name']}** ({player['team']}) - {player['goals']} goals\n"
            answer += f"   - Shots: {player['total_shots']}, xG: {float(player['total_xg']):.2f}, "
            answer += f"Conversion: {float(player['conversion_rate']):.1f}%\n\n"
        return {
            'type': 'aggregate',
            'query_type': 'top_scorer',
            'answer': answer,
            'data': results,
            'sources': [{'type': 'database', 'query': 'player_season_stats', 'season': season}]
        }
    

    def _get_team_goals(self, team: Optional[str], season: str) -> Dict[str, Any]:

        query="""
        SELECT team, SUM(goals) as total_goals, COUNT(DISTINCT player_name) as scorers
        FROM metrics.player_season_stats
        WHERE season = %s
        """

        params=[season]

        if team:
            query+=" AND team = %s"
            params.append(team)
        query+=" GROUP BY team ORDER BY total_goals DESC"

        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        if not results:
            return {
                'type': 'aggregate',
                'query_type': 'team_goals',
                'answer': f"No goal data found for {team or 'any team'} in {season} season.",
                'data': [],
                'sources': []
            }
        
        answer = f"**Total Goals ({season} season):**\n\n"
        for r in results:
            answer += f"- **{r['team']}**: {r['total_goals']} goals from {r['scorers']} different scorers\n"
        
        return {
            'type': 'aggregate',
            'query_type': 'team_goals',
            'answer': answer,
            'data': results,
            'sources': [{'type': 'database', 'query': 'player_season_stats', 'season': season}]
        }
    
    def _get_best_conversion(self, team: Optional[str], season: str) -> Dict[str, Any]:
        query = """
        SELECT player_name, team, goals, total_shots, conversion_rate
        FROM metrics.player_season_stats
        WHERE season = %s AND total_shots >= 10
        """
        params=[season]

        if team:
            query += " AND team = %s"
            params.append(team)
        
        query += " ORDER BY conversion_rate DESC LIMIT 10"
        
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        team_str = team if team else "all teams"
        answer = f"**Best Conversion Rates for {team_str} ({season}, min 10 shots):**\n\n"
        for i, p in enumerate(results[:5], 1):
            answer += f"{i}. **{p['player_name']}** ({p['team']}) - {float(p['conversion_rate']):.1f}%\n"
            answer += f"   - {p['goals']} goals from {p['total_shots']} shots\n\n"
        
        return {
            'type': 'aggregate',
            'query_type': 'best_conversion',
            'answer': answer,
            'data': results,
            'sources': [{'type': 'database', 'query': 'player_season_stats', 'season': season}]
        }
    
    def _get_most_shots(self, team: Optional[str], season: str) -> Dict[str, Any]:
       
        query = """
        SELECT player_name, team, total_shots, goals, conversion_rate
        FROM metrics.player_season_stats
        WHERE season = %s
        """
        params = [season]
        
        if team:
            query += " AND team = %s"
            params.append(team)
        
        query += " ORDER BY total_shots DESC LIMIT 10"
        
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        team_str = team if team else "all teams"
        answer = f"**Most Shots for {team_str} ({season}):**\n\n"
        for i, p in enumerate(results[:5], 1):
            answer += f"{i}. **{p['player_name']}** ({p['team']}) - {p['total_shots']} shots\n"
            answer += f"   - {p['goals']} goals, {float(p['conversion_rate']):.1f}% conversion\n\n"
        
        return {
            'type': 'aggregate',
            'query_type': 'most_shots',
            'answer': answer,
            'data': results,
            'sources': [{'type': 'database', 'query': 'player_season_stats', 'season': season}]
        }
    
    def _get_season_summary(self, team: Optional[str], season: str) -> Dict[str, Any]:
        
        query = """
        SELECT 
            team_name,
            COUNT(*) as matches,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) as draws,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(team_goals) as goals_for,
            SUM(opponent_goals) as goals_against,
            ROUND(AVG(team_xg)::numeric, 2) as avg_xg
        FROM metrics.team_matches
        WHERE season = %s
        """
        params = [season]
        
        if team:
            query += " AND team_name = %s"
            params.append(team)
        
        query += " GROUP BY team_name ORDER BY wins DESC"
        
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        team_str = team if team else "All Teams"
        answer = f"**Season Summary - {team_str} ({season}):**\n\n"
        
        for r in results:
            answer += f"**{r['team_name']}**\n"
            answer += f"- Matches: {r['matches']} (W{r['wins']}-D{r['draws']}-L{r['losses']})\n"
            answer += f"- Goals: {r['goals_for']} scored, {r['goals_against']} conceded\n"
            answer += f"- Average xG: {r['avg_xg']}\n\n"
        
        return {
            'type': 'aggregate',
            'query_type': 'season_summary',
            'answer': answer,
            'data': results,
            'sources': [{'type': 'database', 'query': 'team_matches', 'season': season}]
        }
    
    def _get_match_count(self, team: Optional[str], season: str) -> Dict[str, Any]:
        
        query = """
        SELECT team_name, COUNT(*) as matches
        FROM metrics.team_matches
        WHERE season = %s
        """
        params = [season]
        
        if team:
            query += " AND team_name = %s"
            params.append(team)
        
        query += " GROUP BY team_name"
        
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        answer = f"**Match Count ({season}):**\n\n"
        for r in results:
            answer += f"- **{r['team_name']}**: {r['matches']} matches played\n"
        
        return {
            'type': 'aggregate',
            'query_type': 'match_count',
            'answer': answer,
            'data': results,
            'sources': [{'type': 'database', 'query': 'team_matches', 'season': season}]
        }
    
    def _get_win_rate(self, team: Optional[str], season: str) -> Dict[str, Any]:
        
        query = """
        SELECT 
            team_name,
            COUNT(*) as matches,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            ROUND(100.0 * SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate
        FROM metrics.team_matches
        WHERE season = %s
        """
        params = [season]
        
        if team:
            query += " AND team_name = %s"
            params.append(team)
        
        query += " GROUP BY team_name ORDER BY win_rate DESC"
        
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        answer = f"**Win Rate ({season}):**\n\n"
        for r in results:
            answer += f"- **{r['team_name']}**: {r['win_rate']}% ({r['wins']}/{r['matches']} wins)\n"
        
        return {
            'type': 'aggregate',
            'query_type': 'win_rate',
            'answer': answer,
            'data': results,
            'sources': [{'type': 'database', 'query': 'team_matches', 'season': season}]
        }






    

