from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings
from typing import List, Dict, Any
import os

class EmbeddingManager:
    def __init__(self,persist_directory: str = "./data/chroma"):
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.chroma_client = chromadb.Client(Settings(
            persist_directory=persist_directory,
            anonymized_telemetry=False
        ))

        try:
            self.collection = self.chroma_client.get_collection("epl_matches")
        except:
            self.collection = self.chroma_client.create_collection(
                name="epl_matches",
                metadata={"description": "EPL teams match statistics and analysis"}
            )

    
    def create_match_document(self,match:Dict[str,Any])->str:
        venue_text = "Home" if match.get('venue') == 'H' else "Away"
        
        total_shots = match.get('total_shots') or 0
        goals = match.get('goals') or 0
        conversion_rate = (goals / total_shots * 100) if total_shots else 0
        
        team_name = match.get('team_name', 'Unknown')
        team_xg = match.get('arsenal_xg') or 0
        opponent_xg = match.get('opponent_xg') or 0
        team_goals = match.get('arsenal_goals') or 0
        opponent_goals = match.get('opponent_goals') or 0
        shots_on_target = match.get('shots_on_target') or 0
        avg_shot_xg = match.get('avg_shot_xg') or 0
        big_chances = match.get('big_chances') or 0

        text=f"""
        Team: {team_name}
        Match: {team_name} vs {match.get('opponent', 'Unknown')} on {match.get('match_date', 'Unknown')} ({match.get('season', 'Unknown')} season)
        Venue: {venue_text}
        Result: {match.get('result', 'Unknown')} ({team_goals}-{opponent_goals})

        Expected Goals (xG):
        - {team_name} xG: {team_xg:.2f}
        - Opponent xG: {opponent_xg:.2f}
        - xG Overperformance: {(team_goals - float(team_xg)):.2f}

        Shot Statistics:
        - Total Shots: {total_shots}
        - Shots on Target: {shots_on_target}
        - Goals Scored: {goals}
        - Conversion Rate: {conversion_rate:.1f}%
        - Average Shot xG: {float(avg_shot_xg):.3f}
        - Big Chances: {big_chances}

        Scorers: {match.get('scorers') or 'None'}
        """
        return text.strip()
    def embed_matches(self,matches:List[Dict[str,Any]]):
        documents = []
        metadatas = []
        ids = []

        for match in matches:
            doc_text=self.create_match_document(match)
            documents.append(doc_text)

            metadatas.append({
                "match_date": str(match.get('match_date', '')),
                "season": str(match.get('season', '')),
                "team": str(match.get('team_name', '')),
                "opponent": str(match.get('opponent', '')),
                "result": str(match.get('result', '')),
                "venue": str(match.get('venue', ''))
            })

            ids.append(f"{match.get('team_name', 'unknown')}_{match.get('match_date', 'unknown')}_{match.get('opponent', 'unknown')}")

        self.collection.add(
            documents=documents,
            metadatas=metadatas,
            ids=ids
        )

        print(f" Added {len(documents)} matches to vector database")    

    
    def create_player_document(self, player: Dict[str, Any]) -> str:
        team_name = player.get('team_name', 'Unknown')
        player_name = player.get('player_name', 'Unknown')
        season = player.get('season', 'Unknown')
        goals = player.get('goals') or 0
        total_shots = player.get('total_shots') or 0
        total_xg = player.get('total_xg') or 0
        conversion_rate = player.get('conversion_rate') or 0
        big_chances = player.get('big_chances') or 0
        big_chances_scored = player.get('big_chances_scored') or 0
        matches_played = player.get('matches_played') or 0

        text = f"""
        Player Statistics: {player_name}
        Team: {team_name}
        Season: {season}
        
        Goal Scoring:
        - Goals Scored: {goals}
        - Total Shots: {total_shots}
        - Total xG: {total_xg:.2f}
        - Conversion Rate: {conversion_rate:.1f}%
        - Matches Played: {matches_played}
        
        Big Chances:
        - Big Chances: {big_chances}
        - Big Chances Scored: {big_chances_scored}
        
        {player_name} is a player for {team_name} in the {season} season with {goals} goals from {total_shots} shots.
        Top scorer ranking: This player has scored {goals} goals for {team_name} in {season}.
        """

        return text.strip()
    

    def embed_player_stats(self, players: List[Dict[str, Any]]):
        documents = []
        metadatas = []
        ids = []

        for player in players:
            doc_text = self.create_player_document(player)
            documents.append(doc_text)

            metadatas.append({
                "type": "player_stats",
                "player_name": str(player.get('player_name', '')),
                "team": str(player.get('team_name', '')),
                "season": str(player.get('season', '')),
                "goals": str(player.get('goals', 0))
            })

            ids.append(f"player_{player.get('team_name', 'unknown')}_{player.get('season', 'unknown')}_{player.get('player_name', 'unknown')}")

        if documents:
            self.collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            print(f" Added {len(documents)} player stats to vector database")

    def search(self, query: str, n_results: int = 5) -> Dict[str, Any]:

        results = self.collection.query(
            query_texts=[query],
            n_results=n_results
        )

        return {
            "documents": results['documents'][0] if results['documents'] else [],
            "metadatas": results['metadatas'][0] if results['metadatas'] else [],
            "distances": results['distances'][0] if results['distances'] else []
        }
    
    def clear_collection(self):
        self.chroma_client.delete_collection("epl_matches")
        self.collection = self.chroma_client.create_collection(
            name="epl_matches",
            metadata={"description": "EPL teams match statistics and analysis"}
        )

