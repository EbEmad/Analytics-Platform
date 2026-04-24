from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv
from utils.db_connector import DatabaseConnector
from utils.question_handler import QuestionHandler
from utils.cache import ResponseCache
from rag.embeddings import EmbeddingManager
from rag.chain import RAGChain
import uvicorn
from utils.metrics import setup_metrics

load_dotenv()

app = FastAPI(title="EPL Analytics RAG Chatbot API")

setup_metrics(app)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
db=DatabaseConnector()
question_handler=QuestionHandler(db)
response_cache = ResponseCache(ttl_seconds=1800)
embeddings=EmbeddingManager(persist_directory=os.getenv('CHROMA_DB_PATH', './data/chroma'))
rag_chain=RAGChain()


class ChatMessage(BaseModel):
    role:str
    content:str

class ChatRequest(BaseModel):
    question:str
    conversation_history:Optional[List[ChatMessage]]=[]

class ChatResponse(BaseModel):
    answer: str
    sources: List[Dict]
    confidence: float
    model: str

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "EPL Analytics RAG Chatbot"}

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Hybrid chat endpoint - uses direct SQL for aggregate questions,
    falls back to RAG + LLM for complex analysis questions.
    Includes caching for faster repeated queries.
    """
    try:
        cached_response = response_cache.get(request.question)
        if cached_response:
            return ChatResponse(
                answer=cached_response['answer'],
                sources=cached_response['sources'],
                confidence=cached_response['confidence'],
                model=cached_response['model'] + " (cached)"
            )
        aggregate_result=question_handler.handle_aggregate_question(request.question)

        if aggregate_result:
            response_data = {
                'answer': aggregate_result['answer'],
                'sources': aggregate_result['sources'],
                'confidence': 0.95,
                'model': "direct-sql"
            }

            response_cache.set(request.question,response_data)

            return ChatResponse(
                answer=aggregate_result['answer'],
                sources=aggregate_result['sources'],
                confidence=0.95, 
                model="direct-sql"
            )
        search_results = embeddings.search(request.question, n_results=5)

        documents = search_results['documents']
        metadatas = search_results['metadatas']
        distances = search_results['distances']
        context = rag_chain.build_context(documents)

        history=[{"role":msg.role,"content":msg.content} for msg in request.conversation_history]

        response=rag_chain.invoke(
            question=request.question,
            context=context,
            history=history
        )

        avg_distance = sum(distances) / len(distances) if distances else 1.0
        confidence = max(0.0, min(1.0, 1.0 - (avg_distance / 2.0)))

        sources = []
        for meta in metadatas:
            if meta.get('type') == 'player_stats':
                sources.append({
                    "type": "player_stats",
                    "player_name": meta.get('player_name', 'Unknown'),
                    "team": meta.get('team', 'Unknown'),
                    "season": meta.get('season', 'Unknown'),
                    "goals": meta.get('goals', '0')
                })
            else:
                sources.append({
                    "type": "match",
                    "team": meta.get('team', 'Arsenal'),
                    "match_date": meta.get('match_date', 'Unknown'),
                    "opponent": meta.get('opponent', 'Unknown'),
                    "result": meta.get('result', 'Unknown'),
                    "season": meta.get('season', 'Unknown')
                })

        response_data = {
            'answer': response['answer'],
            'sources': sources,
            'confidence': round(confidence, 2),
            'model': response['model']
        }

        response_cache.set(request.question,response_data)

        return ChatResponse(
            answer=response['answer'],
            sources=sources,
            confidence=round(confidence, 2),
            model=response['model']
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing chat: {str(e)}")
    


@app.post("/rebuild-embeddings")
async def rebuild_embeddings():
    try:
        embeddings.clear_collection()
        matches=db.fetch_all_matches()
        embeddings.embed_matches(matches)

        player_stats=db.fetch_all_player_stats()
        embeddings.embed_player_stats(player_stats)

        return {
            "status": "success",
            "matches_indexed": len(matches),
            "players_indexed": len(player_stats),
            "message": "Embeddings rebuilt successfully with matches and player stats"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error rebuilding embeddings: {str(e)}")



@app.post("/cache/clear")
async def clear_cache():
    try:
        response_cache.clear()
        return {"status": "success", "message": "Cache cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__=="__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
