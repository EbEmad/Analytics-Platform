import time
import hashlib
from typing import Dict,Any,Optional
from functools import lru_cache

class ResponseCache:

    def __init__(self,ttl_seconds:int=3600):
        self.cache:Dict[str,Dict[str,Any]]={}
        self.ttl=ttl_seconds
    
    def _normalize_question(self, question: str) -> str:
        normalized=question.lower().strip()
        normalized=' '.join(normalized.split())
        return normalized

    def _get_cache_key(self, question: str) -> str:
        normalized=self._normalize_question(question)
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def get(self, question: str) -> Optional[Dict[str, Any]]:
        key=self._get_cache_key(question)

        if key in self.cache:
            entry=self.cache[key]
            if time.time() - entry['timestamp'] < self.ttl:
                return entry['response']
            else:
                del self.cache[key]
        
        return None
    def set(self, question: str, response: Dict[str, Any]) -> None:
        key=self._get_cache_key(question)
        self.cache[key]={
            'response': response,
            'timestamp': time.time()
        }

    def clear(self)->None:
        self.cache.clear()
    def get_stats(self)->Dict[str,Any]:
        now = time.time()
        valid_count=sum(
            1 for entry in self.cache.values()
            if now-entry['timestamp']<self.ttl
        )
        return {
            'total_entries': len(self.cache),
            'valid_entries': valid_count,
            'ttl_seconds': self.ttl
        }




