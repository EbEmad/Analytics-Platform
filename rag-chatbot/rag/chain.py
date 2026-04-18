import ollama
import os
from typing import List,Dict,Any

class RAGChain:
    def __init__(self):
        self.ollama_host=os.getenv('OLLAMA_HOST', 'http://ollama:11434')
        self.model=os.getenv('OLLAMA_MODEL', 'qwen2.5:72b')
    
    
    with open('./system_prompts/analyst.txt', 'r') as f:
        self.system_prompt = f.read()

    
    def build_context(self,documents:List[str])->str:
        context_parts=[]
        for i,doc in enumerate(documents,1):
            context_parts.append(f"--- Match Data {i} --\n{doc}\n")
        
        return "\n".join(context_parts)
    

    def invoke(self, question: str, context: str, history: List[Dict] = None) -> Dict[str, Any]:

        messages=[]
        messages.append({
            "role":"system",
            "content": self.system_prompt
        })
        if history:
            for msg in  history:
                if msg['role'] in ['user', 'assistant']:
                    messages.append({
                        'role':msg['role'],
                        'content':msg['content']
                    })
        user_message=f""" 
        Using the following football match data, please answer the question:
        RELEVANT MATCH DATA:
        {context}
        USER QUESTION:
        {question}
        Please provide a data-driven analysis with specific statistics, match dates, and tactical insights.
        """

        messages.append({
            "role":"user",
            "content":user_message
        })

        try:
            response=ollama.chat(
                model=self.model,
                messages=messages,
                options={
                    'temperature': 0.7,
                    'top_p': 0.9,
                    'num_predict': 1024
                }
            )

            answer =response['message']['content']
            return {
                "answer":answer,
                "model":f"ollama-{self.model}",
                "tokens_used": response.get('eval_count', 0) + response.get('prompt_eval_count', 0)
            }
        except Exception as e:
            return {
                "answer": f"Error connecting to Ollama: {str(e)}. Please ensure Ollama is running and the model '{self.model}' is installed.",
                "model": f"ollama-{self.model}",
                "tokens_used": 0
            }

