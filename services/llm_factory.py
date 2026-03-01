from typing import Optional

from langchain_core.language_models import BaseChatModel
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI


def build_llm(provider: str, model: str, api_key: Optional[str] = None) -> BaseChatModel:
    if provider == "ollama":
        return ChatOllama(model=model, temperature=0.1)
    elif provider == "openai":
        return ChatOpenAI(model=model, api_key=api_key)
    else:
        raise ValueError("Unsupported provider")