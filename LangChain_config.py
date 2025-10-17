from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from langsmith import traceable
from dotenv import load_dotenv
import os

load_dotenv()

# Configuração do LangSmith
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGCHAIN_API_KEY")
os.environ["LANGCHAIN_PROJECT"] = "CND_completo"

class ExtratorIA:
    def __init__(self):
        self.llm = ChatOpenAI(
            model="gpt-4o-mini",
            api_key=os.getenv("CHAVE_OPENIA"),
            temperature=0)

    @traceable(name="Extracao_Info_IA")
    def extrair_info(self, texto, tipo_certidao: str):
        mensagens = [
            SystemMessage(content=f"Você é um assistente que extrai dados de certidões fiscais do tipo {tipo_certidao}."),
            HumanMessage(content=(
                f"Texto da certidão:\n\n{texto}\n\n"
                "Extraia apenas as seguintes informações:\n"
                "- Número da certidão (somente os dígitos)\n"
                "- Data de emissão (formato dd/mm/aaaa)\n"
                "- Validade (formato dd/mm/aaaa)\n\n"
                "Responda de forma direta e organizada, por exemplo:\n"
                "Número: 123456\nEmissão: 01/10/2025\nValidade: 01/12/2025"))]

        resposta = self.llm.invoke(mensagens)
        return resposta.content.strip()

