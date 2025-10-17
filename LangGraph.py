from langgraph.graph import StateGraph, START, END
from langsmith import traceable
from typing import TypedDict
from datetime import datetime
from cnd_langchain import (cnd_divida_ativa, cnd_fgts, cnd_trabalhista, cnd_municipal, pode_tentar, registrar_log, exibir_status_certidao, tentar_ate_dar_certo)
from SolutionPacket.Solution_bank import Bank
from SolutionPacket.Solution_telegram import TelegramSend
from dotenv import load_dotenv
import traceback
import os

# Carrega env
load_dotenv()

# Variáveis ambiente
ITOKEN = os.getenv("ITOKEN")
CHAT_ID = os.getenv("CHAT_ID")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

# Inicializa Telegram e Banco
telegram = TelegramSend("CND")

# Instancia banco: sua classe Bank pode aceitar uma connection string ou outra assinatura.
# Aqui eu monto uma connection string ODBC conforme você usou antes.
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_HOST};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS}"
try:
    banco = Bank(conn_str)
except TypeError:
    # Se sua classe Bank espera diferente (ex: Bank(nome) ou Bank() e depois .bank_connection),
    # tratamos em fallback: tente criar sem argumentos e conectar manualmente
    banco = Bank()
    try:
        banco.bank_connection(DB_USER, DB_PASS, DB_HOST, DB_NAME)
    except Exception as e:
        print("[ERRO] Não foi possível inicializar banco automaticamente:", e)
        raise

class EstadoRPA(TypedDict):
    etapa: str
    resultado: str

# ---------- Helpers ----------
@traceable(name="Enviar_Telegram")
def enviar_mensagem_telegram(mensagem: str, token: str, chat_id: str):
    try:
        telegram.telegram_bot(mensagem, token, chat_id)
        print("[OK] Mensagem enviada ao Telegram.")
    except Exception as e:
        print(f"[ERRO] Falha ao enviar mensagem Telegram: {e}")
        raise

@traceable(name="Registrar_Banco")
def registrar_log_banco(nome_certidao: str, tentativas: int, resultado: int, data_execucao: datetime):
    """
    Insere registro simples no banco. Usa `banco.executar_query` se existir,
    ou `banco.execution_query` como fallback dependendo da sua implementação.
    """
    try:
        data_str = data_execucao.strftime("%Y-%m-%d %H:%M:%S")
        query = (
            "INSERT INTO [rpa].[dbo].[cnd_testes] (nome_certidao, data_execucao, tentativas, resultado) "
            f"VALUES ('{nome_certidao}', '{data_str}', {tentativas}, {resultado})")

        # Preferir método executar_query se existir
        if hasattr(banco, "executar_query"):
            banco.executar_query(query)
        elif hasattr(banco, "execution_query"):
            banco.execution_query(query)
        else:
            # Último recurso: se banco expõe cursor/connection diretamente
            if hasattr(banco, "connection") and banco.connection:
                cur = banco.connection.cursor()
                cur.execute(query)
                banco.connection.commit()
                cur.close()
            else:
                raise RuntimeError("Objeto banco não fornece método para executar query.")

        print("[OK] Registro inserido no banco.")
    except Exception as e:
        print(f"[ERRO] Falha ao registrar no banco: {e}")
        traceback.print_exc()
        # não raise aqui para não interromper fluxo completo; mas opcionalmente raise

# ---------- Etapas do grafo ----------
@traceable(name="Etapa_Divida_Ativa")
def etapa_divida(state: EstadoRPA):
    inicio = datetime.now()
    print(f"[{inicio}] Iniciando etapa: Dívida Ativa")

    # Verifica se pode tentar
    if pode_tentar("divida_ativa", inicio.date()):
        # tentar_ate_dar_certo já faz tentativas e logs internos no seu módulo
        sucesso, erro_final = tentar_ate_dar_certo(cnd_divida_ativa, 3)

        if not sucesso:
            # registra como falha
            registrar_log_banco("divida_ativa", tentativas=3, resultado=0, data_execucao=inicio)
            # opcional: notifica erro no telegram
            enviar_mensagem_telegram(f"[ERRO] Falha na certidão Dívida Ativa. Erro: {erro_final}", ITOKEN, CHAT_ID)
            state["etapa"] = "divida_ativa_failed"
            state["resultado"] = f"Falha na Dívida Ativa: {erro_final}"
            return state

        # se cnd_divida_ativa retorna dados, usamos; caso contrário, tentamos inferir
        try:
            resultado_pdf = cnd_divida_ativa()
        except Exception as e:
            # Se cnd_divida_ativa já tiver sido chamada por tentar_ate_dar_certo, pode retornar None.
            # Só checamos a saída e seguimos defensivamente.
            resultado_pdf = None

        # Se função retornou dict com infos, use — senão, tente ler status do DB e exibir
        if isinstance(resultado_pdf, dict):
            numero = resultado_pdf.get("numero", "Não encontrado")
            emissao = resultado_pdf.get("emissao", "Não encontrado")
            validade = resultado_pdf.get("validade", "Não encontrado")
            arquivo = resultado_pdf.get("arquivo", "Não informado")
        else:
            # fallback: não temos retorno estruturado — informe que processo foi executado e peça verificação
            numero = "N/D"
            emissao = "N/D"
            validade = "N/D"
            arquivo = "N/D"
            print("[WARN] cnd_divida_ativa não retornou dict com infos — verifique implementação em cnd_langchain.py")

        mensagem = (
            "Certidão Dívida Ativa gerada com sucesso!\n\n"
            f"Número: {numero}\n"
            f"Emissão: {emissao}\n"
            f"Validade: {validade}\n\n"
            f"Arquivo salvo em: {arquivo}")

        # Envia telegram
        try:
            enviar_mensagem_telegram(mensagem, ITOKEN, CHAT_ID)
        except Exception:
            print("[WARN] Falha ao enviar telegram; continuando.")

        # Registra no banco (exemplo: 1 tentativa, sucesso)
        registrar_log_banco("divida_ativa", tentativas=1, resultado=1, data_execucao=inicio)

    else:
        print("[INFO] Não é permitido tentar emitir Dívida Ativa hoje conforme regras do banco.")
        enviar_mensagem_telegram("Execução de Dívida Ativa pulada por regra de tentativas no banco.", ITOKEN, CHAT_ID)

    # exibe status via função util (se implementada)
    try:
        exibir_status_certidao("divida_ativa")
    except Exception:
        pass

    fim = datetime.now()
    print(f"[{fim}] Etapa Dívida Ativa finalizada. Duração: {fim - inicio}")

    state["etapa"] = "divida_ativa_ok"
    state["resultado"] = f"Certidão Dívida Ativa concluída em {fim - inicio}."
    return state

@traceable(name="Etapa_FGTS")
def etapa_fgts(state: EstadoRPA):
    inicio = datetime.now()
    print(f"[{inicio}] Iniciando etapa: FGTS")

    if pode_tentar("fgts", inicio.date()):
        sucesso, erro_final = tentar_ate_dar_certo(cnd_fgts, 3)
        if sucesso:
            registrar_log_banco("fgts", tentativas=1, resultado=1, data_execucao=inicio)
        else:
            registrar_log_banco("fgts", tentativas=3, resultado=0, data_execucao=inicio)
            enviar_mensagem_telegram(f"[ERRO] Falha na FGTS: {erro_final}", ITOKEN, CHAT_ID)

    exibir_status_certidao("fgts")
    fim = datetime.now()
    state["etapa"] = "fgts_ok"
    state["resultado"] = f"Certidão FGTS concluída em {fim - inicio}."
    print(f"[{fim}] Etapa FGTS finalizada. Duração: {fim - inicio}")
    return state

@traceable(name="Etapa_Trabalhista")
def etapa_trabalhista(state: EstadoRPA):
    inicio = datetime.now()
    print(f"[{inicio}] Iniciando etapa: Trabalhista")

    if pode_tentar("trabalhista", inicio.date()):
        sucesso, erro_final = tentar_ate_dar_certo(cnd_trabalhista, 3, os.path.join(os.getcwd(), "CND - Trabalhista"))
        if sucesso:
            registrar_log_banco("trabalhista", tentativas=1, resultado=1, data_execucao=inicio)
        else:
            registrar_log_banco("trabalhista", tentativas=3, resultado=0, data_execucao=inicio)
            enviar_mensagem_telegram(f"[ERRO] Falha na Trabalhista: {erro_final}", ITOKEN, CHAT_ID)

    exibir_status_certidao("trabalhista")
    fim = datetime.now()
    state["etapa"] = "trabalhista_ok"
    state["resultado"] = f"Certidão Trabalhista concluída em {fim - inicio}."
    print(f"[{fim}] Etapa Trabalhista finalizada. Duração: {fim - inicio}")
    return state

@traceable(name="Etapa_Municipal")
def etapa_municipal(state: EstadoRPA):
    inicio = datetime.now()
    print(f"[{inicio}] Iniciando etapa: Municipal")

    if pode_tentar("municipal", inicio.date()):
        sucesso, erro_final = tentar_ate_dar_certo(cnd_municipal, 3)
        if sucesso:
            registrar_log_banco("municipal", tentativas=1, resultado=1, data_execucao=inicio)
        else:
            registrar_log_banco("municipal", tentativas=3, resultado=0, data_execucao=inicio)
            enviar_mensagem_telegram(f"[ERRO] Falha na Municipal: {erro_final}", ITOKEN, CHAT_ID)

    exibir_status_certidao("municipal")
    fim = datetime.now()
    state["etapa"] = "municipal_ok"
    state["resultado"] = f"Certidão Municipal concluída em {fim - inicio}."
    print(f"[{fim}] Etapa Municipal finalizada. Duração: {fim - inicio}")
    return state

# ---------- Grafo ----------
graph = StateGraph(EstadoRPA)
graph.add_node("divida", etapa_divida)
graph.add_node("fgts", etapa_fgts)
graph.add_node("trabalhista", etapa_trabalhista)
graph.add_node("municipal", etapa_municipal)

graph.add_edge(START, "divida")
graph.add_edge("divida", "fgts")
graph.add_edge("fgts", "trabalhista")
graph.add_edge("trabalhista", "municipal")
graph.add_edge("municipal", END)

app = graph.compile()

if __name__ == "__main__":
    try:
        print("[LangGraph] Iniciando execução completa das certidões...")
        app.invoke({"etapa": "inicio", "resultado": ""})
    except Exception as e:
        print("[ERRO] Execução interrompida:", e)
        traceback.print_exc()
    finally:
        print("[LangGraph] Execução finalizada.")
