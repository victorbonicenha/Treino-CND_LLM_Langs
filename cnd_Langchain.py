from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from datetime import datetime
from SolutionPacket.Solution_bank import Bank
from SolutionPacket.Solution_telegram import TelegramSend
from LangChain_config import ExtratorIA
import base64
from dotenv import load_dotenv
import shutil
import requests
from time import sleep
import pdfplumber
import easyocr
import os
import sys

# ------------ Configs ------------ #
load_dotenv()
CNPJ_BASE = os.getenv("CNPJ_BASE")
CNPJ_BASICO = os.getenv('CNPJ_BASICO')
CNPJ = os.getenv("CNPJ_SC")
CPF = os.getenv('CPF')
NOME = os.getenv("NOME")

# ------------ API Captchas ------------ #
API_KEY = os.getenv("CHAVE_API_CAPTCHA")
ANTICAPTCHA_CREATE_URL = "https://api.anti-captcha.com/createTask"
ANTICAPTCHA_RESULT_URL = "https://api.anti-captcha.com/getTaskResult"

# ------------ telegram ------------ #
ITOKEN = os.getenv("ITOKEN_TELEGRAM")
CHAT_ID = os.getenv("CHAT_ID")
telegram = TelegramSend("CND")
erro = TelegramSend("CND ERRO:")

# ------------ OpenIA ------------ #
OPENIA_KEY=os.getenv("CHAVE_OPENIA")
chat = ExtratorIA()

# ------------ caminhos e pastas BASE ------------ #
BASE_PATH = os.getenv("BASE_PATH")
pasta_downloads = os.path.join(os.path.expanduser("~"), "Downloads")

# ------------ tempo atual e de pastas ------------ #
meses = {'01': 'Janeiro', '02': 'Fevereiro', '03': 'Março', '04': 'Abril',
         '05': 'Maio', '06': 'Junho', '07': 'Julho', '08': 'Agosto',
         '09': 'Setembro', '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro'}

mes_atual = datetime.now().strftime('%m')
mes_extenso = meses[mes_atual] 
pasta_mes = f"{mes_atual} - {mes_extenso}"  
data_hoje = datetime.now().strftime('%Y-%m-%d')
ano_atual = datetime.now().strftime('%Y')

# ------------ Caminho e pastas Finais ------------ #
pasta_fgts = os.path.join(BASE_PATH, "CND_FGTS", ano_atual, pasta_mes)
pasta_municipal = os.path.join(BASE_PATH, "CND - Municipal", ano_atual, pasta_mes)
pasta_trabalhista = os.path.join(BASE_PATH, "CND - Trabalhista", ano_atual, pasta_mes)
pasta_divida_ativa = os.path.join(BASE_PATH, "CND - Divida Ativa", ano_atual, pasta_mes)

# ------------ Config Banco ------------ #
bank = Bank("RPA")
bank.bank_connection(
    os.getenv("DB_USER"),
    os.getenv("DB_PASS"),
    os.getenv("DB_HOST"),
    os.getenv("DB_NAME"))

# ------------ Start Robo ------------ #
def iniciar_selenium(download_path=None):
    options = Options()
    options.add_argument("--start-maximized")
    if download_path:
        prefs = {
            "download.default_directory": download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True}
        options.add_experimental_option("prefs", prefs)
    navegador = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return navegador
               
# ------------ Configuração para Inserir ou Atualizar o banco ------------ #
def executar_insert_update(query, parametros=None):
    """
    Executa apenas INSERT/UPDATE no banco usando o objeto global `bank`.
    Bloqueia DELETE para evitar remoção acidental.
    """
    if bank.connection is None:
        raise Exception("Conexão com o banco não está aberta.")
    
    comando = query.strip().upper()
    if not (comando.startswith("INSERT") or comando.startswith("UPDATE")):
        raise Exception("Apenas INSERT ou UPDATE são permitidos nessa função.")

    try:
        cursor = bank.connection.cursor()
        if parametros:
            cursor.execute(query, parametros)
        else:
            cursor.execute(query)
        bank.connection.commit()
        cursor.close()
    except Exception as e:
        erro.telegram_bot(f"Erro ao executar insert/update: {e}", ITOKEN, CHAT_ID)
             
# ------------ Configuração para Registros de Logs do banco ------------ #
def registrar_log(certidao, sucesso):
    try:
        data_hoje = datetime.now().date()
        resultado_bd = bank.execution_query(
            "SELECT tentativas, resultado FROM dbo.cnd_testes "
            "WHERE nome_certidao = ? AND CAST(data_execucao AS DATE) = ?",
            (certidao, data_hoje))

        if resultado_bd:
            tentativas_atual, resultado_atual = resultado_bd[0]
            nova_tentativa = tentativas_atual + 1
            novo_resultado = 1 if sucesso == 1 else resultado_atual

            executar_insert_update(
                "UPDATE dbo.cnd_testes SET tentativas=?, resultado=? "
                "WHERE nome_certidao=? AND CAST(data_execucao AS DATE)=?",
                (nova_tentativa, novo_resultado, certidao, data_hoje))
        else:
            data_execucao = datetime.now()
            executar_insert_update(
                "INSERT INTO dbo.cnd_testes (nome_certidao, data_execucao, tentativas, resultado) "
                "VALUES (?, ?, ?, ?)",
                (certidao, data_execucao, 1, sucesso))
            
    except Exception as e:
        itoken = os.getenv("ITOKEN_TELEGRAM")
        chat_id = os.getenv("CHAT_ID")
        erro.telegram_bot(f"Erro ao registrar no banco: {e}", itoken, chat_id)
             
# ------------ Configuração para verificar no banco se está disponivel ou não para a execução do codigo ------------ #
def pode_tentar(certidao, data_hoje):
    try:
        resultado = bank.execution_query(
            "SELECT COUNT(*) FROM dbo.cnd_testes "
            "WHERE nome_certidao=? AND CAST(data_execucao AS DATE)=? "
            "AND (resultado=1 OR (tentativas>=3 AND resultado=0))",
            (certidao, data_hoje))
        return resultado[0][0] < 1
    except Exception as e:
        erro.telegram_bot(f"Erro ao consultar tentativas no banco: {e}", os.getenv("ITOKEN_TELEGRAM"), os.getenv("CHAT_ID"))
        return False
             
# ------------ Configuração para exibir o resultado da execuçao no banco ------------ #
def exibir_status_certidao(certidao):
    try:
        data_hoje = datetime.now().date()
        resultado = bank.execution_query(
            "SELECT tentativas, resultado FROM dbo.cnd_testes "
            "WHERE nome_certidao=? AND CAST(data_execucao AS DATE)=?",
            (certidao, data_hoje))
        if resultado:
            tentativas, res = resultado[0]
            status = "Sucesso" if res == 1 else "Falhou"
            msg = f"Certidão: {certidao}\n\nTentativas: {tentativas}\n\nResultado: {status}"
        else:
            msg = f"Nenhum registro encontrado para '{certidao}' hoje."
        print(msg)
    except Exception as e:
        erro.telegram_bot(f"Erro ao buscar status: {e}", os.getenv("ITOKEN_TELEGRAM"), os.getenv("CHAT_ID"))
             
# ------------ Configuração para resolução de captcha por imagem com API anticaptcha ------------ #
def resolver_captcha_imagem(caminho_imagem, tentativas=3):
    with open(caminho_imagem, 'rb') as f:
        image_base64 = base64.b64encode(f.read()).decode('utf-8')

    url_create = ANTICAPTCHA_CREATE_URL
    url_result = ANTICAPTCHA_RESULT_URL

    for i in range(tentativas):
        payload = {
            "clientKey": API_KEY,
            "task": {
                "type": "ImageToTextTask",
                "body": image_base64}}

        response = requests.post(url_create, json=payload).json()

        if response.get('errorId') != 0:
            continue

        task_id = response.get('taskId')

        for tentativa in range(30):
            sleep(3)
            result = requests.post(url_result, json={
                "clientKey": API_KEY,
                "taskId": task_id}).json()

            if result.get('status') == 'ready':
                texto = result.get('solution', {}).get('text')
                return texto
    return None
         
# ------------ Configuração para resolução de captcha recaptcha (um dos tipos de captchas) com API anticaptcha ------------ #
def resolver_captcha_recaptcha(api_key, site_key, site_url, tentativas=3):
    url_create = "https://api.anti-captcha.com/createTask"
    url_result = "https://api.anti-captcha.com/getTaskResult"

    print(f"[INFO] Iniciando resolução de reCAPTCHA para {site_url}")
    print(f"[DEBUG] API_KEY (primeiros 6): {api_key[:6]}...")
    print(f"[DEBUG] site_key: {site_key}")

    for i in range(tentativas):
        print(f"[INFO] Tentativa {i+1}/{tentativas} de criar task no AntiCaptcha...")
        payload = {
            "clientKey": api_key,
            "task": {
                "type": "NoCaptchaTaskProxyless",
                "websiteURL": site_url,
                "websiteKey": site_key}}

        try:
            response = requests.post(url_create, json=payload, timeout=15).json()
        except Exception as e:
            print(f"[ERRO] Falha na requisição à API: {e}")
            continue

        print(f"[DEBUG] Resposta criação de task: {response}")

        if response.get('errorId') != 0:
            print(f"[ERRO] Falha ao criar task: {response.get('errorDescription')}")
            continue

        task_id = response.get('taskId')
        print(f"[OK] Task criada com sucesso: {task_id}")

        for tentativa_resultado in range(30):
            sleep(3)
            res = requests.post(url_result, json={
                "clientKey": api_key,
                "taskId": task_id}).json()

            print(f"[DEBUG] Consulta {tentativa_resultado+1}/30 -> {res}")

            if res.get('status') == 'ready':
                token = res.get('solution', {}).get('gRecaptchaResponse')
                print(f"[SUCESSO] Token resolvido: {token[:30]}...")
                return token

        print("[AVISO] Timeout ao aguardar resultado, tentando novamente...")

    print("[FALHA] Não foi possível resolver o reCAPTCHA após várias tentativas.")
    return None
         
# ------------ Configuração para resolução de captcha com API anticaptcha ------------ #
def resolver_captcha_anticaptcha(navegador, tentativas=3):
    captcha_element = navegador.find_element(By.XPATH, '//*[@id="captchaImage"]/img')
    captcha_path = os.path.join(os.getcwd(), 'captcha.png')
    captcha_element.screenshot(captcha_path)

    with open(captcha_path, "rb") as img_file:
        b64_string = base64.b64encode(img_file.read()).decode()

    url_create = ANTICAPTCHA_CREATE_URL
    url_result = ANTICAPTCHA_RESULT_URL
    headers = {"Content-Type": "application/json"}

    for i in range(tentativas):
        payload = {
            "clientKey": API_KEY,
            "task": {
                "type": "ImageToTextTask",
                "body": b64_string}}

        response = requests.post(url_create, json=payload, headers=headers).json()

        if response.get('errorId') != 0:
            continue

        task_id = response.get('taskId')
        for tentativa in range(30):
            sleep(3)
            res = requests.post(url_result, json={
                "clientKey": API_KEY,
                "taskId": task_id}, headers=headers).json()

            if res.get('status') == 'ready':
                solution = res.get('solution', {}).get('text')
                os.remove(captcha_path)
                return solution

    os.remove(captcha_path)
    erro.telegram_bot("Timeout: captcha não foi resolvido na certidão municipal.", ITOKEN, CHAT_ID)
    navegador.quit()
    sys.exit()
         
# ------------ Configuração da Open IA para extrair dados especificos partindo de um pdf ------------ #
def extrair_info_Divida_Ativa(caminho_pdf):
    with pdfplumber.open(caminho_pdf) as pdf:
        texto_pdf = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])

    resposta_texto = chat.extrair_info(texto_pdf, "Dívida Ativa")

    dados = {"numero": "", "emissao": "", "validade": ""}
    for linha in resposta_texto.split("\n"):
        if "número" in linha.lower():
            dados["numero"] = linha.split(":")[-1].strip()
        elif "emissão" in linha.lower():
            dados["emissao"] = linha.split(":")[-1].strip()
        elif "validade" in linha.lower():
            dados["validade"] = linha.split(":")[-1].strip()

    return dados

# ------------ FGTS (usa OCR + IA) ------------ #
def ocr_transcrever_FGTS(file_path):
    try:
        reader = easyocr.Reader(['pt'])
        resultados = reader.readtext(file_path)
        if not resultados:
            return ""
        return "\n".join([res[1] for res in resultados])
    except Exception as e:
        print(f"Erro no OCR com EasyOCR: {e}")
        return ""

def extrair_info_FGTS(texto):
    resposta_texto = chat.extrair_info(texto, "FGTS")
    dados = {"numero": "", "emissao": "", "validade": ""}

    for linha in resposta_texto.split("\n"):
        if "número" in linha.lower():
            dados["numero"] = linha.split(":")[-1].strip()
        elif "emissão" in linha.lower() or "obtida" in linha.lower():
            dados["emissao"] = linha.split(":")[-1].strip()
        elif "validade" in linha.lower():
            dados["validade"] = linha.split(":")[-1].strip()

    return dados

# ------------ TRABALHISTA (usa PDF + IA) ------------ #
def extrair_info_Trabalhista(caminho_pdf):
    with pdfplumber.open(caminho_pdf) as pdf:
        texto_pdf = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])

    resposta_texto = chat.extrair_info(texto_pdf, "Trabalhista")
    dados = {"numero": "", "emissao": "", "validade": ""}

    for linha in resposta_texto.split("\n"):
        if "número" in linha.lower():
            dados["numero"] = linha.split(":")[-1].strip()
        elif "emissão" in linha.lower():
            dados["emissao"] = linha.split(":")[-1].strip()
        elif "validade" in linha.lower():
            dados["validade"] = linha.split(":")[-1].strip()

    return dados

# ------------ MUNICIPAL (usa OCR + IA) ------------ #
def ocr_transcrever_Municipal(file_path):
    try:
        reader = easyocr.Reader(['pt'])
        resultados = reader.readtext(file_path)
        if not resultados:
            return ""
        return "\n".join([res[1] for res in resultados])
    except Exception as e:
        print(f"Erro no OCR com EasyOCR: {e}")
        return ""

def extrair_info_Municipal(texto):
    resposta_texto = chat.extrair_info(texto, "Municipal")
    dados = {"emissao": "", "validade": ""}

    for linha in resposta_texto.split("\n"):
        if "emissão" in linha.lower() or "emitida" in linha.lower():
            dados["emissao"] = linha.split(":")[-1].strip()
        elif "validade" in linha.lower():
            dados["validade"] = linha.split(":")[-1].strip()

    return dados

# ------------ Start do robo na primeira certidão: Divida ------------ #
def cnd_divida_ativa():
    url_site = 'https://www.dividaativa.pge.sp.gov.br/sc/pages/home/home_novo.jsf'
    navegador = iniciar_selenium()
    navegador.get(url_site)
    sleep(3)

    try:
        try:
            navegador.find_element(By.XPATH, '//*[@id="modalPanelDebIpvaIDContentDiv"]/div').click()
            sleep(2)
        except:
            pass

        navegador.find_element(By.XPATH, '//*[@id="menu:j_id99_span"]').click()
        sleep(2)

        wait = WebDriverWait(navegador, 20)
        elemento = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="menu:itemMenu3649:anchor"]')))
        elemento.click()

        campo_cnpj = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="emitirCrda:crdaInputCnpjBase"]')))
        campo_cnpj.send_keys(CNPJ_BASE)

        site_key = navegador.find_element(By.XPATH, '//*[@id="recaptcha"]').get_attribute('data-sitekey')
        token = resolver_captcha_recaptcha(API_KEY, site_key, url_site)
        if not token:
            raise Exception("Não foi possível resolver o reCAPTCHA.")

        navegador.execute_script("""
            document.getElementById("g-recaptcha-response").style.display = 'block';
            document.getElementById("g-recaptcha-response").value = arguments[0];
            document.getElementById("g-recaptcha-response").innerHTML = arguments[0]; """, token)
        sleep(2)

        navegador.find_element(By.XPATH, '//*[@id="emitirCrda:j_id78_body"]/div[2]/input[2]').click()
        sleep(4)

        os.makedirs(pasta_divida_ativa, exist_ok=True)

        arquivos_encontrados = False
        for arquivo in os.listdir(pasta_downloads):
            if arquivo.endswith(".pdf") and "crda" in arquivo.lower():
                caminho_origem = os.path.join(pasta_downloads, arquivo)
                nome_novo = f"{os.path.splitext(arquivo)[0]}_{data_hoje}.pdf"
                caminho_final = os.path.join(pasta_divida_ativa, nome_novo)

                try:
                    shutil.move(caminho_origem, caminho_final)
                    print(f"[OK] Arquivo movido para: {caminho_final}")
                    sleep(2)
                    infos = extrair_info_Divida_Ativa(caminho_final)
                    mensagem_final = (
                        "Certidão Dívida Ativa gerada com sucesso!\n\n"
                        f"Número: {infos['numero']}\n"
                        f"Emissão: {infos['emissao']}\n"
                        f"Validade: {infos['validade']}\n\n"
                        f"Arquivo salvo em:\n{caminho_final}")
                    telegram.telegram_bot(mensagem_final, ITOKEN, CHAT_ID)
                    arquivos_encontrados = True
                    break

                except Exception as move_error:
                    raise Exception(f"Erro ao mover/processar PDF: {move_error}")

        if not arquivos_encontrados:
            raise Exception("Nenhum arquivo PDF com 'crda' encontrado na pasta de downloads.")

    except Exception as e:
        raise Exception(f"Erro no fluxo da Dívida Ativa: {e}")

    finally:
        navegador.quit()

# ------------ 2a Certidão: FGTS ------------ #
def cnd_fgts():
    navegador = iniciar_selenium()
    url_site = 'https://consulta-crf.caixa.gov.br/consultacrf/pages/consultaEmpregador.jsf'
    navegador.get(url_site)
    sleep(3)

    navegador.find_element(By.XPATH, '//*[@id="mainForm:txtInscricao1"]').send_keys(CNPJ_BASICO)
    sleep(1)
    navegador.find_element(By.XPATH, '//*[@id="mainForm:uf"]').click()
    navegador.find_element(By.XPATH, '//*[@id="mainForm:uf"]/option[26]').click()
    sleep(3)

    tentativas = 0
    sucesso = False

    while tentativas < 5 and not sucesso:
        tentativas += 1

        CAPTCHA_XPATH = '//*[@id="captchaImg_N2"]'
        captcha_element = navegador.find_element(By.XPATH, CAPTCHA_XPATH)
        navegador.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", captcha_element)
        sleep(1) 

        captcha_src = captcha_element.get_attribute("src")
        if captcha_src.strip() == "data:image/png;base64,":
            navegador.quit()
            raise Exception("CAPTCHA base64 vazio, site possivelmente fora do ar.")

        image_path = 'captcha_fgts.png'
        captcha_element.screenshot(image_path)
        captcha_resolvido = resolver_captcha_imagem(image_path)

        if captcha_resolvido and len(captcha_resolvido) >= 4:
            navegador.find_element(By.XPATH, '//*[@id="mainForm:txtCaptcha"]').clear()
            navegador.find_element(By.XPATH, '//*[@id="mainForm:txtCaptcha"]').send_keys(captcha_resolvido)
            sleep(1)

            navegador.find_element(By.ID, 'mainForm:btnConsultar').click()
            sleep(3)

            if "Código da imagem inválido" in navegador.page_source:
                navegador.find_element(By.XPATH, '//*[@id="mainForm:j_id98"]').click()
                sleep(5)
            else:
                sucesso = True
        else:
            navegador.find_element(By.XPATH, '//*[@id="mainForm:j_id98"]').click()
            sleep(2)

    if sucesso:
        try:
            WebDriverWait(navegador, 15).until(EC.presence_of_element_located((By.XPATH, '//*[@id="mainForm:listaEstabelecimentos:0:linkAction1"]/span'))).click()
            sleep(2)
            navegador.find_element(By.XPATH, '//*[@id="mainForm:j_id51"]').click()
            sleep(2)
            navegador.find_element(By.XPATH, '//*[@id="mainForm:btnVisualizar"]').click()
            sleep(3)

            nome_arquivo = f"fgts_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.png"
            navegador.fullscreen_window()
            sleep(3)
            navegador.get_screenshot_as_file(nome_arquivo)

            screenshot_path = os.path.abspath(nome_arquivo)
            pasta_fgts = os.path.join(BASE_PATH, "CND_FGTS", ano_atual, pasta_mes)
            os.makedirs(pasta_fgts, exist_ok=True)
            novo_caminho = os.path.join(pasta_fgts, nome_arquivo)
            shutil.move(screenshot_path, novo_caminho)
            
            texto_certidao = ocr_transcrever_FGTS(novo_caminho)
            infos = extrair_info_FGTS(texto_certidao)
            mensagem = (
                "Certidão FGTS gerada com sucesso!\n\n"
                f"Validade: {infos['validade']}\n"
                f"Emissão: {infos['emissao']}\n"
                f"Número: {infos['numero']}\n\n"
                f"Arquivo salvo em:\n\n{novo_caminho}")
            telegram.telegram_bot(mensagem, ITOKEN, CHAT_ID)

        except Exception as e:
            screenshot_path = os.path.abspath('erro_captura_certidao.png')
            navegador.get_screenshot_as_file(screenshot_path)
            navegador.quit()
            raise Exception(f"Erro ao gerar certidão FGTS: {str(e)}")

    else:
        navegador.quit()
        raise Exception("Captcha não resolvido após 5 tentativas.")

    sleep(5)
    navegador.quit()
         
# ------------ 3a Certidão: Trabalhista ------------ #
def cnd_trabalhista(base_path):
    navegador = iniciar_selenium(base_path)

    url = 'https://cndt-certidao.tst.jus.br/inicio.faces'
    navegador.get(url)
    sleep(3)

    try:
        navegador.find_element(By.XPATH, '//*[@id="corpo"]/div/div[2]/input[1]').click()
        sleep(2)

        navegador.find_element(By.XPATH, '//*[@id="gerarCertidaoForm:cpfCnpj"]').send_keys(CNPJ)
        sleep(2)

        wait = WebDriverWait(navegador, 10)
        captcha_element = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="idImgBase64"]')))

        captcha_path = os.path.join(base_path, 'captcha_trabalhista.png')
        os.makedirs(base_path, exist_ok=True)
        captcha_element.screenshot(captcha_path)

        captcha_text = resolver_captcha_imagem(captcha_path)

        if not captcha_text or len(captcha_text.strip()) < 4:
            raise Exception("Falha ao resolver o CAPTCHA da Certidão Trabalhista.")

        navegador.find_element(By.XPATH, '//*[@id="idCampoResposta"]').send_keys(captcha_text)
        navegador.find_element(By.XPATH, '//*[@id="gerarCertidaoForm:btnEmitirCertidao"]').click()
        sleep(4)

        #screenshot_trabalhista = os.path.join(os.getcwd(), f"print_trabalhista_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.png")
        #navegador.save_screenshot(screenshot_trabalhista)

    except Exception as e:
        navegador.quit()
        raise Exception(f"[TRABALHISTA] Erro na emissão da certidão: {str(e)}")

    navegador.quit()
    encontrou = False
    
    try:
        for arquivo in os.listdir(base_path):
            if "certidao" in arquivo.lower() and arquivo.lower().endswith(".pdf"):
                origem = os.path.join(base_path, arquivo)
                destino = os.path.join(pasta_trabalhista, arquivo)
                os.makedirs(pasta_trabalhista, exist_ok=True)
                shutil.move(origem, destino)
                nome_arquivo = os.path.basename(destino)
                caminho_final = os.path.join(pasta_trabalhista, nome_arquivo)
                infos = extrair_info_Trabalhista(destino)
                mensagem_final = (
                    "Certidão Trabalhista Gerada com Sucesso!\n\n"
                    f"Número: {infos['numero']}\n"
                    f"Emissão: {infos['emissao']}\n"
                    f"Validade: {infos['validade']}\n\n"
                    f"Arquivo {nome_arquivo} foi movido para a pasta:\n\n{caminho_final}")
                
                telegram.telegram_bot(mensagem_final, ITOKEN, CHAT_ID)
                encontrou = True
                break
    except Exception as move_erro:
        erro.telegram_bot(f"Erro ao mover PDF:\n{str(move_erro)}", ITOKEN, CHAT_ID)
        raise

    if not encontrou:
        raise Exception("PDF da Certidão Trabalhista não encontrado.")

    #os.remove(screenshot_trabalhista)
         
# ------------ 4a Certidão: Municipal ------------ #
def cnd_municipal():
    navegador = iniciar_selenium()
    url_site = 'https://portal.diadema.sp.gov.br/certidao-negativa-mobiliaria-e-imobiliaria-de-debitos/'
    navegador.get(url_site)

    try:
        navegador.find_element(By.CLASS_NAME, 'eicon-close').click()
    except NoSuchElementException:
        pass

    wait = WebDriverWait(navegador, 20)

    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="page"]/div/section[2]/div/div/div/div/div/p[4]/a/b'))).click()
    sleep(2)
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="vCPFSOLICITANTE"]'))).send_keys(CPF)
    sleep(1)
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="vNOMESOLICITANTE"]'))).send_keys(NOME)
    sleep(1)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="vTIPOFILTRO"]'))).click()
    sleep(1)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="vTIPOFILTRO"]/option[3]'))).click()
    sleep(1)
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="vNRFILTRO"]'))).send_keys(CNPJ)
    sleep(1)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="Rowfinalidade"]/td[2]'))).click()
    sleep(1)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="vMIAID"]/option[17]'))).click()
    sleep(1)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="TABLECONTRIBUINTE"]/tbody/tr[29]/td[2]'))).click()

    captcha_text = resolver_captcha_anticaptcha(navegador)
  
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="_cfield"]'))).send_keys(captcha_text)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="TABLE3"]/tbody/tr/td[1]/input'))).click()

    try:
        WebDriverWait(navegador, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="divMensagem"]/div')))
        captcha_text = resolver_captcha_anticaptcha(navegador)
        navegador.find_element(By.XPATH, '//*[@id="_cfield"]').clear()
        navegador.find_element(By.XPATH, '//*[@id="_cfield"]').send_keys(captcha_text)
        navegador.find_element(By.XPATH, '//*[@id="TABLE3"]/tbody/tr/td[1]/input').click()
    except:
        pass

    sleep(3)
    try:
        navegador.get('https://portaldeservicos.diadema.sp.gov.br/eagata/servlet/hwvdocumentos_v3')
        navegador.fullscreen_window()
        sleep(2)
        navegador.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(1.5)

        screenshot_path = f"cnd_municipal_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.png"
        navegador.save_screenshot(screenshot_path)

        os.makedirs(pasta_municipal, exist_ok=True)
        destino_final = os.path.join(pasta_municipal, os.path.basename(screenshot_path))
        shutil.move(screenshot_path, destino_final)

        texto_certidao = ocr_transcrever_Municipal(destino_final)
        infos = extrair_info_Municipal(texto_certidao)
        mensagem = (
            "Certidão Municipal gerada com sucesso!\n\n"
            f"Emissão: {infos['emissao']}\n"
            f"Validade: {infos['validade']}\n\n"
            f"Arquivo salvo em:\n\n{destino_final}")
        telegram.telegram_bot(mensagem, ITOKEN, CHAT_ID)

    except Exception as e:
        navegador.quit()
        raise Exception(f"Erro ao processar certidão municipal: {e}")

    finally:
        navegador.quit()

# ------------ Configuração para tentar até 3 vezes o processo, caso erre as 3 => +1 tentativa adicionada no banco ------------ #
def tentar_ate_dar_certo(funcao, tentativas=3, *args, **kwargs):
    ultimo_erro = None
    for tentativa in range(1, tentativas + 1):
        try:
            print(f"{funcao.__name__} Tentativa {tentativa}")
            funcao(*args, **kwargs)
            print(f"{funcao.__name__} Finalizada com sucesso.")
            return tentativa, None
        except Exception as erro_execucao:
            print(f"{funcao.__name__} Tentativa {tentativa} falhou: {erro_execucao}")
            ultimo_erro = erro_execucao
            sleep(5)

    print(f"{funcao.__name__} Falhou após {tentativas} tentativas.")
    return 0, ultimo_erro 
         
# ------------ Execução do codigo  ------------ #
if __name__ == "__main__":
    
    if pode_tentar("divida_ativa", data_hoje):
        sucesso, erro_final = tentar_ate_dar_certo(cnd_divida_ativa, 3)
        if sucesso:
            registrar_log("divida_ativa", 1)
        else:
            registrar_log("divida_ativa", 0)
            msg = "Falha após 3 tentativas na Certidão Dívida Ativa."
            if erro_final:
                msg += f"\nErro final: {erro_final}"
            telegram.telegram_bot(msg, ITOKEN, CHAT_ID)
        exibir_status_certidao("divida_ativa")
        sleep(3)

    if pode_tentar("fgts", data_hoje):
        sucesso, erro_final = tentar_ate_dar_certo(cnd_fgts, 3)
        if sucesso:
            registrar_log("fgts", 1)
        else:
            registrar_log("fgts", 0)
            msg = "Falha após 3 tentativas na Certidão FGTS."
            if erro_final:
                msg += f"\nErro final: {erro_final}"
            telegram.telegram_bot(msg, ITOKEN, CHAT_ID)
        exibir_status_certidao("fgts")
        sleep(3)

    if pode_tentar("trabalhista", data_hoje):
        sucesso, erro_final = tentar_ate_dar_certo(cnd_trabalhista, 3, os.path.join(os.getcwd(), 'CND - Trabalhista'))
        if sucesso:
            registrar_log("trabalhista", 1)
        else:
            registrar_log("trabalhista", 0)
            msg = "Falha após 3 tentativas na Certidão Trabalhista."
            if erro_final:
                msg += f"\nErro final: {erro_final}"
            telegram.telegram_bot(msg, ITOKEN, CHAT_ID)
        exibir_status_certidao("trabalhista")
        sleep(3)


    if pode_tentar("municipal", data_hoje):
            sucesso, erro_final = tentar_ate_dar_certo(cnd_municipal, 3)
            if sucesso:
                registrar_log("municipal", 1)
            else:
                registrar_log("municipal", 0)
                msg = "Falha após 3 tentativas na Certidão Municipal."
                if erro_final:
                    msg += f"\nErro final: {erro_final}"
                    telegram.telegram_bot(msg, ITOKEN, CHAT_ID)
            exibir_status_certidao("municipal")
            sleep(3)
