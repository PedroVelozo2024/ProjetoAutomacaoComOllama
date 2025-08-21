import win32com.client
from typing import List, Dict, Any, Set, Tuple
import time
import re
import textwrap
from colorama import Fore, Style
from datetime import datetime, timedelta
import hashlib
import pythoncom
import logging
import pandas as pd
import json
import os
from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.schema import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
import concurrent.futures
from functools import lru_cache
import threading

# Importações para banco de dados
from sqlalchemy import create_engine, Column, String, DateTime, Float, Text, Date, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Configuração do banco de dados
Base = declarative_base()

class ExportacaoDB(Base):
    """Tabela para armazenar dados de exportação"""
    __tablename__ = 'dbCONTAINER'
    
    id = Column(String(50), primary_key=True)
    data_embarque = Column(Date)  # Alterado para tipo Date
    planta_carregamento = Column(String(100))
    tipo_embarque = Column(String(50))
    temperatura = Column(String(50))
    ordem = Column(String(50), unique=True, nullable=False)  # Chave única para evitar duplicidades
    porto_saida = Column(String(100))
    porto_chegada = Column(String(100))
    companhia = Column(String(100))
    navio = Column(String(100))
    dline = Column(String(50))
    reserva_booking = Column(String(100))
    id_autorizacao = Column(String(100))
    resumo_embarque = Column(Text)
    transportador_ter = Column(String(100))
    eta = Column(Date)  # Alterado para tipo Date
    valor_pedido = Column(Numeric(15, 2))  # Alterado para tipo numérico
    data_processamento = Column(DateTime, default=datetime.now)
    data_atualizacao = Column(DateTime, default=datetime.now, onupdate=datetime.now)

# Configuração de cache para evitar recarregamentos desnecessários
@lru_cache(maxsize=1)
def inicializar_ollama():
    """Inicializa o modelo Ollama com cache"""
    try:
        llm = Ollama(
            model="llama3:8b-instruct-fp16", 
            temperature=0.1,
            num_predict=800,
            num_thread=8,  # Aumenta threads para processamento paralelo
            num_gpu=1      # Utiliza GPU se disponível
        )
        print(f"{Fore.GREEN}✓ Ollama inicializado com otimizações{Style.RESET_ALL}")
        return llm
    except Exception as e:
        print(f"{Fore.RED}Falha ao inicializar Ollama: {e}{Style.RESET_ALL}")
        return None
# Template pré-compilado para melhor performance
def criar_template_json():
    """Cria o template para extração de dados em formato JSON específico"""
    template = """
    ANALISE E EXTRAIA APENAS OS DADOS DESTE E-MAIL ESPECÍFICO sobre exportação:

    TEXTO ORIGINAL:
    {texto}

    INSTRUÇÕES ESTRITAS:
    1. Extraia APENAS informações deste e-mail específico
    2. NÃO combine, resuma ou inclua informações de outros e-mails
    3. Retorne SEMPRE em formato JSON válido com a estrutura EXATA abaixo
    4. Se um campo não existir no e-mail, deixe como string vazia ""
    5. Se não encontrar dados de exportação, retorne: {{"status": "SEM_DADOS_EXPORTACAO"}}

    INSTRUÇÕES ESPECÍFICAS PARA DATAS:
    - Para datas, use sempre o formato YYYY-MM-DD (ex: 2024-07-10)
    - Se encontrar datas no formato DD/MM/YYYY, converta para YYYY-MM-DD
    - Se a data não for clara, deixe como string vazia ""

    ESTRUTURA JSON OBRIGATÓRIA:
    {{
      "data_embarque": "",
      "planta_carregamento": "",
      "Tipo_de_embarque": "",
      "Temperatura": "",
      "Ordem": "",
      "Porto_de_saida": "",
      "Porto_de_chegada": "",
      "Companhia": "",
      "Navio": "",
      "DLine": "",
      "Reserva_(Booking)": "",
      "ID_(autorização)": "",
      "Resumo_embarque": "",
      "Transportador_Ter": "",
      "ETA": "",
      "Valor_Pedido_(R$)": ""
    }}

    IMPORTANTE: Processe apenas este e-mail individualmente.

    JSON:
    """
    
    return PromptTemplate.from_template(template)

# Compila regex patterns uma vez para melhor performance
PADROES_REMOCAO = [
    re.compile(r"Att,.*?minervafoods\.com.*?(?=\n\s*\n|\Z)", re.DOTALL|re.IGNORECASE),
    re.compile(r"Esta mensagem é endereçada exclusivamente.*?privilegiadas.*?(?=\n\s*\n|\Z)", re.DOTALL|re.IGNORECASE),
    re.compile(r"From:.*?\nTo:.*?\nSubject:.*?(?=\n\s*\n)", re.DOTALL|re.IGNORECASE),
    re.compile(r"Skype:.*?\n", re.IGNORECASE),
    re.compile(r"Telefone:.*?\n", re.IGNORECASE),
    re.compile(r"\bRamal:.*?\n", re.IGNORECASE),
    re.compile(r"\n{3,}"),
    re.compile(r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"),
    re.compile(r"\[image:.*?\]"),
    re.compile(r"<.*?>"),
    re.compile(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"),
    re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    re.compile(r"^.*?escreveu:$", re.MULTILINE),
    re.compile(r"Em \d{1,2} de [a-z]+ de \d{4}.*?escreveu:$", re.DOTALL|re.IGNORECASE),
    re.compile(r"Original Message.*?-----", re.DOTALL|re.IGNORECASE),
    re.compile(r"-----Original Message-----.*?$", re.DOTALL|re.IGNORECASE)
]

def processar_com_ollama_json(texto: str, llm, prompt_template) -> Dict[str, Any]:
    """Processa o texto usando Ollama para extração de dados em JSON"""
    if not texto.strip() or len(texto.strip()) < 50:
        return {"status": "TEXTO_INSUFICIENTE"}
    try:
        # Chain otimizada
        resultado = prompt_template.invoke({"texto": texto})
        resultado = llm.invoke(resultado.text)
        # Parse rápido do JSON
        try:
            dados_json = json.loads(resultado.strip())
            if isinstance(dados_json, dict) and "status" not in dados_json:
                return dados_json
            else:
                return {"status": "SEM_DADOS_EXPORTACAO"}
                
        except json.JSONDecodeError:
            # Busca rápida por JSON no texto
            json_match = re.search(r'\{[^{}]*\}', resultado, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group())
                except:
                    return {"erro": "JSON_INVALIDO", "raw_text": resultado[:200]}
            return {"erro": "JSON_INVALIDO", "raw_text": resultado[:200]}
    except Exception as e:
        print(f"{Fore.YELLOW}Erro no Ollama: {e}{Style.RESET_ALL}")
        return {"erro": f"ERRO_PROCESSAMENTO: {str(e)}"}

def limpar_texto_rapido(email_body: str) -> str:
    """Remove assinaturas e informações redundantes de forma otimizada"""
    if not email_body:
        return ""
    
    texto_limpo = email_body
    for padrao in PADROES_REMOCAO:
        texto_limpo = padrao.sub("", texto_limpo)
    
    # Remove linhas vazias e espaços excessivos de forma eficiente
    texto_limpo = re.sub(r'\n\s*\n', '\n\n', texto_limpo)
    texto_limpo = texto_limpo.strip()
    
    return textwrap.dedent(texto_limpo)

def processar_email_json_rapido(email_body: str, llm, prompt_template_json, assunto: str) -> Dict[str, Any]:
    """Processa o e-mail de forma otimizada"""
    if not email_body:
        return {"status": "EMAIL_VAZIO"}
        
    try:
        # Limpeza rápida
        texto_limpo = limpar_texto_rapido(email_body)
        
        if len(texto_limpo) > 80:
            contexto = f"ASSUNTO: {assunto}\n\n{texto_limpo}"
            return processar_com_ollama_json(contexto, llm, prompt_template_json)
        else:
            return {"status": "TEXTO_MUITO_CURTO"}
            
    except Exception as e:
        print(f"{Fore.YELLOW}Erro no processamento: {e}{Style.RESET_ALL}")
        return {"erro": f"ERRO_PROCESSAMENTO: {str(e)}"}

def inicializar_outlook():
    """Inicializa o Outlook com segurança"""
    try:
        pythoncom.CoInitialize()
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        print(f"{Fore.GREEN}✓ Outlook inicializado{Style.RESET_ALL}")
        return outlook
    except Exception as e:
        print(f"{Fore.RED}Falha ao inicializar Outlook: {e}{Style.RESET_ALL}")
        return None

def obter_emails_exportacao_rapido(pasta, ultima_verificacao: datetime = None):
    """Obtém e-mails de forma otimizada"""
    try:
        emails_exportacao = []
        items = pasta.Items
        
        # Ordena por data recebimento (mais recentes primeiro)
        items.Sort("[ReceivedTime]", True)
        
        for email in items:
            assunto = getattr(email, 'Subject', '').upper()
            
            if "PROGRAMAÇÃO EXPORTAÇÃO" in assunto or "PROGRAMACAO EXPORTACAO" in assunto:
                if ultima_verificacao:
                    data_email = getattr(email, 'ReceivedTime', None)
                    if data_email:
                        try:
                            if data_email.replace(tzinfo=None) >= ultima_verificacao:
                                emails_exportacao.append(email)
                        except:
                            emails_exportacao.append(email)
                else:
                    emails_exportacao.append(email)
        
        return emails_exportacao
        
    except Exception as e:
        print(f"{Fore.RED}Erro ao obter e-mails: {e}{Style.RESET_ALL}")
        return []

# Cache para JSON existente
_json_cache = None
_json_cache_time = 0

def carregar_json_existente_rapido(nome_arquivo: str) -> Dict[str, Any]:
    """Carrega o JSON existente com cache"""
    global _json_cache, _json_cache_time
    
    current_time = time.time()
    if _json_cache and current_time - _json_cache_time < 30:  # Cache por 30 segundos
        return _json_cache
    
    try:
        if os.path.exists(nome_arquivo):
            with open(nome_arquivo, 'r', encoding='utf-8') as f:
                _json_cache = json.load(f)
                _json_cache_time = current_time
                return _json_cache
    except Exception as e:
        print(f"{Fore.YELLOW}Erro ao carregar JSON: {e}{Style.RESET_ALL}")
    
    # Retorna estrutura vazia
    empty_data = {
        "metadata": {
            "processamento_timestamp": datetime.now().isoformat(),
            "total_emails_processados": 0,
            "emails_com_dados": 0,
            "ultima_atualizacao": datetime.now().isoformat(),
            "ordens_unicas": []
        },
        "emails": []
    }
    _json_cache = empty_data
    return empty_data

def obter_ordens_unicas_existentes_rapido(nome_arquivo: str) -> Set[str]:
    """Obtém ordens únicas de forma otimizada"""
    dados = carregar_json_existente_rapido(nome_arquivo)
    return set(dados["metadata"].get("ordens_unicas", []))

def verificar_duplicidade_ordem_rapida(dados_exportacao: Dict[str, Any], ordens_existentes: Set[str]) -> bool:
    """Verificação rápida de duplicidade"""
    ordem = dados_exportacao.get("Ordem", "").strip()
    return bool(ordem) and ordem in ordens_existentes

def encontrar_email_por_ordem(dados_existentes: Dict[str, Any], ordem: str) -> Tuple[int, Dict[str, Any]]:
    """Encontra um email existente pela ordem e retorna seu índice e dados"""
    for i, email in enumerate(dados_existentes["emails"]):
        if email["dados_exportacao"].get("Ordem", "").strip() == ordem:
            return i, email
    return -1, None

def comparar_datas_email(data_nova_str: str, data_existente_str: str) -> bool:
    """Compara duas datas de email e retorna True se a nova para ser mais recente"""
    try:
        # Tenta converter as datas para objetos datetime
        formato_data = "%Y-%m-%d %H:%M:%S"
        data_nova = datetime.strptime(data_nova_str, formato_data)
        data_existente = datetime.strptime(data_existente_str, formato_data)
        
        return data_nova > data_existente
    except (ValueError, TypeError):
        # Se houver erro na conversão, assume que a nova é mais recente
        return True

# Lock para escrita thread-safe
_json_lock = threading.Lock()

def salvar_json_incremental_rapido(nome_arquivo: str, email_processado: Dict[str, Any], ordens_existentes: Set[str]) -> Tuple[bool, Set[str]]:
    """Salvamento incremental otimizado com substituição de dados mais recentes"""
    global _json_cache  # Declaração global no início da função
    
    with _json_lock:  # Thread-safe
        try:
            dados_exportacao = email_processado.get("dados_exportacao", {})
            ordem = dados_exportacao.get("Ordem", "").strip()
            data_nova_email = email_processado["metadata"]["data_recebimento"]
            
            # Carrega dados atuais
            dados_existentes = carregar_json_existente_rapido(nome_arquivo)
            
            # Verifica se a ordem já existe
            if ordem and ordem in ordens_existentes:
                # Encontra o email existente com a mesma ordem
                indice_existente, email_existente = encontrar_email_por_ordem(dados_existentes, ordem)
                
                if indice_existente != -1 and email_existente:
                    data_existente_email = email_existente["metadata"]["data_recebimento"]
                    
                    # Verifica se o novo email é mais recente
                    if comparar_datas_email(data_nova_email, data_existente_email):
                        print(f"{Fore.YELLOW}🔄 ATUALIZANDO: Ordem '{ordem}' com dados mais recentes{Style.RESET_ALL}")
                        
                        # Substitui o email antigo pelo novo
                        dados_existentes["emails"][indice_existente] = email_processado
                        
                        # Atualiza metadados
                        dados_existentes["metadata"]["ultima_atualizacao"] = datetime.now().isoformat()
                        
                        # Salva de forma otimizada
                        with open(nome_arquivo, 'w', encoding='utf-8') as f:
                            json.dump(dados_existentes, f, ensure_ascii=False, indent=2)
                        
                        # Atualiza cache
                        _json_cache = dados_existentes
                        
                        return True, ordens_existentes
                    else:
                        print(f"{Fore.YELLOW}⚠️  DUPLICIDADE: Ordem '{ordem}' já existe com dados mais recentes{Style.RESET_ALL}")
                        return False, ordens_existentes
                else:
                    print(f"{Fore.YELLOW}⚠️  DUPLICIDADE: Ordem '{ordem}' já existe{Style.RESET_ALL}")
                    return False, ordens_existentes
            
            # Se não é duplicidade ou se é uma ordem nova, adiciona normalmente
            dados_existentes["emails"].append(email_processado)
            
            # Atualiza ordens se for nova
            if ordem:
                ordens_existentes.add(ordem)
                dados_existentes["metadata"]["ordens_unicas"] = list(ordens_existentes)
            
            # Atualiza metadados
            total = len(dados_existentes["emails"])
            dados_existentes["metadata"]["total_emails_processados"] = total
            dados_existentes["metadata"]["emails_com_dados"] = sum(
                1 for email in dados_existentes["emails"] 
                if email['dados_exportacao'].get('status') != 'SEM_DADOS_EXPORTACAO' and
                not email['dados_exportacao'].get('erro')
            )
            dados_existentes["metadata"]["ultima_atualizacao"] = datetime.now().isoformat()
            
            # Salva de forma otimizada
            with open(nome_arquivo, 'w', encoding='utf-8') as f:
                json.dump(dados_existentes, f, ensure_ascii=False, indent=2)
            
            # Atualiza cache
            _json_cache = dados_existentes
            
            return True, ordens_existentes
            
        except Exception as e:
            print(f"{Fore.RED}Erro ao salvar JSON: {e}{Style.RESET_ALL}")
            return False, ordens_existentes

def converter_para_data(data_str):
    """
    Converte string para objeto date do Python
    Suporta múltiplos formatos de data
    """
    if not data_str or pd.isna(data_str) or data_str == "":
        return None
    
    # Remove espaços e converte para string
    data_str = str(data_str).strip()
    
    # Lista de formatos possíveis
    formatos_data = [
        '%Y-%m-%d',          # 2024-01-15
        '%d/%m/%Y',          # 15/01/2024
        '%d-%m-%Y',          # 15-01-2024
        '%m/%d/%Y',          # 01/15/2024 (formato americano)
        '%Y/%m/%d',          # 2024/01/15
        '%d.%m.%Y',          # 15.01.2024
        '%d %b %Y',          # 15 Jan 2024
        '%d %B %Y',          # 15 January 2024
    ]
    
    for formato in formatos_data:
        try:
            return datetime.strptime(data_str, formato).date()
        except ValueError:
            continue
    
    # Se nenhum formato funcionar, tenta parser mais flexível
    try:
        from dateutil import parser
        return parser.parse(data_str, dayfirst=True).date()
    except:
        print(f"{Fore.YELLOW}⚠️  Não foi possível converter a data: '{data_str}'{Style.RESET_ALL}")
        return None

def converter_para_decimal(valor_str):
    """
    Converte string para valor decimal/número
    """
    if not valor_str or pd.isna(valor_str) or valor_str == "":
        return None
    
    try:
        # Remove caracteres não numéricos (R$, pontos, espaços, etc.)
        valor_limpo = re.sub(r'[^\d,]', '', str(valor_str))
        valor_limpo = valor_limpo.replace(',', '.')
        
        return float(valor_limpo) if valor_limpo else None
    except:
        print(f"{Fore.YELLOW}⚠️  Não foi possível converter o valor: '{valor_str}'{Style.RESET_ALL}")
        return None

def inicializar_banco_dados():
    """Inicializa o banco de dados SQLite"""
    try:
        # Cria diretório para banco de dados se não existir
        os.makedirs('data', exist_ok=True)
        
        # Configuração do banco de dados
        engine = create_engine('sqlite:///data/exportacao.db', echo=False)
        Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        print(f"{Fore.GREEN}✓ Banco de dados inicializado com sucesso{Style.RESET_ALL}")
        return session, engine
        
    except Exception as e:
        print(f"{Fore.RED}Erro ao inicializar banco de dados: {e}{Style.RESET_ALL}")
        return None, None

def sincronizar_json_para_banco(nome_arquivo_json: str, session):
    """Sincroniza dados do JSON para o banco de dados com tratamento de tipos"""
    try:
        # Carrega dados do JSON
        dados_json = carregar_json_existente_rapido(nome_arquivo_json)
        
        # Contadores para estatísticas
        total_inseridos = 0
        total_atualizados = 0
        total_ignorados = 0
        total_erros = 0
        
        for email in dados_json["emails"]:
            dados_exportacao = email.get("dados_exportacao", {})
            
            # Pula registros sem dados válidos
            if dados_exportacao.get("status") == "SEM_DADOS_EXPORTACAO" or dados_exportacao.get("erro"):
                total_ignorados += 1
                continue
            
            ordem = dados_exportacao.get("Ordem", "").strip()
            if not ordem:
                total_ignorados += 1
                continue
            
            try:
                # Converter dados para tipos corretos
                data_embarque = converter_para_data(dados_exportacao.get("data_embarque", ""))
                eta = converter_para_data(dados_exportacao.get("ETA", ""))
                valor_pedido = converter_para_decimal(dados_exportacao.get("Valor_Pedido_(R$)", ""))
                
                # Verifica se a ordem já existe no banco
                registro_existente = session.query(ExportacaoDB).filter_by(ordem=ordem).first()
                
                if registro_existente:
                    # Atualiza registro existente
                    registro_existente.data_embarque = data_embarque
                    registro_existente.planta_carregamento = dados_exportacao.get("planta_carregamento", "")
                    registro_existente.tipo_embarque = dados_exportacao.get("Tipo_de_embarque", "")
                    registro_existente.temperatura = dados_exportacao.get("Temperatura", "")
                    registro_existente.porto_saida = dados_exportacao.get("Porto_de_saida", "")
                    registro_existente.porto_chegada = dados_exportacao.get("Porto_de_chegada", "")
                    registro_existente.companhia = dados_exportacao.get("Companhia", "")
                    registro_existente.navio = dados_exportacao.get("Navio", "")
                    registro_existente.dline = dados_exportacao.get("DLine", "")
                    registro_existente.reserva_booking = dados_exportacao.get("Reserva_(Booking)", "")
                    registro_existente.id_autorizacao = dados_exportacao.get("ID_(autorização)", "")
                    registro_existente.resumo_embarque = dados_exportacao.get("Resumo_embarque", "")
                    registro_existente.transportador_ter = dados_exportacao.get("Transportador_Ter", "")
                    registro_existente.eta = eta
                    registro_existente.valor_pedido = valor_pedido
                    
                    total_atualizados += 1
                    print(f"{Fore.YELLOW}🔄 Atualizando ordem no banco: {ordem}{Style.RESET_ALL}")
                    
                else:
                    # Cria novo registro
                    novo_registro = ExportacaoDB(
                        id=hashlib.md5(ordem.encode()).hexdigest()[:50],
                        data_embarque=data_embarque,
                        planta_carregamento=dados_exportacao.get("planta_carregamento", ""),
                        tipo_embarque=dados_exportacao.get("Tipo_de_embarque", ""),
                        temperatura=dados_exportacao.get("Temperatura", ""),
                        ordem=ordem,
                        porto_saida=dados_exportacao.get("Porto_de_saida", ""),
                        porto_chegada=dados_exportacao.get("Porto_de_chegada", ""),
                        companhia=dados_exportacao.get("Companhia", ""),
                        navio=dados_exportacao.get("Navio", ""),
                        dline=dados_exportacao.get("DLine", ""),
                        reserva_booking=dados_exportacao.get("Reserva_(Booking)", ""),
                        id_autorizacao=dados_exportacao.get("ID_(autorização)", ""),
                        resumo_embarque=dados_exportacao.get("Resumo_embarque", ""),
                        transportador_ter=dados_exportacao.get("Transportador_Ter", ""),
                        eta=eta,
                        valor_pedido=valor_pedido
                    )
                    
                    session.add(novo_registro)
                    total_inseridos += 1
                    print(f"{Fore.GREEN}✅ Inserindo nova ordem no banco: {ordem}{Style.RESET_ALL}")
            
            except Exception as e:
                total_erros += 1
                print(f"{Fore.RED}❌ Erro ao processar ordem {ordem}: {e}{Style.RESET_ALL}")
                continue
        
        # Commit das alterações
        session.commit()
        
        print(f"{Fore.CYAN}📊 Sincronização concluída:{Style.RESET_ALL}")
        print(f"   Inseridos: {total_inseridos}")
        print(f"   Atualizados: {total_atualizados}")
        print(f"   Ignorados: {total_ignorados}")
        print(f"   Erros: {total_erros}")
        
        return True
        
    except IntegrityError:
        session.rollback()
        print(f"{Fore.RED}❌ Erro de integridade: ordem duplicada no banco de dados{Style.RESET_ALL}")
        return False
    except Exception as e:
        session.rollback()
        print(f"{Fore.RED}❌ Erro na sincronização com banco de dados: {e}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()
        return False

def processar_lote_emails(emails_lote: List, llm, prompt_template_json, dados_existentes: Dict, ordens_unicas: Set, nome_arquivo: str, session_db) -> Tuple[int, int, int]:
    """Processa um lote de emails em paralelo"""
    emails_processados = 0
    emails_duplicados = 0
    emails_atualizados = 0
    
    for email in emails_lote:
        corpo = getattr(email, 'Body', '')
        assunto = getattr(email, 'Subject', 'Sem assunto')
        data = getattr(email, 'ReceivedTime', datetime.now())
        remetente = getattr(email, 'SenderEmailAddress', '')
        
        print(f"{Fore.GREEN}Processando: {assunto[:50]}...{Style.RESET_ALL}")
        
        if llm:
            dados_exportacao = processar_email_json_rapido(corpo, llm, prompt_template_json, assunto)
        else:
            dados_exportacao = {"status": "PROCESSAMENTO_AI_INDISPONIVEL"}
        
        # Converte data para string formatada
        if hasattr(data, 'strftime'):
            data_str = data.strftime("%Y-%m-%d %H:%M:%S")
        else:
            data_str = str(data)
        
        email_processado = {
            "metadata": {
                "numero_email": dados_existentes['metadata']['total_emails_processados'] + emails_processados + 1,
                "assunto": assunto,
                "data_recebimento": data_str,
                "endereco_email_remetente": remetente,
                "processamento_timestamp": datetime.now().isoformat()
            },
            "dados_exportacao": dados_exportacao
        }
        
        # Verifica se é uma atualização
        ordem = dados_exportacao.get("Ordem", "").strip()
        if ordem and ordem in ordens_unicas:
            indice_existente, email_existente = encontrar_email_por_ordem(dados_existentes, ordem)
            if indice_existente != -1 and email_existente:
                data_existente = email_existente["metadata"]["data_recebimento"]
                if comparar_datas_email(data_str, data_existente):
                    emails_atualizados += 1
        
        sucesso, ordens_unicas = salvar_json_incremental_rapido(nome_arquivo, email_processado, ordens_unicas)
        
        if sucesso:
            emails_processados += 1
            print(f"{Fore.CYAN}✓ Adicionado ({emails_processados}){Style.RESET_ALL}")
            
            # Sincroniza com banco de dados após cada inserção
            if session_db:
                sincronizar_json_para_banco(nome_arquivo, session_db)
        else:
            emails_duplicados += 1
    
    return emails_processados, emails_duplicados, emails_atualizados

def carregar_ultima_verificacao() -> datetime:
    """Carrega a data/hora da última verificação dos e-mails processados."""
    nome_arquivo = "ultima_verificacao.json"
    if os.path.exists(nome_arquivo):
        try:
            with open(nome_arquivo, "r", encoding="utf-8") as f:
                dados = json.load(f)
                valor = dados.get("ultima_verificacao")
                if valor:
                    return datetime.fromisoformat(valor)
        except Exception as e:
            print(f"{Fore.YELLOW}Erro ao carregar última verificação: {e}{Style.RESET_ALL}")
    # Se não existir, retorna None
    return None

def salvar_ultima_verificacao():
    """Salva a data/hora da última verificação dos e-mails processados."""
    nome_arquivo = "ultima_verificacao.json"
    try:
        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump({"ultima_verificacao": datetime.now().isoformat()}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"{Fore.YELLOW}Erro ao salvar última verificação: {e}{Style.RESET_ALL}")

def monitorar_novos_emails_continuamente(outlook, llm, prompt_template_json, nome_arquivo_json, session_db, intervalo_verificacao=60):
    """Monitora continuamente por novos emails de exportação"""
    print(f"{Fore.CYAN}🚀 INICIANDO MONITORAMENTO CONTÍNUO...{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Verificando novos emails a cada {intervalo_verificacao} segundos{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Pressione Ctrl+C para parar o monitoramento{Style.RESET_ALL}")
    
    try:
        while True:
            # Carrega dados atuais
            dados_existentes = carregar_json_existente_rapido(nome_arquivo_json)
            ordens_unicas = obter_ordens_unicas_existentes_rapido(nome_arquivo_json)
            
            # Obtém a última verificação
            ultima_verificacao = carregar_ultima_verificacao()
            
            # Busca novos emails desde a última verificação
            inbox = outlook.GetDefaultFolder(6)
            novos_emails = obter_emails_exportacao_rapido(inbox, ultima_verificacao)
            
            if novos_emails:
                print(f"{Fore.GREEN}📨 Novos emails encontrados: {len(novos_emails)}{Style.RESET_ALL}")
                
                # Processa os novos emails
                processados, duplicados, atualizados = processar_lote_emails(
                    novos_emails, llm, prompt_template_json, dados_existentes, ordens_unicas, nome_arquivo_json, session_db
                )
                
                # Atualiza a última verificação
                salvar_ultima_verificacao()
                
                print(f"{Fore.CYAN}📊 Estatísticas desta verificação:{Style.RESET_ALL}")
                print(f"   Novos processados: {processados}")
                print(f"   Atualizações: {atualizados}")
                print(f"   Duplicados: {duplicados}")
            else:
                print(f"{Fore.BLUE}⏰ Nenhum novo email encontrado. Próxima verificação em {intervalo_verificacao} segundos...{Style.RESET_ALL}")
            
            # Aguarda o próximo ciclo
            time.sleep(intervalo_verificacao)
            
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}⏹️  Monitoramento interrompido pelo usuário{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}❌ Erro no monitoramento: {e}{Style.RESET_ALL}")

def pipeline_principal_otimizada(processar_todos: bool = False, modo_monitoramento: bool = False):
    """Fluxo principal otimizado"""
    print(f"{Fore.CYAN}=== SISTEMA OTIMIZADO DE PROCESSAMENTO ==={Style.RESET_ALL}")
    
    nome_arquivo_json = "programacao_exportacao.json"
    
    # Inicializações otimizadas
    llm = inicializar_ollama()
    prompt_template_json = criar_template_json() if llm else None
    outlook = inicializar_outlook()
    
    # Inicializa banco de dados
    session_db, engine_db = inicializar_banco_dados()
    
    if not outlook:
        return

    try:
        # Sincroniza dados existentes do JSON para o banco
        if session_db:
            print(f"{Fore.YELLOW}🔄 Sincronizando dados existentes com banco de dados...{Style.RESET_ALL}")
            sincronizar_json_para_banco(nome_arquivo_json, session_db)
        
        if modo_monitoramento:
            # Modo de monitoramento contínuo
            monitorar_novos_emails_continuamente(outlook, llm, prompt_template_json, nome_arquivo_json, session_db)
        else:
            # Modo de processamento único (comportamento original)
            inbox = outlook.GetDefaultFolder(6)
            ultima_verificacao = None if processar_todos else carregar_ultima_verificacao()
            
            print(f"{Fore.YELLOW}Buscando e-mails...{Style.RESET_ALL}")
            emails = obter_emails_exportacao_rapido(inbox, ultima_verificacao)
            
            if not emails:
                print(f"{Fore.YELLOW}Nenhum e-mail encontrado.{Style.RESET_ALL}")
                return
                
            print(f"{Fore.WHITE}E-mails para processar: {len(emails)}{Style.RESET_ALL}")
            
            # Carrega dados de forma otimizada
            dados_existentes = carregar_json_existente_rapido(nome_arquivo_json)
            ordens_unicas = obter_ordens_unicas_existentes_rapido(nome_arquivo_json)
            
            total_processados = 0
            total_duplicados = 0
            total_atualizados = 0
            
            # Processa em lotes para melhor performance
            tamanho_lote = 5  # Ajuste conforme necessidade
            lotes = [emails[i:i + tamanho_lote] for i in range(0, len(emails), tamanho_lote)]
            
            for i, lote in enumerate(lotes, 1):
                print(f"{Fore.BLUE}Processando lote {i}/{len(lotes)}...{Style.RESET_ALL}")
                
                processados, duplicados, atualizados = processar_lote_emails(
                    lote, llm, prompt_template_json, dados_existentes, ordens_unicas, nome_arquivo_json, session_db
                )
                
                total_processados += processados
                total_duplicados += duplicados
                total_atualizados += atualizados
                
                # Atualiza dados existentes após cada lote
                dados_existentes = carregar_json_existente_rapido(nome_arquivo_json)
                
                if i < len(lotes) and llm:
                    print(f"{Fore.YELLOW}Pausa breve...{Style.RESET_ALL}")
                    time.sleep(1)
            
            # Salva última verificação
            salvar_ultima_verificacao()
            
            # Estatísticas finais
            dados_finais = carregar_json_existente_rapido(nome_arquivo_json)
            
            print(f"\n{Fore.GREEN}✅ PROCESSAMENTO CONCLUÍDO!{Style.RESET_ALL}")
            print(f"{Fore.WHITE}Novos e-mails: {total_processados}")
            print(f"Atualizações: {total_atualizados}")
            print(f"Duplicados ignorados: {total_duplicados}")
            print(f"Total no JSON: {dados_finais['metadata']['total_emails_processados']}")
            print(f"Com dados válidos: {dados_finais['metadata']['emails_com_dados']}")
            print(f"Ordens únicas: {len(ordens_unicas)}{Style.RESET_ALL}")
        
    except Exception as e:
        print(f"{Fore.RED}Erro no processamento: {e}{Style.RESET_ALL}")
    finally:
        # Fecha sessão do banco de dados
        if session_db:
            session_db.close()
        
        if not modo_monitoramento:
            pythoncom.CoUninitialize()

    def atualizar_planilha_excel_com_banco():
        """
        Função para atualizar a planilha Excel com dados do banco de dados
        """
    try:
        # Configurações
        nome_arquivo_excel = "CTNR 07 (1).xlsx"
        nome_aba = "MINERVA 2.0"
        nome_tabela = "Tabela1"
        
        # Mapeamento de colunas entre banco e Excel
        mapeamento_colunas = {
            'data_embarque': 'DT EMBARQUE',
            'planta_carregamento': 'UNIDADE', 
            'Tipo_de_embarque': 'TIPO EMBARQUE',
            'Temperatura': 'TEMPERATURA',
            'ordem': 'CONTRATO',
            'porto_saida': 'PORTO SAIDA',
            'porto_chegada': 'PORTO CHEGADA', 
            'companhia': 'ARMADOR',
            'navio': 'NAVIO',
            'dline': 'DEADLINE',
            'reserva_booking': 'BOOKING'
        }
        
        print(f"{Fore.CYAN}📊 INICIANDO ATUALIZAÇÃO DA PLANILHA EXCEL{Style.RESET_ALL}")
        
        # 1. Conectar ao banco de dados
        engine_banco = create_engine('sqlite:///data/exportacao.db')
        Session = sessionmaker(bind=engine_banco)
        session = Session()
        
        # 2. Ler dados do banco
        query = session.query(ExportacaoDB).all()
        dados_banco = pd.DataFrame([{
            'data_embarque': item.data_embarque,
            'planta_carregamento': item.planta_carregamento,
            'Tipo_de_embarque': item.tipo_embarque,
            'Temperatura': item.temperatura,
            'ordem': item.ordem,
            'porto_saida': item.porto_saida,
            'porto_chegada': item.porto_chegada,
            'companhia': item.companhia,
            'navio': item.navio,
            'dline': item.dline,
            'reserva_booking': item.reserva_booking
        } for item in query])
        
        session.close()
        
        if dados_banco.empty:
            print(f"{Fore.YELLOW}⚠️  Nenhum dado encontrado no banco de dados{Style.RESET_ALL}")
            return
        
        print(f"{Fore.GREEN}✅ Dados do banco carregados: {len(dados_banco)} registros{Style.RESET_ALL}")
        
        # 3. Ler a planilha Excel
        if not os.path.exists(nome_arquivo_excel):
            print(f"{Fore.RED}❌ Arquivo Excel não encontrado: {nome_arquivo_excel}{Style.RESET_ALL}")
            return
        
        # Ler a aba específica
        df_excel = pd.read_excel(nome_arquivo_excel, sheet_name=nome_aba)
        
        # Verificar se a coluna de ordem (CONTRATO) existe
        coluna_ordem_excel = mapeamento_colunas['ordem']
        if coluna_ordem_excel not in df_excel.columns:
            print(f"{Fore.RED}❌ Coluna '{coluna_ordem_excel}' não encontrada no Excel{Style.RESET_ALL}")
            return
        
        print(f"{Fore.GREEN}✅ Planilha Excel carregada: {len(df_excel)} registros{Style.RESET_ALL}")
        
        # 4. Preparar dados para merge
        # Renomear colunas do banco para os nomes do Excel
        dados_banco_renomeado = dados_banco.rename(columns=mapeamento_colunas)
        
        # Garantir que a coluna de ordem seja string em ambos os DataFrames
        df_excel[coluna_ordem_excel] = df_excel[coluna_ordem_excel].astype(str)
        dados_banco_renomeado[coluna_ordem_excel] = dados_banco_renomeado[coluna_ordem_excel].astype(str)
        
        # 5. Identificar ordens existentes e novas
        ordens_excel = set(df_excel[coluna_ordem_excel].dropna().unique())
        ordens_banco = set(dados_banco_renomeado[coluna_ordem_excel].dropna().unique())
        
        ordens_para_atualizar = ordens_excel.intersection(ordens_banco)
        ordens_para_adicionar = ordens_banco - ordens_excel
        
        print(f"{Fore.CYAN}📈 Estatísticas:{Style.RESET_ALL}")
        print(f"   Ordens no Excel: {len(ordens_excel)}")
        print(f"   Ordens no Banco: {len(ordens_banco)}")
        print(f"   Ordens para atualizar: {len(ordens_para_atualizar)}")
        print(f"   Ordens para adicionar: {len(ordens_para_adicionar)}")
        
        # 6. Atualizar registros existentes
        if ordens_para_atualizar:
            print(f"{Fore.YELLOW}🔄 Atualizando registros existentes...{Style.RESET_ALL}")
            
            # Filtrar dados do banco que existem no Excel
            dados_para_atualizar = dados_banco_renomeado[
                dados_banco_renomeado[coluna_ordem_excel].isin(ordens_para_atualizar)
            ]
            
            # Remover registros antigos do Excel
            df_excel = df_excel[~df_excel[coluna_ordem_excel].isin(ordens_para_atualizar)]
            
            # Adicionar dados atualizados
            df_excel = pd.concat([df_excel, dados_para_atualizar], ignore_index=True)
            
            print(f"{Fore.GREEN}✅ {len(dados_para_atualizar)} registros atualizados{Style.RESET_ALL}")
        
        # 7. Adicionar novos registros
        if ordens_para_adicionar:
            print(f"{Fore.BLUE}➕ Adicionando novos registros...{Style.RESET_ALL}")
            
            # Filtrar dados do banco que não existem no Excel
            dados_para_adicionar = dados_banco_renomeado[
                dados_banco_renomeado[coluna_ordem_excel].isin(ordens_para_adicionar)
            ]
            
            # Adicionar novos dados ao Excel
            df_excel = pd.concat([df_excel, dados_para_adicionar], ignore_index=True)
            
            print(f"{Fore.GREEN}✅ {len(dados_para_adicionar)} novos registros adicionados{Style.RESET_ALL}")
        
        # 8. Salvar a planilha atualizada
        # Usar ExcelWriter para preservar outras abas e formatações
        with pd.ExcelWriter(nome_arquivo_excel, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            df_excel.to_excel(writer, sheet_name=nome_aba, index=False)
        
        print(f"{Fore.GREEN}🎯 Planilha atualizada com sucesso!{Style.RESET_ALL}")
        print(f"{Fore.CYAN}📊 Total de registros na planilha: {len(df_excel)}{Style.RESET_ALL}")
        
        return True
        
    except Exception as e:
        print(f"{Fore.RED}❌ Erro ao atualizar planilha: {e}{Style.RESET_ALL}")
        return False

def atualizar_planilha_avancado():
    """
    Versão mais avançada com mais funcionalidades do pandas
    """
    try:
        nome_arquivo_excel = "CTNR 07 (1).xlsx"
        nome_aba = "MINERVA 2.0"
        
        # Mapeamento completo
        mapeamento = {
            'data_embarque': 'DT EMBARQUE',
            'planta_carregamento': 'UNIDADE', 
            'Tipo_de_embarque': 'TIPO EMBARQUE',
            'Temperatura': 'TEMPERATURA',
            'ordem': 'CONTRATO',
            'porto_saida': 'PORTO SAIDA',
            'porto_chegada': 'PORTO CHEGADA', 
            'companhia': 'ARMADOR',
            'navio': 'NAVIO',
            'dline': 'DEADLINE',
            'reserva_booking': 'BOOKING'
        }
        
        # 1. Ler dados do banco
        engine = create_engine('sqlite:///data/exportacao.db')
        query = "SELECT * FROM dbCONTAINER"
        dados_banco = pd.read_sql(query, engine)
        
        if dados_banco.empty:
            print(f"{Fore.YELLOW}⚠️  Nenhum dado no banco{Style.RESET_ALL}")
            return
        
        # 2. Ler Excel preservando formatação
        with pd.ExcelFile(nome_arquivo_excel, engine='openpyxl') as xls:
            df_excel = pd.read_excel(xls, sheet_name=nome_aba)
            
            # Preservar outras abas
            outras_abas = {sheet: pd.read_excel(xls, sheet_name=sheet) 
                          for sheet in xls.sheet_names if sheet != nome_aba}
        
        coluna_ordem = mapeamento['ordem']
        
        # 3. Merge avançado usando pandas
        # Preparar dados
        dados_banco_preparados = (dados_banco
            .rename(columns=mapeamento)
            .astype({coluna_ordem: str})
            .drop_duplicates(subset=[coluna_ordem])
        )
        
        df_excel[coluna_ordem] = df_excel[coluna_ordem].astype(str)
        
        # 4. Fazer merge para identificar ações necessárias
        merge_info = pd.merge(
            df_excel[[coluna_ordem]], 
            dados_banco_preparados[[coluna_ordem]], 
            on=coluna_ordem, 
            how='outer', 
            indicator=True
        )
        
        # 5. Separar em diferentes ações
        apenas_excel = merge_info[merge_info['_merge'] == 'left_only'][coluna_ordem]
        apenas_banco = merge_info[merge_info['_merge'] == 'right_only'][coluna_ordem]
        em_ambos = merge_info[merge_info['_merge'] == 'both'][coluna_ordem]
        
        # 6. Processar cada caso
        # Manter dados que só existem no Excel
        df_final = df_excel[df_excel[coluna_ordem].isin(apenas_excel)]
        
        # Atualizar dados que existem em ambos
        if not em_ambos.empty:
            dados_atualizados = dados_banco_preparados[
                dados_banco_preparados[coluna_ordem].isin(em_ambos)
            ]
            df_final = pd.concat([df_final, dados_atualizados], ignore_index=True)
        
        # Adicionar dados que só existem no banco
        if not apenas_banco.empty:
            dados_novos = dados_banco_preparados[
                dados_banco_preparados[coluna_ordem].isin(apenas_banco)
            ]
            df_final = pd.concat([df_final, dados_novos], ignore_index=True)
        
        # 7. Ordenar por ordem (contrato)
        df_final = df_final.sort_values(by=coluna_ordem).reset_index(drop=True)
        
        # 8. Salvar preservando outras abas
        with pd.ExcelWriter(nome_arquivo_excel, engine='openpyxl') as writer:
            # Salvar aba principal atualizada
            df_final.to_excel(writer, sheet_name=nome_aba, index=False)
            
            # Salvar outras abas
            for sheet_name, sheet_data in outras_abas.items():
                sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"{Fore.GREEN}✅ Planilha atualizada com sucesso!{Style.RESET_ALL}")
        print(f"{Fore.CYAN}📊 Estatísticas:{Style.RESET_ALL}")
        print(f"   Total registros: {len(df_final)}")
        print(f"   Mantidos do Excel: {len(apenas_excel)}")
        print(f"   Atualizados: {len(em_ambos)}")
        print(f"   Adicionados: {len(apenas_banco)}")
        
        return True
        
    except Exception as e:
        print(f"{Fore.RED}❌ Erro na atualização avançada: {e}{Style.RESET_ALL}")
        return False

# Adicionar esta função ao seu pipeline principal
def pipeline_completa_com_excel(processar_todos=False, modo_monitoramento=False, atualizar_excel=True):
    """
    Pipeline completa incluindo atualização do Excel
    """
    # Executar o processamento principal
    pipeline_principal_otimizada(processar_todos, modo_monitoramento)
    
    # Atualizar Excel se solicitado
    if atualizar_excel:
        print(f"\n{Fore.CYAN}=== ATUALIZANDO PLANILHA EXCEL ==={Style.RESET_ALL}")
        sucesso = atualizar_planilha_avancado()
        
        if sucesso:
            print(f"{Fore.GREEN}✅ Processo completo concluído!{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}⚠️  Processo concluído com avisos na planilha{Style.RESET_ALL}")

# Função para ser chamada manualmente se necessário
def atualizar_excel_manual():
    """Função para atualizar o Excel manualmente"""
    return atualizar_planilha_avancado()

if __name__ == "__main__":
    # Medição de tempo
    inicio = time.time()
    # Para modo contínuo, use: pipeline_principal_otimizada(modo_monitoramento=True)
    # Para processamento único, use: pipeline_principal_otimizada(processar_todos=False)
    pipeline_completa_com_excel(modo_monitoramento=True, atualizar_excel=True)
    fim = time.time()
    print(f"{Fore.CYAN}⏰ Tempo total: {fim - inicio:.2f} segundos{Style.RESET_ALL}")   