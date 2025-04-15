from utils.call_athena import AthenaQueryExecutor
import pandas as pd
from openai import OpenAI
import os
import openai
import os
import concurrent.futures
import importlib
import uuid
from utils.token_count import TokenCounter
from utils.call_data_catalog import TablesDataCatalog
from dotenv import load_dotenv

load_dotenv()

config = {
    "client_id": os.getenv("APP_CLIENT_ID"),
    "client_secret": os.getenv("APP_CLIENT_SECRET"),
    "access_key": os.getenv("ACCESS_KEY"),
    "environment": os.getenv("ENVIRONMENT")
}

llm = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
modelo = "gpt-4"

# Garante que cada sessão tenha um ID único
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

def execute_query_with_timeout(table, date_attribute, database, output_location, workgroup) -> pd.DataFrame:
    TIMEOUT = 65
    def execute_query():
        query_executor = AthenaQueryExecutor()
        db = f"{database}"
        # query = f"SELECT * FROM `{database}`.`{table}` limit 1000;"
        query = f"""
        SELECT *
        FROM `{database}`.`{table}`
        WHERE CAST({date_attribute} AS DATE) BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}'
        """
        print(query)
        result = query_executor.run_athena_query(query, db, output_location, workgroup)
        if result is not None:
            return result
        else:
            return None
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(execute_query)
            return future.result(timeout=TIMEOUT)
    except concurrent.futures.TimeoutError:
        spinner._exit()  # None, None, None
        st.error("Erro: Tempo limite excedido ao executar a consulta. Por favor, diminua o período dos filtros entre as datas e tente novamente.")
        print(f"Erro: Tempo limite de {TIMEOUT} segundos excedido ao executar a consulta.")
        st.stop()
        return None
    except Exception as e:
        print(f"Erro ao executar a consulta: {e}", flush=True)
        st.error(f"Erro: Ocorreu um erro ao executar a consulta: {e}")
        st.stop()
        return None

def generate_attribute_filter(llm, schema_str):
    try:
        prompt_attribute = f"""
- Você está atuando como especialista em análise de dados e deve identificar o melhor atributo relacionado a datas para ser utilizado em uma consulta SQL.

**Regras:**
- Analise o schema da tabela fornecido abaixo para identificar os atributos disponíveis.
- Considere apenas os atributos que possuem os seguintes tipos: 'datetime64[ns]', 'datetime64[ns]', 'timedelta64' ou similares.
- Caso existam múltiplos atributos relacionados a datas, selecione o mais relevante para consultas temporais, COMO filtros por período.
- Se não houver atributos relacionados a datas, retorne None.

{schema_str}

- Gere apenas o *nome do atributo* analisado, sem explicações adicionais.
"""
        # Conta o número de tokens do generate_attribute_filter
        counter = TokenCounter(model="GPT-4")
        num_tokens_attribute_filter = counter.count_tokens(prompt_attribute)

        # O LLM consulta o schema e gera o atributo de data
        resposta_llm = llm.invoke(prompt_attribute)
        attribute_filter = resposta_llm.content.strip()
        attribute_filter = re.sub(r"['\"]", "", attribute_filter)
        print(f"Antes: {attribute_filter}")
        print(f"Depois: {attribute_filter}")
        return attribute_filter, num_tokens_attribute_filter

    except Exception as e:
        print(f"Erro inesperado ao processar a consulta: {e}", flush=True)
        return "Desculpe, ocorreu um erro inesperado ao processar sua pergunta. Por favor, tente novamente."

def generate_query(llm, user_question, schema_str):
    try:
        prefixo = f"""
- Você está atuando como analista de dados e deve gerar consultas Pandas para responder perguntas sobre um DataFrame.
- Se a pergunta do usuário mencionar termos de dataset, base, base de dado, dados ou qualquer outra coisa relacionada
a dados, ela está se referindo ao dataframe.
- Caso a pergunta contenha elementos ou dados que não tenham contexto com as informações do DataSet, Retorne None

**Regras:**
- Você deve considerar os tipos dos dados para gerar a consulta corretamente.
- Se as colunas for do tipo 'object', não deve gerar a consulta com operações aritméticas.
- Você deve utilizar o seguinte schema do Dataset para gerar as consultas:
{schema_str}

- Algumas colunas podem conter *valores nulos*. Esses valores já foram preenchidos automaticamente:
- Strings vazias foram substituídas por "Desconhecido".
- Valores numéricos nulos foram substituídos por 0.

- As perguntas podem ter *contexto baseado no histórico da conversa*.
- Sempre leve em consideração o histórico da conversa anteriormente.

- Gere somente a query Pandas, sem explicações adicionais.

**Restrições:** A consulta *NÃO pode modificar os dados* (exemplo: 'DROP', 'DELETE', 'ALTER', etc.).
"""
        sufixo = f"""
- Agora gere uma consulta Pandas para a seguinte pergunta, considerando o histórico da conversa:
{user_question}
"""
        # # Mantém apenas as últimas 10 interações no histórico
        # if len(st.session_state.historico) > 10:
        #     st.session_state.historico = st.session_state.historico[-10:]
        prompt_historico = "\n".join([f"{msg['role']}: {msg['content']}" for msg in st.session_state.get('messages', [])])
        print("prompt_historico:", prompt_historico)
        prompt_completo = f"{prefixo}\n\nHistórico da Conversa:\n{prompt_historico}\n\n{sufixo.format(user_question=user_question)}"
        print("prompt_completo:", prompt_completo)

        # Conta o número de tokens em generate_query
        counter = TokenCounter(model="GPT-4")
        num_tokens_generate_query = counter.count_tokens(prompt_completo)

        # O LLM gera a consulta Pandas
        resposta_llm = llm.invoke(prompt_completo)
        query_pandas = resposta_llm.content.strip()
        # Remover o código Markdown
        query_pandas = re.sub(r"``````", r"\1", query_pandas, flags=re.DOTALL)
        print("query_pandas:", query_pandas)

        return query_pandas, num_tokens_generate_query

    except Exception as e:
        print(f"Erro ao processar query: {e}", flush=True)
        return "Desculpe, ocorreu um erro ao processar sua pergunta. Por favor, tente novamente."
def ask_question(llm, query_pandas, response, schema_str):
    try:
        # Enriquecimento da resposta com LLM
        explanation_prompt = f"""
            Você gerou a seguinte consulta Pandas:

            - O esquema do Dataset é o seguinte:
            {schema_str}

            - O resultado foi:
            {response}

            - Você está atuando como especialista em dados bancários e deve fornecer insights de forma resumida
            para o usuário com base nos resultados obtidos.
            - Não é necessário explicar a query, apenas o resultado.
        """
        resposta_llm = llm.invoke(explanation_prompt)
        return resposta_llm.content.strip()
    except Exception as e:
        print(f"Erro ao processar explicação: {e}", flush=True)
        return "Desculpe, ocorreu um erro ao processar a explicação. Por favor, tente novamente."
