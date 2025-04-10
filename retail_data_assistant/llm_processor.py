import json
import os
import streamlit as st
from openai import OpenAI
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LLMProcessor:
    def __init__(self):
        # Load OpenAI API key from Streamlit secrets
        try:
            self.api_key = st.secrets["openai"]["api_key"]
        except Exception as e:
            logger.error(f"Failed to load OpenAI API key: {e}")
            st.error("OpenAI API key not found in secrets. Please set up your .streamlit/secrets.toml file.")
            self.api_key = None
        
        self.client = OpenAI(api_key=self.api_key) if self.api_key else None
        
        # Load metadata
        self.metadata_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "metadata")
        self.schema = self._load_json("schema.json")
        self.dictionary = self._load_json("dictionary.json")
        self.query_examples = self._load_json("query_examples.json")
        
        logger.info("LLM Processor initialized")

    def _load_json(self, filename):
        """Load JSON file from metadata directory."""
        try:
            file_path = os.path.join(self.metadata_dir, filename)
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            logger.warning(f"File not found: {file_path}")
            return {}
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return {}

    def generate_sql(self, user_query):
        """Generate SQL query from natural language query using LLM."""
        if not self.client:
            raise Exception("OpenAI client not initialized. Check your API key.")
        
        # Prepare system prompt with context
        system_prompt = self._prepare_system_prompt()
        
        try:
            # Call the LLM
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",  # Using GPT-4o mini as specified
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_query}
                ],
                temperature=0.1,  # Low temperature for more deterministic responses
                max_tokens=500,   # Limiting token count for the response
            )
            
            # Extract SQL from response
            sql_query = response.choices[0].message.content.strip()
            
            # If the response contains markdown SQL block, extract just the SQL
            if "```sql" in sql_query:
                sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
            elif "```" in sql_query:
                sql_query = sql_query.split("```")[1].split("```")[0].strip()
            
            # Ensure the query has a LIMIT clause
            if "LIMIT" not in sql_query.upper():
                # Удаляем точку с запятой в конце запроса, если она есть
                if sql_query.strip().endswith(';'):
                    sql_query = sql_query.strip()[:-1]
                
                # Добавляем LIMIT в конец запроса
                sql_query = f"{sql_query} LIMIT 1000"
            
            logger.info(f"Generated SQL query: {sql_query}")
            return sql_query
            
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            raise Exception(f"Failed to generate SQL: {str(e)}")

    def _prepare_system_prompt(self):
        """Prepare system prompt with schema and example information."""
        # Convert schema to string representation
        schema_str = json.dumps(self.schema, indent=2, ensure_ascii=False)
        dictionary_str = json.dumps(self.dictionary, indent=2, ensure_ascii=False)
        examples_str = json.dumps(self.query_examples, indent=2, ensure_ascii=False)
        
        # Create system prompt
        system_prompt = f"""
        Ты - SQL эксперт, помогающий пользователям создавать SQL запросы к базе данных торговой сети на основе их вопросов на естественном языке.
        
        ВАЖНО: Возвращай ТОЛЬКО SQL запрос, без каких-либо дополнительных комментариев или пояснений.
        
        Используй следующую схему базы данных:
        {schema_str}
        
        Вот словарь бизнес-терминов, который тебе следует учитывать:
        {dictionary_str}
        
        Примеры вопросов и соответствующих SQL запросов:
        {examples_str}
        
        Правила:
        1. Используй только SQL синтаксис, совместимый с DuckDB
        2. Ограничивай результаты до 1000 строк максимум (используй LIMIT)
        3. При запросах временных рядов, упорядочивай данные по времени
        4. Всегда учитывай оптимизацию запросов
        5. Если пользователь не указал конкретный период времени, используй последние данные
        6. Для расчета прибыли используй формулу: сумма(unit_price - unit_cost) * quantity
        7. Используй только таблицы и поля, определенные в схеме
        8. Возвращай ТОЛЬКО SQL запрос и ничего больше
        
        Преобразуй вопрос пользователя в SQL запрос.
        """
        
        return system_prompt 