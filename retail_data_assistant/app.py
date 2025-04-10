import streamlit as st
import pandas as pd
from data_manager.query_executor import QueryExecutor
from llm_processor import LLMProcessor
from data_manager.formatter import format_results
import os
import sys

# Add the root directory to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# App title and configuration
st.set_page_config(
    page_title="Retail Data Assistant",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title("🛒 Retail Data Assistant")
st.markdown("""
    Задайте вопрос о данных торговой сети на естественном языке, 
    и система автоматически переведет его в SQL-запрос и покажет результаты.
""")

# Initialize components
@st.cache_resource
def get_query_executor():
    return QueryExecutor()

@st.cache_resource
def get_llm_processor():
    return LLMProcessor()

# Get instances
query_executor = get_query_executor()
llm_processor = get_llm_processor()

# Make sure the session state for query input exists
if 'query_input' not in st.session_state:
    st.session_state.query_input = ""

# Add temporary storage for example queries
if 'example_query' not in st.session_state:
    st.session_state.example_query = ""
    
# If we have a pending example, put it in the query_input field
if st.session_state.example_query:
    st.session_state.query_input = st.session_state.example_query
    st.session_state.example_query = ""  # Clear after use

# User input
user_query = st.text_area("Ваш запрос:", 
    placeholder="Например: Покажи топ-10 товаров по продажам за последний месяц", 
    height=100,
    key="query_input")

# Process button
if st.button("📊 Выполнить запрос"):
    if user_query:
        with st.spinner("Обрабатываю ваш запрос..."):
            # Process with LLM
            try:
                st.subheader("Ваш запрос:")
                st.info(user_query)
                
                # Generate SQL
                sql_query = llm_processor.generate_sql(user_query)
                
                st.subheader("Сгенерированный SQL запрос:")
                st.code(sql_query, language="sql")
                
                # Execute the query
                results = query_executor.execute_query(sql_query)
                
                # Format and display results
                if results is not None and len(results) > 0:
                    formatted_results = format_results(results)
                    
                    st.subheader("Результаты:")
                    st.dataframe(formatted_results, use_container_width=True)
                    
                    # Download option
                    csv = formatted_results.to_csv(index=False)
                    st.download_button(
                        label="Скачать результаты как CSV",
                        data=csv,
                        file_name="results.csv",
                        mime="text/csv",
                    )
                else:
                    st.warning("Запрос выполнен успешно, но данные не найдены.")
            except Exception as e:
                st.error(f"Произошла ошибка: {str(e)}")
    else:
        st.warning("Пожалуйста, введите запрос.")

# Sidebar with examples
with st.sidebar:
    st.header("Примеры запросов")
    example_queries = [
        "Покажи топ-10 товаров по продажам за последний месяц",
        "Какие магазины имеют наибольшую выручку в категории 'Молочные продукты'?",
        "Сравни продажи по регионам за первый квартал этого года",
        "Как изменилась средняя маржа по категориям товаров за последние 3 месяца?",
        "Какие товары чаще всего покупают вместе с хлебом?",
        "Покажи динамику продаж мороженого по месяцам за прошлый год",
        "Какие клиенты потратили больше всего в прошлом месяце и что они покупали?",
        "Какие категории товаров приносят наибольшую прибыль в магазинах формата мини-маркет?",
        "Сравни эффективность промо-акций за последний квартал",
        "У каких товаров критический уровень запасов в магазинах Москвы?"
    ]
    
    for i, query in enumerate(example_queries):
        if st.button(f"Пример {i+1}", key=f"example_{i}"):
            # Store the example in a temporary variable
            st.session_state.example_query = query
            # Need to rerun to update the text area
            st.rerun()

    st.markdown("---")
    st.markdown("### О проекте")
    st.markdown("""
        Это MVP интеллектуального агента для анализа данных торговой сети 
        с использованием естественного языка.
        
        **Технологии:**
        - Streamlit
        - OpenAI GPT-4o mini
        - DuckDB
        - Python
    """) 