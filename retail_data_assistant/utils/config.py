import os

# Application settings
APP_NAME = "Retail Data Assistant"
APP_VERSION = "0.1.0"
APP_DESCRIPTION = "BI AI-Agent for retail data analysis using natural language"

# Database settings
DB_PATH = "retail_data.db"
DB_QUERY_TIMEOUT = 10  # seconds
DB_MAX_ROWS = 1000

# OpenAI API settings
# Note: The actual API key should be stored in .streamlit/secrets.toml
OPENAI_MODEL = "gpt-4o-mini"
OPENAI_TEMPERATURE = 0.1
OPENAI_MAX_TOKENS = 500

# Path settings
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
METADATA_DIR = os.path.join(ROOT_DIR, "metadata")

# Ensure directories exist
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

if not os.path.exists(METADATA_DIR):
    os.makedirs(METADATA_DIR)

# Logging settings
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s" 