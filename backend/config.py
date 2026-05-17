import os
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "quant_terminal.db")
CHROMA_PATH = os.path.join(DATA_DIR, "chroma_db")
LOG_PATH = os.path.join(DATA_DIR, "system.log")
CREDENTIALS_PATH = os.path.join(DATA_DIR, "credentials.json")

os.makedirs(DATA_DIR, exist_ok=True)

OLLAMA_BASE_URL = "http://localhost:11434"
DEFAULT_OLLAMA_MODEL = "llama3"

MAX_CANDLES_STORE = 5000
DEFAULT_RISK_PER_TRADE = 0.02
MAX_POSITION_RISK = 0.05
BACKTEST_DEFAULT_CAPITAL = 10000.0
