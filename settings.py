import os
from os.path import dirname, abspath

from dotenv import load_dotenv

from custom_types.controller_type import EMode

load_dotenv('.env')

BASE_DIR = dirname(dirname(abspath(__file__)))

API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

SYMBOLS = ['ucrusdt', 'cartusdt', 'gensusdt']

TP_TARGET = 1.0  # Increase from average price (1.0 == 100%)
COPY_BUY_PERCENT = 0.5  # Amount to copy buy from organizers (1.0 == 100%)
SUPPORTED_CURRENCIES = ['usdt', 'btc']
RECORD_SECONDS_TO_KEEP = 3
# Increase in price % within an amount of time before we stop copy buy (1.0 == 100%)
RECORD_TARGET_PERCENT_TO_STOP_COPY_BUY = 0.5

DECIMALS = 1
TELEGRAM_MODE=EMode.PRODUCTION
