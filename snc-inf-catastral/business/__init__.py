import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()
NOMBRE_ARCHIVO = os.path.join(f"{os.getenv('LOGS_FOLDER')}",
                              f"{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s : %(message)s',
                    handlers=[
                        logging.FileHandler(NOMBRE_ARCHIVO),
                        logging.StreamHandler(sys.stdout)
                    ])
