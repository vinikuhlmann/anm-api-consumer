import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

# DATABASE
# ==============================================================================

USER = os.environ["POSTGRES_USER"]
PASSWORD = os.environ["POSTGRES_PASSWORD"]
HOST = os.environ["POSTGRES_HOST"]
PORT = os.environ["POSTGRES_PORT"]
DATABASE = os.environ["POSTGRES_DB"]
URI = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
__engine = create_engine(URI)
Session = sessionmaker(__engine)

# LOGGING
# ==============================================================================

# current working directory
ROOT = Path(__file__).parent.parent
LOG_FILEPATH = ROOT / "logs" / f"{datetime.now().strftime('%Y-%m-%d')}.log"
LOG_FILEPATH.parent.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILEPATH, mode="a"),
        logging.StreamHandler(),
    ],
)
urllib3_logger = logging.getLogger("urllib3")
urllib3_logger.setLevel(logging.ERROR)
