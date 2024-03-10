import os
import tracemalloc

from database import update_database
from scraper import fetch_data
from utils import Logger

# Set the working directory to the root of the project
here = os.path.abspath(__file__)
scripts = os.path.dirname(here)
src = os.path.dirname(scripts)
root = os.path.dirname(src)
os.chdir(root)

logger = Logger(__name__)


def main():
    logger.info("Iniciando o programa")
    tracemalloc.start()
    if fetch_data():
        update_database()
        pass
    logger.info("Programa finalizado com sucesso")
    logger.info(
        f"Máximo de memória alocada: {tracemalloc.get_traced_memory()[1] / 1e6:.2f} MB"
    )

    pass


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Erro inesperado")
        raise
