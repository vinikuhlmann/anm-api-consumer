import os
import tracemalloc

from preprocessor import Preprocessor
from updater import DatabaseUpdater
from scraper import SigmineScraper
from utils import Logger
from pathlib import Path

logger = Logger(__name__)


def main():
    logger.info("Iniciando o programa")
    tracemalloc.start()

    scraper = SigmineScraper()
    to_clean = scraper()
    logger.debug(
        f"Memória máxima usada pelo scraper: {tracemalloc.get_traced_memory()[1] / 1e6:.2f} MB"
    )
    tracemalloc.reset_peak()

    if to_clean:
        preprocessor = Preprocessor()
        preprocessor(to_clean)
        logger.debug(
            f"Memória máxima usada pelo preprocessor: {tracemalloc.get_traced_memory()[1] / 1e6:.2f} MB"
        )
        tracemalloc.reset_peak()

        tracemalloc.reset_peak()
        updater = DatabaseUpdater()
        updater()
        logger.debug(
            f"Memória máxima usada pelo updater: {tracemalloc.get_traced_memory()[1] / 1e6:.2f} MB"
        )

    logger.info("Programa finalizado com sucesso")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Erro inesperado")
        raise
