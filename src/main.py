import logging

from .modules import cfem

logger = logging.getLogger(__name__)


def main():
    logger.info("==== INICIANDO ETL CFEM ====")
    cfem.run()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(e)
