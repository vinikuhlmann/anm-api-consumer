import logging

from . import arrecadacao, schema

logger = logging.getLogger(__name__)


def run():
    logger.info("Criando tabelas no banco de dados...")
    schema.create_tables()

    logger.info("---- Dados de arrecadação ----")
    arrecadacao.run()

    logger.info("ETL finalizado com sucesso")
