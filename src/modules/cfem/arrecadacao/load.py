import logging
import os

import pandas as pd
from sqlalchemy import sql

from src import Session

logger = logging.getLogger(__name__)


def load(filepath):

    logger.debug(f"Lendo dados de {filepath}...")
    df = pd.read_csv(filepath)

    with Session.begin() as session:
        logger.debug("Truncando tabela...")
        SQL = sql.text(f"TRUNCATE TABLE cfem.arrecadacao").execution_options(
            autocommit=True
        )
        session.execute(SQL)

        logger.debug("Carregando dados...")
        CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1024"))
        df.to_sql(
            "arrecadacao",
            schema="cfem",
            con=session.connection(),
            if_exists="append",
            index_label="id",
            chunksize=CHUNK_SIZE,
        )

    logger.info("Dados carregados com sucesso")
