"""
update_db.py

This script is responsible for updating the database with the latest data from SIGMINE.
"""

import csv
from io import StringIO

import pandas as pd
from sqlalchemy import (
    CHAR,
    FLOAT,
    VARCHAR,
    Column,
    ForeignKeyConstraint,
    MetaData,
    PrimaryKeyConstraint,
    create_engine,
    text,
)
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import declarative_base

from utils import Config, Logger

logger = Logger(__name__)

Base = declarative_base()


class ProcessosTable(Base):
    __tablename__ = "processos"
    numero = Column(CHAR(6), nullable=False)
    ano = Column(CHAR(4), nullable=False)
    nome = Column(VARCHAR(150), nullable=False)
    area_ha = Column(FLOAT, default=0)
    subs = Column(VARCHAR(50))
    uso = Column(VARCHAR(50))
    cod_ult = Column(CHAR(4))
    desc_ult = Column(VARCHAR(100))
    PrimaryKeyConstraint(numero, ano)


class FasesTable(Base):
    __tablename__ = "fases"
    numero = Column(CHAR(6), nullable=False)
    ano = Column(CHAR(4), nullable=False)
    mes = Column(CHAR(2), nullable=False)
    dia = Column(CHAR(2))
    fase = Column(VARCHAR(50), nullable=False)
    estado = Column(CHAR(2))
    PrimaryKeyConstraint(numero, ano, fase, estado)
    ForeignKeyConstraint(["numero", "ano"], ["processos.numero", "processos.ano"])


class DatabaseUpdater:

    engine: Engine = None

    @classmethod
    def get_engine(cls, user, password, host, port, database):
        """Creates a sqlalchemy engine"""
        if cls.engine is None:
            engine = create_engine(
                f"postgresql://{user}:{password}@{host}:{port}/{database}"
            )
            logger.info(f"Conectando como {user} em {database} ({host}:{port})")
            cls.engine = engine
        return cls.engine

    def __init__(self, config: Config = Config("database")):
        self.engine = DatabaseUpdater.get_engine(
            config["user"],
            config["password"],
            config["host"],
            config["port"],
            config["database"],
        )
        self.chunksize = int(config["chunksize"])

    def __psql_insert_copy(self, table, conn: Engine, keys, data_iter):
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ", ".join(f'"{k}"' for k in keys)
            if table.schema:
                table_name = f"{table.schema}.{table.name}"
            else:
                table_name = table.name

            sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)

    def _get_processos(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby(["numero", "ano"]).agg(
            {
                "numero": "first",
                "ano": "first",
                "nome": "first",
                "area_ha": "sum",
                "subs": "first",
                "uso": "first",
                "cod_ult": "first",
                "desc_ult": "first",
            }
        )

    def _get_fases(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[["numero", "ano", "mes", "dia", "fase", "uf"]]

    def _update_table(self, conn: Engine, df: pd.DataFrame, table: str):
        df.to_sql(
            table,
            conn,
            if_exists="append",
            index=False,
            chunksize=self.chunksize,
            method=self.__psql_insert_copy,
        )

    def __call__(self):
        config = Config("preprocessor")
        df = pd.concat(
            (
                pd.read_csv(
                    file,
                    dtype={
                        "numero": str,
                        "ano": str,
                        "mes": str,
                        "dia": str,
                        "uf": "category",
                        "fase": "category",
                        "area_ha": float,
                        "nome": str,
                        "subs": "category",
                        "uso": "category",
                        "cod_ult": str,
                        "desc_ult": str,
                    },
                )
                for file in config["output_path"].glob("*.csv")
            )
        )

        with self.engine.connect() as conn:
            logger.info("Removendo tabelas antigas")
            meta = MetaData()
            meta.reflect(bind=conn)
            meta.drop_all(bind=conn)

            logger.info("Criando tabelas")
            Base.metadata.create_all(self.engine)

            logger.info("Atualizando a tabela processos")
            processos = self._get_processos(df)
            self._update_table(conn, processos, "processos")
            del processos

            logger.info("Atualizando a tabela fases")
            fases = self._get_fases(df)
            self._update_table(conn, fases, "fases")
            del fases

            role = "readonly"
            logger.info(f"Concedendo permissões de leitura para a role {role}")
            conn.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}"))

            conn.commit()

    logger.info("Banco de dados atualizado com sucesso")
