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
    numero = Column(CHAR(6))
    ano = Column(CHAR(4))
    nome = Column(VARCHAR(150), nullable=False)
    area_ha = Column(FLOAT, default=0)
    subs = Column(VARCHAR(50))
    uso = Column(VARCHAR(50))
    cod_ult = Column(CHAR(4))
    desc_ult = Column(VARCHAR(100))
    PrimaryKeyConstraint(numero, ano)


class FasesTable(Base):
    __tablename__ = "fases"
    numero = Column(CHAR(6))
    ano = Column(CHAR(4))
    mes = Column(CHAR(2))
    dia = Column(CHAR(2))
    fase = Column(VARCHAR(50))
    estado = Column(CHAR(2))
    PrimaryKeyConstraint(numero, ano, fase, estado)
    ForeignKeyConstraint(["numero", "ano"], ["processos.numero", "processos.ano"])


def get_engine() -> Engine:
    """Creates a sqlalchemy engine"""
    config = Config("database")
    user, password, host, port, database = (
        config["user"],
        config["password"],
        config["host"],
        config["port"],
        config["database"],
    )
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    logger.info(f"Conectando como {user} em {database} ({host}:{port})")
    return engine


def psql_insert_copy(table, conn: Engine, keys, data_iter):
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


def update_database():
    engine = get_engine()
    config = Config("scraper")
    with engine.connect() as conn:
        meta = MetaData()

        logger.info("Removendo tabelas antigas")
        meta.reflect(bind=conn)
        meta.drop_all(bind=conn)

        logger.info("Criando tabelas")
        Base.metadata.create_all(engine)

        logger.info("Atualizando a tabela processos")
        processos = pd.read_csv(config["processos_path"], dtype=str)
        processos["area_ha"] = processos["area_ha"].astype(float)
        processos.to_sql(
            "processos",
            conn,
            if_exists="append",
            method=psql_insert_copy,
            index=False,
        )
        del processos

        logger.info("Atualizando a tabela fases")
        fases = pd.read_csv(config["fases_path"])
        fases.to_sql(
            "fases",
            conn,
            if_exists="append",
            method=psql_insert_copy,
            index=False,
        )
        del fases

        role = "readonly"
        logger.info(f"Concedendo permissões de leitura para a role {role}")
        conn.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}"))

        conn.commit()

    logger.info("Banco de dados atualizado com sucesso")
