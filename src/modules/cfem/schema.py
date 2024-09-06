"""
Cria as tabelas do banco de dados para o schema cfem.
"""

from sqlalchemy import (
    CHAR,
    DDL,
    FLOAT,
    INTEGER,
    VARCHAR,
    Column,
    Index,
    event,
    MetaData,
)
from sqlalchemy.ext.declarative import declarative_base

from src import Session

SCHEMA = "cfem"
metadata = MetaData(schema=SCHEMA)
Base = declarative_base(metadata=metadata)
event.listen(
    Base.metadata, "before_create", DDL(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
)

from sqlalchemy import CHAR, FLOAT, INTEGER, VARCHAR, Column, Index


class TaxRevenueTable(Base):
    __tablename__ = "arrecadacao"

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ano = Column(CHAR(4), nullable=False)
    mes = Column(CHAR(2), nullable=False)
    processo = Column(VARCHAR(18))  # processos antigos tem menos caracteres
    tipo_pessoa = Column(CHAR(1))
    cpf_cnpj = Column(CHAR(14))
    substancia = Column(VARCHAR(60), nullable=False)
    uf = Column(CHAR(2))
    municipio = Column(VARCHAR(60))
    quantidade_comercializada = Column(FLOAT)
    unidade_medida = Column(CHAR(2))
    valor_recolhido = Column(FLOAT, nullable=False)

    __table_args__ = (
        Index(
            "idx_arrecadacao_processo",
            "processo",
            unique=False,
            postgresql_using="hash",
            postgresql_where=(processo != None),  # muitos processos nulos
        ),
        Index(
            "idx_arrecadacao_cnpj",
            "cpf_cnpj",
            unique=False,
            postgresql_using="hash",
            postgresql_where=(tipo_pessoa == "J"),  # PFs tem CPF obfuscado
        ),
        Index(
            "idx_arrecadacao_uf_ano_mes",
            "uf",  # menor cardinalidade primeiro
            "ano",
            "mes",
            unique=False,
            postgresql_using="btree",
        ),
    )


def create_tables():
    with Session.begin() as session:
        Base.metadata.create_all(session.connection())
