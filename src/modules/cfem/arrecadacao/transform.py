import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def __read_csv(filepath) -> pd.DataFrame:
    logger.debug(f"Lendo dados de {filepath}...")

    DTYPE = {
        "Ano": str,
        "Mês": str,
        "Processo": str,
        "AnoDoProcesso": str,
        "Tipo_PF_PJ": str,
        "CPF_CNPJ": str,
        "Substância": str,
        "UF": str,
        "Município": str,
        "QuantidadeComercializada": float,
        "UnidadeDeMedida": str,
        "ValorRecolhido": float,
    }

    return pd.read_csv(
        filepath,
        sep=";",
        encoding="iso-8859-1",
        decimal=",",
        na_values="-",
        dtype=DTYPE,
    )


def __rename_columns(df):
    columns = {
        "Ano": "ano",
        "Mês": "mes",
        "Processo": "processo",
        "AnoDoProcesso": "ano_processo",
        "Tipo_PF_PJ": "tipo_pessoa",
        "CPF_CNPJ": "cpf_cnpj",
        "Substância": "substancia",
        "UF": "uf",
        "Município": "municipio",
        "QuantidadeComercializada": "quantidade_comercializada",
        "UnidadeDeMedida": "unidade_medida",
        "ValorRecolhido": "valor_recolhido",
    }
    return df.rename(columns=columns)


def transform(input_filepath, output_filepath):
    df = __read_csv(input_filepath)

    logger.debug("Transformando dados...")

    df = __rename_columns(df)
    df.select_dtypes(include="object").apply(
        lambda x: x.str.normalize("NFKD").str.strip()
    )
    df.processo = df.processo + "/" + df.ano_processo
    df.drop(columns="ano_processo", inplace=True)
    df.tipo_pessoa = np.where(df.tipo_pessoa == "PJ", "J", "F")
    df.unidade_medida = df.unidade_medida.str.strip()

    logger.debug(f"Salvando dados limpos em {output_filepath}...")
    df.to_csv(output_filepath, index=False)
