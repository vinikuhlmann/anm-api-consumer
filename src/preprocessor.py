from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from utils import Config, Logger


logger = Logger(__name__)


class Preprocessor:
    """Preprocesses the data from the SIGMINE database."""

    def __init__(self, config: Config = Config("preprocessor")):
        self.output_path = config["output_path"]

    def _clean_data(self, df: dd.DataFrame) -> dd.DataFrame:
        """Makes the data ready to be inserted into the database"""
        df.columns = df.columns.str.lower()
        cols = [
            "numero",
            "ano",
            "uf",
            "area_ha",
            "fase",
            "ult_evento",
            "nome",
            "subs",
            "uso",
        ]
        df = df.drop_duplicates().loc[:, cols]
        df = df.replace("DADO NÃO CADASTRADO", pd.NA)
        df["numero"] = df["numero"].astype(str)
        df["ano"] = df["ano"].astype(str)

        # Splitting the ult_evento column
        new_cols = (
            df["ult_evento"]
            .str.extract("(\d+) - (.*) EM (\d{2})\/(\d{2})\/\d{4}", expand=True)
            .rename(columns={0: "cod_ult", 1: "desc_ult", 2: "dia", 3: "mes"})
        )
        df = df.drop(columns="ult_evento")
        df = (
            dd.concat([df, new_cols], axis=1, ignore_unknown_divisions=True)
            .dropna(subset=["numero", "ano", "nome", "fase", "uf"])
            .drop_duplicates(subset=["numero", "ano", "fase", "uf"])
        )

        return df

    def __call__(self, to_clean: list[Path]):
        logger.info("Limpando os dados")
        for file in to_clean:
            df = dd.read_parquet(file)
            df = self._clean_data(df)
            df.to_csv(
                self.output_path / file.with_suffix(".csv").name,
                index=False,
                single_file=True,
            )
            logger.info(f"{file.name} limpo")
