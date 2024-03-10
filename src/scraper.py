"""
scraper.py

This script is responsible for scraping data from the SIGMINE database and saving it to a csv file.

Usage: ExtratorSigmine().extract()

Executing it as __main__ will open the GUI for the extraction process.

List of configs in config.ini under the [scraper] section:
- output-filepath: The directory where the csv and timestamps files will be saved
- timestamps-filename: The name of the timestamps file
- log-level (optional): The log level for the logger (overrides the default log level)

Author: Vinicius S. F. Kuhlmann
Date: March 5, 2024
"""

import concurrent.futures
import json
import re
import threading
import zipfile
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import TypedDict

import pandas as pd
import urllib3
from bs4 import BeautifulSoup
from dbfread import DBF

from utils import Config, Logger, TmpDir, is_created_today

config = Config("scraper")
logger = Logger(__name__)


class BrazilianStates(StrEnum):
    """Enum with the Brazilian states."""

    AC = "AC"
    AL = "AL"
    AP = "AP"
    AM = "AM"
    BA = "BA"
    CE = "CE"
    DF = "DF"
    ES = "ES"
    GO = "GO"
    MA = "MA"
    MT = "MT"
    MS = "MS"
    MG = "MG"
    PA = "PA"
    PB = "PB"
    PR = "PR"
    PE = "PE"
    PI = "PI"
    RJ = "RJ"
    RN = "RN"
    RS = "RS"
    RO = "RO"
    RR = "RR"
    SC = "SC"
    SP = "SP"
    SE = "SE"
    TO = "TO"


class Timestamps(TypedDict):
    BrazilianStates: datetime


@dataclass
class TimestampFetcher:

    timestamps_path: Path

    def _parse_timestamps(self, raw_timestamps: dict[str, str]) -> Timestamps:
        """Parses the strings to enum and datetime objects."""
        DATE_FMT = "%m/%d/%Y %I:%M %p"
        timestamps = {
            BrazilianStates[state]: datetime.strptime(date, DATE_FMT)
            for state, date in raw_timestamps.items()
        }
        return timestamps

    def _fetch_timestamps_from_sigmine(self) -> Timestamps:
        """Fetches the timestamps from the SIGMINE database."""

        # Fetches the page content
        URL = "https://app.anm.gov.br/dadosabertos/SIGMINE/PROCESSOS_MINERARIOS/"
        logger.info(f"Lendo timestamps em {URL}")
        with urllib3.PoolManager() as pool:
            page = pool.request("GET", URL).data
        soup = BeautifulSoup(page, "html.parser")
        raw_text = soup.get_text()

        # Removes prefix and excess whitespace
        text = (
            raw_text.split("[To Parent Directory]")[1]
            .replace("  ", " ")
            .replace("  ", "")
        )

        # Extracts the timestamps
        pattern = (
            r"(\d{1,2}\/\d{1,2}\/\d{4} \d{1,2}:\d{2} (?:AM|PM))\s?[0-9]+ ([A-Z]{2}).zip"
        )
        matches = re.findall(pattern, text)
        logger.debug(f"{len(matches)} timestamps lidas de {URL} Ex: {matches[0]}")
        raw_timestamps = {state: date for date, state in matches}

        # Save the timestamps to a local json file
        self.timestamps_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.timestamps_path, "w") as f:
            json.dump(raw_timestamps, f, default=str)

        timestamps = self._parse_timestamps(raw_timestamps)
        logger.info(f"Timestamps salvas em {self.timestamps_path}")

        return timestamps

    def _get_current_timestamps(self) -> Timestamps:
        """Gets the last update datetime for each state from the local json file."""
        try:
            with open(self.timestamps_path, "r") as f:
                raw_timestamps = json.load(f)
        except FileNotFoundError:
            logger.warning("Arquivo de timestamps não encontrado")
            return {}

        # Convert the strings to enum and datetime objects
        timestamps = self._parse_timestamps(raw_timestamps)
        logger.info("Timestamps locais carregadas")

        return timestamps

    def get_states_to_update(self) -> set[BrazilianStates]:
        """Gets the states that need to be updated."""
        current_timestamps = self._get_current_timestamps()
        latest_timestamps = self._fetch_timestamps_from_sigmine()

        if current_timestamps == {}:
            return set(BrazilianStates)

        states_to_update = {
            state
            for state in BrazilianStates
            if current_timestamps[state] < latest_timestamps[state]
        }

        return states_to_update


@dataclass
class SigmineScraper:
    """Scraper for the SIGMINE database."""

    timestamp_fetcher: TimestampFetcher
    cache_path: Path

    def _extract_state(
        self, state: BrazilianStates, dir: Path, pool: urllib3.PoolManager
    ) -> pd.DataFrame:
        """Extracts data from the SIGMINE database for a given state."""
        logger.info(f"Iniciando extração de dados de {state.value}")

        ZIP_F = f"{state.value}.zip"
        URL = (
            f"https://app.anm.gov.br/dadosabertos/SIGMINE/PROCESSOS_MINERARIOS/{ZIP_F}"
        )

        # Download the zip file
        logger.debug(f"Baixando {ZIP_F} de {URL}")
        with open(dir / ZIP_F, "wb") as f:
            for chunk in pool.request("GET", URL, preload_content=False):
                f.write(chunk)
        logger.debug(f"{ZIP_F} baixado")

        # Extract the dbf file
        DBF_F = f"{state.value}.dbf"
        logger.debug(f"Extraindo {ZIP_F} para {DBF_F}")
        with zipfile.ZipFile(dir / ZIP_F, "r") as zip_ref:
            zip_ref.extractall(path=dir, members=[DBF_F])

        # Read the dbf file
        ENCODING = "iso-8859-1"
        logger.debug(f"Lendo {DBF_F} com encoding {ENCODING}")
        table = DBF(dir / DBF_F, encoding=ENCODING)
        df = pd.DataFrame(iter(table))
        df["UF"] = state.value

        logger.info(f"Dados de {state.value} extraídos")
        return df

    def __call__(self) -> pd.DataFrame:
        """
        Extracts data from outdated states at the SIGMINE database and saves it to a csv file.

        States that need to be updated are determined by comparing the latest timestamps from the SIGMINE database
        with the local timestamps file.
        """
        # Get the states that need to be updated, and also updates the local timestamps file
        states_to_update = self.timestamp_fetcher.get_states_to_update()

        # If the file doesn't exist, all states need to be updated regardless
        if not self.cache_path.exists():
            states_to_update = set(BrazilianStates)

        if len(states_to_update) == 0:
            return pd.DataFrame()

        logger.info(f"{len(states_to_update)} estados precisam ser atualizados")

        # fmt: off
        # Use multithreading to extract the data from the states in parallel
        with TmpDir() as tmpdir, \
        urllib3.PoolManager() as pool, \
        concurrent.futures.ThreadPoolExecutor(max_workers=int(config["max_threads"])) as executor:
            # fmt: on
            futures = [
                executor.submit(self._extract_state, state, tmpdir, pool)
                for state in states_to_update
            ]
            dfs = [future.result() for future in concurrent.futures.as_completed(futures)]
        df = pd.concat(dfs, copy=False)
        logger.info("Dados crus extraídos e concatenados")

        return df


@dataclass
class DataFetcher:

    scraper: SigmineScraper

    def _get_data_from_file(self) -> pd.DataFrame:
        """Reads the data from a file if it exists and is updated."""
        f = self.scraper.cache_path
        if not is_created_today(f):
            return pd.DataFrame()

        df = pd.read_csv(f)
        return df

    def __call__(self) -> pd.DataFrame:
        """Reads the data from a file if it was created today, otherwise runs the scraper."""
        df = self._get_data_from_file()
        if not df.empty:
            logger.info("Cache de dados crus está atualizado")
            return df

        logger.info(
            "Cache de dados não existe ou não está atualizado, rodando o scraper"
        )
        df = self.scraper()
        if df.empty:
            logger.info(
                "Todos os estados já estão atualizados, nenhum dado foi extraído"
            )
            return df

        f = self.scraper.cache_path
        df.to_csv(f, index=False)
        logger.info(f"Cache de dados crus criado em {f}")

        return df


@dataclass
class DataPreprocessor:
    """Preprocesses the data from the SIGMINE database."""

    processos_path: Path
    fases_path: Path

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
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
        df.replace("DADO NÃO CADASTRADO", pd.NA, inplace=True)
        df["numero"] = df["numero"].astype(str)
        df["ano"] = df["ano"].astype(str)

        # Splitting the ult_evento column
        new_cols = (
            df["ult_evento"]
            .str.extract("(\d+) - (.*) EM (\d{2})\/(\d{2})\/\d{4}", expand=True)
            .rename({0: "cod_ult", 1: "desc_ult", 2: "dia", 3: "mes"}, axis=1)
        )
        df = pd.concat([df, new_cols], axis=1)

        return df

    def _get_processos(self, df: pd.DataFrame) -> pd.DataFrame:
        processos = (
            df.groupby(["numero", "ano"])
            .agg(
                {
                    "nome": "first",
                    "area_ha": "sum",
                    "subs": "first",
                    "uso": "first",
                    "cod_ult": "first",
                    "desc_ult": "first",
                }
            )
            .dropna(subset=["nome"])
            .reset_index()
        )
        return processos

    def _get_fases(self, df: pd.DataFrame) -> pd.DataFrame:
        fases = (
            df[["numero", "ano", "mes", "dia", "fase", "uf"]]
            .dropna(subset=["numero", "ano", "fase", "uf"])
            .drop_duplicates(subset=["numero", "ano", "fase", "uf"])
        )
        return fases

    def __call__(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        logger.info("Limpando os dados")
        df = self._clean_data(df)
        processos = self._get_processos(df)
        fases = self._get_fases(df)
        del df

        logger.info(
            f"Salvando os dados limpos em {self.processos_path} e {self.fases_path}"
        )
        processos.to_csv(self.processos_path, index=False)
        fases.to_csv(self.fases_path, index=False)
        del processos
        del fases


def fetch_data() -> bool:
    """Fetches the data from the SIGMINE database and saves it to csv files.

    Returns False if no data was collected (probably because there's no new data available).
    Otherwise, returns True.
    """
    timestamp_fetcher = TimestampFetcher(config["timestamps_path"])
    scraper = SigmineScraper(timestamp_fetcher, config["cache_path"])
    fetcher = DataFetcher(scraper)
    preprocessor = DataPreprocessor(config["processos_path"], config["fases_path"])
    df = fetcher()
    if df.empty:
        return False
    preprocessor(df)
    return True


if __name__ == "__main__":
    try:
        fetch_data()
    except Exception:
        logger.exception(f"Um erro ocorreu:\n")
