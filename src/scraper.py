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
import tracemalloc
import re
import zipfile
from datetime import datetime
from os.path import getctime
from pathlib import Path
import tempfile
import shutil
import pandas as pd
import urllib3
from bs4 import BeautifulSoup
from dbfread import DBF

from utils import Config, Logger

logger = Logger(__name__)

from enum import StrEnum


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


class SigmineScraper:
    """Scraper for the SIGMINE database."""

    def __init__(self, config: Config = Config("scraper")):
        self.output_path = config["output_path"]
        self.max_threads = int(config["max_threads"])

    def __parse_timestamps(
        self, raw_timestamps: dict[str, str]
    ) -> dict[BrazilianStates, datetime]:
        """Parses the strings to enum and datetime objects."""
        DATE_FMT = "%m/%d/%Y %I:%M %p"
        timestamps = {
            BrazilianStates[state]: datetime.strptime(date, DATE_FMT)
            for state, date in raw_timestamps.items()
        }
        return timestamps

    def _fetch_timestamps_from_sigmine(
        self, pool: urllib3.PoolManager
    ) -> dict[BrazilianStates, datetime]:
        """Fetches the timestamps from the SIGMINE database."""

        # Fetches the page content
        URL = "https://app.anm.gov.br/dadosabertos/SIGMINE/PROCESSOS_MINERARIOS/"
        logger.info(f"Lendo timestamps em {URL}")
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
        raw_timestamps = {state: date for date, state in matches}
        timestamps = self.__parse_timestamps(raw_timestamps)

        logger.debug(f"Timestamps: {json.dumps(timestamps, default=str)}")

        return timestamps

    def _get_states_to_update(
        self, timestamps: dict[BrazilianStates, datetime]
    ) -> list[BrazilianStates]:
        """Gets the states that need to be updated."""
        path = lambda state: self.output_path / f"{state.value}.parquet"
        get_filedt = lambda fp: datetime.fromtimestamp(getctime(fp))
        return [
            state
            for state, date in timestamps.items()
            if not path(state).exists() or get_filedt(path(state)) < date
        ]

    def _extract_state(
        self,
        state: BrazilianStates,
        pool: urllib3.PoolManager,
    ):
        """Extracts data from the SIGMINE database for a given state."""
        logger.info(f"Iniciando extração de dados de {state.value}")

        ZIP_F = f"{state.value}.zip"
        URL = (
            f"https://app.anm.gov.br/dadosabertos/SIGMINE/PROCESSOS_MINERARIOS/{ZIP_F}"
        )

        # fmt: off
        with tempfile.TemporaryDirectory(dir=self.output_path) as tmpdir:
        # fmt: on
            
            # Download the zip file
            zippath = Path(tmpdir) / ZIP_F
            with pool.request("GET", URL, preload_content=False) as r, open(zippath, "wb") as f:
                shutil.copyfileobj(r, f)

            # Extract the dbf file
            DBF_F = f"{state.value}.dbf"
            dbfpath = Path(tmpdir) / DBF_F
            with zipfile.ZipFile(zippath, "r") as zip_ref:
                zip_ref.extractall(path=tmpdir, members=[DBF_F])
            logger.debug(f"{DBF_F} extraído para {dbfpath}")

            # Read the dbf file
            ENCODING = "iso-8859-1"
            logger.debug(f"Lendo {dbfpath} com encoding {ENCODING}")
            table = DBF(dbfpath, encoding=ENCODING)
            df = pd.DataFrame(iter(table))

        df["UF"] = state.value
        path = self.output_path / f"{state.value}.parquet"
        df.to_parquet(path, index=False)
        del df

        logger.info(
            f"Dados de {state.value} salvos em {path}"
        )

    def __call__(self) -> list[Path]:
        """
        Extracts data from outdated states at the SIGMINE database and saves it to a csv files.

        Returns a list of the paths to the csv files.

        States that need to be updated are determined by comparing the file creation date with
        the timestamps from the SIGMINE database.

        Multithreading is used; number of threads is determined by the max_threads config.
        """
        self.output_path.mkdir(exist_ok=True)
        paths = []
        tracemalloc.start()

        # fmt: off
        with urllib3.PoolManager() as pool, \
        concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            # fmt: on
            timestamps = self._fetch_timestamps_from_sigmine(pool)
            states_to_update = self._get_states_to_update(timestamps)
            logger.debug(f"{len(states_to_update)} estados precisam ser atualizados")

            # Extract the data using multithreading
            futures = {
                executor.submit(self._extract_state, state, pool): state
                for state in states_to_update
            }
            for future in concurrent.futures.as_completed(futures):
                logger.info(f"Memoria usada: {tracemalloc.get_traced_memory()[0] / 1e6:.2f} MB")
                state = futures[future]
                del futures[future]
                path = self.output_path / f"{state.value}.parquet"
                paths.append(path)
        
        logger.info("Dados crus extraídos")
        return paths
