import logging
import os

import requests

logger = logging.getLogger(__name__)


def extract(filepath):
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1024"))
    URL = "https://app.anm.gov.br/dadosabertos/ARRECADACAO/CFEM_Arrecadacao.csv"
    request = requests.get(URL, stream=True, verify=False)
    file = open(filepath, "wb")

    logger.debug(f"Baixando dados de {URL} para {filepath}...")
    with request, file:
        for chunk in request.iter_content(chunk_size=CHUNK_SIZE):
            file.write(chunk)

    logger.debug(f"{filepath} baixado com sucesso")
