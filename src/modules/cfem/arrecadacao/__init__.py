import logging
from tempfile import NamedTemporaryFile

from .extract import extract
from .load import load
from .transform import transform

logger = logging.getLogger(__name__)


def run():
    with NamedTemporaryFile() as tmp:
        logger.info("Extraindo dados")
        extract(tmp.name)

        logger.info("Transformando dados")
        transform(tmp.name, tmp.name)

        logger.info("Carregando dados")
        load(tmp.name)
