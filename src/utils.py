import logging
from configparser import ConfigParser
from datetime import datetime
from os.path import getctime
from pathlib import Path


def get_root() -> Path:
    """Returns the root of the project"""
    here = Path(__file__).resolve()
    src = here.parent
    root = src.parent
    return root


root = get_root()


class ConfigError(Exception):
    pass


class Config(ConfigParser):
    path = root / "config.ini"

    def __init__(self, section: str):
        super().__init__()
        self.section = section
        try:
            self.read(self.path)
        except (FileNotFoundError, KeyError):
            raise ConfigError("config.ini não encontrado no diretório raiz")

    def __getitem__(self, key: str):
        value = self.get(self.section, key)
        if key.endswith("path"):
            value = root / value
        return value


def create_log_dir():
    log_dir = Config("logger")["log_path"]
    Path(log_dir).mkdir(exist_ok=True)


create_log_dir()


class Logger(logging.Logger):
    config = Config("logger")
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s (%(levelname)s): %(message)s", "%H:%M:%S"
    )
    file_handler = logging.FileHandler(
        config["log_path"] / f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.log",
        mode="w",
    )
    stream_handler = logging.StreamHandler()
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    def __init__(self, name: str):
        super().__init__(name, level=Logger.config["log_level"])
        self.addHandler(Logger.file_handler)
        self.addHandler(Logger.stream_handler)


logger = Logger(__name__)


class TmpDir:
    """Context manager for a temporary directory."""

    def __enter__(self) -> Path:
        self.tmpdir = root / "tmp"
        self.tmpdir.mkdir(exist_ok=True)
        logger.debug(f"Usando diretório temporário {self.tmpdir}")
        return self.tmpdir

    def __exit__(self, exc_type, exc_value, traceback):
        for f in self.tmpdir.iterdir():
            f.unlink()
        self.tmpdir.rmdir()
        logger.debug(f"Diretório temporário {self.tmpdir} e seu conteúdo apagados")


def is_created_today(filepath: Path) -> bool:
    """Checks if a file was created today"""
    try:
        timestamp = getctime(filepath)
    except FileNotFoundError:
        return False
    dt = datetime.fromtimestamp(timestamp)
    today = datetime.now().date()
    return dt.date() == today
