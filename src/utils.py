import logging
from configparser import ConfigParser
from datetime import datetime
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
        self.default_section = None
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


Path(Config("logger")["output_path"]).mkdir(exist_ok=True)


class Logger(logging.Logger):
    config = Config("logger")
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s (%(levelname)s): %(message)s", "%H:%M:%S"
    )
    file_handler = logging.FileHandler(
        config["output_path"] / f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.log",
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
