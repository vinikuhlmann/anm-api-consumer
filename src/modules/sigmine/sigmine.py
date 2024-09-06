# import csv
# import logging
# from pathlib import Path
# from zipfile import ZipFile

# import dask.dataframe as dd
# import requests
# from dbfread import DBF

# from etl import ETLPipeline
# from db import DatabaseHandler

# logger = logging.getLogger("main.sigmine")


# class Sigmine(ETLPipeline):

#     def extract(self):
#         logger.info("Extraindo os dados do SIGMINE")

#         def extract_file(zip_filepath: Path):

#             dbf_filepath = zip_filepath.with_suffix(".dbf")
#             csv_filepath = zip_filepath.with_suffix(".csv")

#             logger.debug(f"Baixando {zip_filepath.name}")
#             URL = f"https://app.anm.gov.br/dadosabertos/SIGMINE/PROCESSOS_MINERARIOS/{zip_filepath.name}"
#             request, file = requests.get(URL, stream=True), open(zip_filepath, "wb")
#             with request, file:
#                 for chunk in request.iter_content(chunk_size=8192):
#                     file.write(chunk)

#             logger.debug(f"Extraindo {zip_filepath.name}")
#             with ZipFile(zip_filepath, "r") as zip_ref:
#                 zip_ref.extract(dbf_filepath.name, self.dir)

#             logger.debug(f"Convertendo {dbf_filepath.name} para {csv_filepath.name}")
#             table = DBF(dbf_filepath, encoding="iso-8859-1")
#             with open(csv_filepath, "w", newline="") as csv_file:
#                 writer = csv.writer(csv_file)
#                 writer.writerow(table.field_names)
#                 for record in table:
#                     writer.writerow(list(record.values()))

#         for filename in ("BRASIL.zip", "BRASIL_INATIVOS.zip"):
#             zip_filepath = self.dir / filename
#             extract_file(zip_filepath)

#     def transform(self):
#         logger.info("Limpando os dados do SIGMINE")

#         ativos_csv, inativos_csv = (
#             self.dir / "BRASIL.csv",
#             self.dir / "BRASIL_INATIVOS.csv",
#         )

#         def read(filepath):
#             return dd.read_csv(
#                 filepath,
#                 usecols=[
#                     "DSPROCESSO",
#                     "NOME",
#                     "SUBS",
#                     "USO",
#                     "AREA_HA",
#                     "ULT_EVENTO",
#                 ],
#                 na_values=["DADO NÃO CADASTRADO", "Não informado"],
#             )

#         # Concatenar ativos e inativos
#         ativos_df = read(ativos_csv)
#         inativos_df = read(inativos_csv)
#         ativos_df["ativo"] = True
#         inativos_df["ativo"] = False
#         df = dd.concat([ativos_df, inativos_df], axis=0, ignore_unknown_divisions=True)

#         # Renomear colunas
#         df.columns = df.columns.str.lower()
#         df = df.rename(columns={"dsprocesso": "processo"})

#         # Processos
#         processos_cols = [
#             "processo",
#             "ativo",
#             "nome",
#             "subs",
#             "uso",
#             "area_ha",
#             "ult_evento",
#         ]
#         processos_df = df[processos_cols].drop_duplicates("processo")

#         # Extrair código, descrição, dia e mês do último evento
#         cod_desc_dia_mes = (
#             processos_df["ult_evento"]
#             .str.extract("(\d+) - (.*) EM (\d{2})\/(\d{2})\/\d{4}", expand=True)
#             .rename(columns={0: "cod_ult", 1: "desc_ult", 2: "dia", 3: "mes"})
#         )
#         processos_df = processos_df.drop(columns="ult_evento")
#         processos_df = dd.concat(
#             [processos_df, cod_desc_dia_mes],
#             axis=1,
#             ignore_unknown_divisions=True,
#         )

#         logger.debug("Limpando dados de processos")
#         processos_csv_filepath = self.dir / "processos.csv"
#         processos_df.to_csv(processos_csv_filepath, index=False, single_file=True)

#         # # Fases
#         # fases_pkey = ["processo", "fase", "uf"]
#         # fases_df = df[fases_pkey].drop_duplicates(["processo", "fase", "uf"])

#         # logger.debug("Limpando dados de fases")
#         # fases_csv_filepath = self.dir / "fases.csv"
#         # fases_df.to_csv(fases_csv_filepath, index=False, single_file=True)

#     def load(self, database_handler: DatabaseHandler):
#         logger.info("Carregando os dados limpos do SIGMINE no banco de dados")

#         logger.debug("Carregando dados de processos")
#         database_handler.upsert_csv(
#             "sigmine",
#             "processos",
#             self.dir / "processos.csv",
#         )

#         # logger.debug("Carregando dados de fases")
#         # database.upsert_csv(
#         #     "sigmine",
#         #     "fases",
#         #     self.clean_data_dirpath / "fases.csv",
#         #     constraint="fases_unique",
#         # )
