# import logging
# from pathlib import Path
# from zipfile import ZipFile

# import dask.dataframe as dd
# import requests

# from etl import ETLPipeline
# from db import DatabaseHandler

# logger = logging.getLogger("main.scm")


# class SCMHelper(ETLPipeline):

#     def merge(
#         self,
#         df: dd.DataFrame,
#         _with: list[Path],
#         drop_id=True,
#     ):
#         for filename in _with:
#             merge_df = dd.read_csv(
#                 self.raw_data_dirpath / filename,
#                 sep=";",
#                 encoding="iso-8859-1",
#                 decimal=",",
#                 dtype=str,
#             )
#             id_col = f"ID{filename.split('.')[0]}"
#             df = df.merge(merge_df, how="left", on=id_col)
#             if drop_id:
#                 df = df.drop(columns=id_col)

#         return df

#     def _write_csv(self, df, filename: Path):
#         df.to_csv(self.clean_data_dirpath / filename, index=False, single_file=True)


class Processos(SCMHelper):

    def extract(self) -> dd.DataFrame:
        df = dd.read_csv(
            self.raw_data_dirpath / "Processo.txt",
            sep=";",
            encoding="iso-8859-1",
            decimal=",",
            dtype=str,
        )
        merge = [
            "TipoRequerimento.txt",
            "FaseProcesso.txt",
            "UnidadeAdministrativaRegional.txt",
            "UnidadeProtocolizadora.txt",
        ]
        df = self.merge(df, merge)
        return df

    def transform(self):
        logger.debug("Transformando processos")

        df = self.extract()

        columns = {
            "DSProcesso": "processo",
            "BTAtivo": "ativo",
            "DSFaseProcesso": "fase",
            "DSTipoRequerimento": "tipo_requerimento",
            "QTAreaHA": "area_ha",
            "NRNUP": "nup",
            "DTProtocolo": "data_protocolo",
            "DTPrioridade": "data_prioridade",
            "DSUnidadeAdministrativaRegional": "unidade_administrativa_regional",
            "DSUnidadeProtocolizadora": "unidade_protocolizadora",
        }
        df = df.rename(columns=columns)[list(columns.values())]
        df.ativo = (
            df.ativo.replace({"S": "True", "N": "False"}).fillna("False").astype(bool)
        )
        df.fase = df.fase.str.upper()
        df.area_ha = df.area_ha.str.replace(",", ".")
        df.data_protocolo = df.data_protocolo.str.replace(" 00:00:00", "")
        df.data_prioridade = df.data_prioridade.str.replace(" 00:00:00", "")

        self._write_csv(df, "processos.csv")

    def load(self, database: DatabaseHandler):
        logger.debug("Carregando processos")
        database.upsert_csv(
            "scm", "processos", self.clean_data_dirpath / "processos.csv"
        )


# class Associacoes(SCMHelper):

#     def extract(self) -> dd.DataFrame:
#         df = dd.read_csv(
#             self.raw_data_dirpath / "ProcessoAssociacao.txt",
#             sep=";",
#             encoding="iso-8859-1",
#             decimal=",",
#             dtype=str,
#         )
#         merge = ["TipoAssociacao.txt"]
#         df = self.merge(df, merge)
#         return df

#     def transform(self):
#         logger.debug("Transformando associacoes")

#         df = self.extract()

#         columns = {
#             "DSProcesso": "processo1",
#             "DSProcessoAssociado": "processo2",
#             "DSTipoAssociacao": "tipo",
#             "DTAssociacao": "data_associacao",
#             "DTDesassociacao": "data_desassociacao",
#             "OBAssociacao": "observacao",
#         }
#         df = df.rename(columns=columns)[list(columns.values())]
#         df.observacao = df.observacao.str.replace("*", "")

#         self._write_csv(df, "associacoes.csv")

#     def load(self, database: DatabaseHandler):
#         logger.debug("Carregando associacoes")
#         database.upsert_csv(
#             "scm", "associacoes", self.clean_data_dirpath / "associacoes.csv"
#         )


# class Pessoas(SCMHelper):

#     def extract(self) -> dd.DataFrame:
#         df = dd.read_csv(
#             self.raw_data_dirpath / "Pessoa.txt",
#             sep=";",
#             encoding="iso-8859-1",
#             decimal=",",
#             dtype=str,
#         )
#         return df

#     def transform(self):
#         logger.debug("Transformando pessoas")

#         df = self.extract()

#         columns = {
#             "IDPessoa": "pessoa",
#             "NRCPFCNPJ": "cpf_cnpj",
#             "TPPessoa": "tipo",
#             "NMPessoa": "nome",
#         }
#         df = df.rename(columns=columns)[list(columns.values())]
#         df = df.drop_duplicates(subset=["cpf_cnpj", "nome"])

#         self._write_csv(df, "pessoas.csv")

#     def load(self, database: DatabaseHandler):
#         logger.debug("Carregando pessoas")
#         database.upsert_csv("scm", "pessoas", self.clean_data_dirpath / "pessoas.csv")


# class Relacoes(SCMHelper):

#     def extract(self) -> dd.DataFrame:
#         df = dd.read_csv(
#             self.raw_data_dirpath / "ProcessoPessoa.txt",
#             sep=";",
#             encoding="iso-8859-1",
#             decimal=",",
#             dtype=str,
#         )
#         merge = [
#             "TipoRelacao.txt",
#             "TipoResponsabilidadeTecnica.txt",
#             "TipoRepresentacaoLegal.txt",
#         ]
#         df = self.merge(df, merge)
#         return df

#     def transform(self):
#         logger.debug("Transformando relacoes")

#         df = self.extract()

#         columns = {
#             "DSProcesso": "processo",
#             "IDPessoa": "pessoa",
#             "DSTipoRelacao": "tipo",
#             "DSTipoResponsabilidadeTecnica": "responsabilidade_tecnica",
#             "DSTipoRepresentacaoLegal": "representacao_legal",
#             "DTInicioVigencia": "inicio_vigencia",
#             "DTFimVigencia": "fim_vigencia",
#             "DTPrazoArrendamento": "prazo_arrendamento",
#         }
#         df = df.rename(columns=columns)[list(columns.values())]
#         df.responsabilidade_tecnica = df.responsabilidade_tecnica.str.replace("***", "")
#         df.representacao_legal = df.representacao_legal.str.replace("***", "")
#         pkey = ["processo", "pessoa", "tipo", "inicio_vigencia"]
#         df = df.drop_duplicates(subset=pkey, keep="last")

#         self._write_csv(df, "relacoes.csv")

#     def load(self, database: DatabaseHandler):
#         logger.debug("Carregando relacoes")
#         database.upsert_csv("scm", "relacoes", self.clean_data_dirpath / "relacoes.csv")


# class Substancias(SCMHelper):

#     def extract(self) -> dd.DataFrame:
#         df = dd.read_csv(
#             self.raw_data_dirpath / "ProcessoSubstancia.txt",
#             sep=";",
#             encoding="iso-8859-1",
#             decimal=",",
#             dtype=str,
#         )
#         merge = [
#             "Substancia.txt",
#             "TipoUsoSubstancia.txt",
#             "MotivoEncerramentoSubstancia.txt",
#         ]
#         df = self.merge(df, merge)
#         return df

#     def transform(self):
#         logger.debug("Transformando substancias")

#         df = self.extract()

#         columns = {
#             "DSProcesso": "processo",
#             "NMSubstancia": "substancia",
#             "DSTipoUsoSubstancia": "tipo_uso",
#             "DSMotivoEncerramentoSubstancia": "motivo_encerramento",
#         }
#         df = df.rename(columns=columns)[list(columns.values())]
#         df.tipo_uso = df.tipo_uso.str.replace("Não informado", "")
#         df = df.sort_values(by=["motivo_encerramento"])
#         df = df.groupby(["processo", "substancia"]).first().reset_index()

#         self._write_csv(df, "substancias.csv")

#     def load(self, database: DatabaseHandler):
#         logger.debug("Carregando substancias")
#         database.upsert_csv(
#             "scm", "substancias", self.clean_data_dirpath / "substancias.csv"
#         )


# class Municipios(SCMHelper):

#     def extract(self) -> dd.DataFrame:
#         df = dd.read_csv(
#             self.raw_data_dirpath / "ProcessoMunicipio.txt",
#             sep=";",
#             encoding="iso-8859-1",
#             decimal=",",
#             dtype=str,
#         )
#         merge = ["Municipio.txt"]
#         df = self.merge(df, merge)
#         return df

#     def transform(self):
#         logger.debug("Transformando municipios")

#         df = self.extract()

#         columns = {
#             "DSProcesso": "processo",
#             "SGUF": "uf",
#             "NMMunicipio": "municipio",
#         }
#         df = df.rename(columns=columns)[list(columns.values())]

#         self._write_csv(df, "municipios.csv")

#     def load(self, database: DatabaseHandler):
#         logger.debug("Carregando municipios")
#         database.upsert_csv(
#             "scm", "municipios", self.clean_data_dirpath / "municipios.csv"
#         )


# class PropriedadesSolo(SCMHelper):

#     def extract(self) -> dd.DataFrame:
#         df = dd.read_csv(
#             self.raw_data_dirpath / "ProcessoPropriedadeSolo.txt",
#             sep=";",
#             encoding="iso-8859-1",
#             decimal=",",
#             dtype=str,
#         )
#         merge = ["CondicaoPropriedadeSolo.txt"]
#         df = self.merge(df, merge)
#         return df

#     def transform(self):
#         logger.debug("Transformando propriedades_solo")

#         df = self.extract()

#         columns = {
#             "DSProcesso": "processo",
#             "DSCondicaoPropriedadeSolo": "condicao",
#         }
#         df = df.rename(columns=columns)[list(columns.values())]

#         self._write_csv(df, "propriedades_solo.csv")

#     def load(self, database: DatabaseHandler):
#         logger.debug("Carregando propriedades_solo")
#         database.upsert_csv(
#             "scm",
#             "propriedades_solo",
#             self.clean_data_dirpath / "propriedades_solo.csv",
#         )


# class Scm(ETLPipeline):

#     helpers = (
#         Processos,
#         Associacoes,
#         Pessoas,
#         Relacoes,
#         Substancias,
#         Municipios,
#         PropriedadesSolo,
#     )

#     def extract(self):
#         logger.info("Extraindo os dados do Cadastro Mineiro")

#         URL = "https://app.anm.gov.br/dadosabertos/SCM/microdados/microdados-scm.zip"
#         zip_filepath = self.raw_data_dirpath / "microdados-scm.zip"
#         logger.debug(f"Baixando arquivo zip de {URL}")
#         logger.debug(f"Baixando de {URL} para {self.raw_filepath}")

#         request, file = requests.get(URL, stream=True), open(zip_filepath, "wb")
#         with request, file:
#             for chunk in request.iter_content(chunk_size=8192):
#                 file.write(chunk)

#         with ZipFile(zip_filepath, "r") as zip_ref:
#             zip_ref.extractall(self.raw_data_dirpath)

#         logger.debug(f"Arquivos extraídos para {self.raw_data_dirpath}")

#     def transform(self):
#         logger.info("Limpando os dados do Cadastro Mineiro")

#         for helper_cls in Scm.helpers:
#             helper = helper_cls(
#                 self.raw_data_dirpath / "microdados-scm", self.clean_data_dirpath
#             )
#             helper.transform()

#     def load(self, database: DatabaseHandler):
#         logger.info("Carregando os dados limpos do Cadastro Mineiro no banco de dados")

#         for cls in Scm.helpers:
#             helper = cls(self.raw_data_dirpath, self.clean_data_dirpath)
#             helper.load(database)
