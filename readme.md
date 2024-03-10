# Objetivo
O objetivo deste script é extrair os dados públicos da base Sistema de Informações Geográficas da Mineração, disponíveis em: https://geo.anm.gov.br/portal/apps/webappviewer/index.html?id=6a8f5ccc4b6a4c2bba79759aa952d908, e enviar estes dados para uma base de dados PostGres hospedada em um servidor Linux com o objetivo de serem usados em um dashboard.

# Instruções

## 1. Clone o repositório na pasta desejada
Use o seguinte comando para clonar o repositório na pasta desejada (recomenda-se a pasta `/opt`):

```
git clone {link deste repositório} /opt/scraper-sigmine
```

## 2. Intale o Miniconda
Siga as instruções a seguir: https://docs.anaconda.com/free/miniconda/#quick-command-line-install

## 3. Instale o ambiente Python
O comando a seguir usa o Miniconda para a criação do ambiente virtual (importante: o caminho completo deve ser utilizado):

```
conda create -p /opt/scraper-sigmine/.conda --file /opt/scraper-sigmine/requirements.txt python=3.11
```

## 4. Aponte o ambiente virtual
Abra o arquivo `main.py`
```
nano /opt/scraper-sigmine/src/main.py
```

Inclua a seguinte linha no começo do arquivo para indicar ao sistema que deve usar o ambiente virtual:

```
#!/opt/scraper-sigmine/.conda/bin/python
```

## 5. Adicione a permissão de execução ao arquivo
O seguinte comando permite que o arquivo seja executado:

```
chmod +x /opt/scraper-sigmine/src/main.py
```

## 6. Preencha as informações de configuração
Abra o arquivo `config.ini` e preencha as informações com asterisco.
```
nano /opt/scraper-sigmine/config.ini
```

Ele armazena as configurações de execução do script e deve possuir o seguinte formato:

```
[logger]
log_path = logs (Pasta ondes os logs do programa serção armazenados)
log_level = INFO (Nível de detalhe do log; escolha entre DEBUG, INFO, WARNING e ERROR)

[scraper]
output_path = scraper-output/cache.parquet (Caminho do cache de dados baixados; também suporta o formato .csv)
timestamps_path = scraper-output/timestamps.json (Caminho das timestamps)
processos_path = scraper-output/processos.csv (Caminho os dados da tabela processos)
fases_path = scraper-output/fases.csv (Caminho dos dados da tabela fases)
max_threads = 10 (Quantidade máxima de threads usadas para download dos dados)

[database]
user = developer
password = ****** (Mesma senha usada para acessar o servidor)
host = ****** (localhost caso esteja dentro do servidor, senão, o IP do servidor)
port = 9856
database = sigmine_db
```

## 7. Pronto! O script pode ser usado
Para executar, invoque `main.py` no terminal:
```
root@manding:~# /opt/scraper-sigmine/src/main.py
```

## 8. Agendamento da execução
Para executar o arquivo automaticamente e rotineiramente, abra o arquivo de configuração do `cron` com:
```
crontab -e
```
E insira uma linha como indicado; por exemplo, para indicar que o programa deve rodar todo dia às 3 da manhã, insira:
```
0 3 * * * /opt/extrator-sigmine/src/main.py
```

# Outras informações

## Algoritmo
1. Verifique se há um arquivo com dados baixados.
2. Se não existir, baixe todos os dados.
3. Se existir e não for recente, verifique se há um arquivo de timestamps.
4. Se não existir, baixe todos os dados.
5. Se existir, compare com as timestamps no sigmine para saber quais dados estão desatualizados e baixe somente eles.
6. Se todos os estados estiverem atualizados, encerre.
7. Copie as timestamps atualizadas e os dados baixados nos locais especificados em `config.ini`.
8. Realize o tratamento dos dados (seleção de colunas, exclusões de valores nulos, etc.).
9. Salve os dados tratados em arquivos csv.
10. Exclua as tabelas com os dados antigos no banco.
11. Crie novas tabelas e preencha-as com os novos dados.
12. Dê as permissões necessárias para o usuário do dashboard.

## Sobre a extração dos dados
O programa percorre os links de download do SIGMINE para baixar os dados necessários. Os dados são baixados dentro de um arquivo .zip, que é descomprimido, e possuem formato .dbf, que é interpretado usando a bilbioteca `dbfread`. Todos os arquivos são baixados simultaneamente por meio de multithreading e armazenados em uma pasta temporária que é excluída após o término de todos os downloads.

## Estrutura da base
### Tabela processos

Armazena os dados de cada processo. Processos são identificados por número e ano, ou seja, **se é o mesmo número mas são anos diferentes, são processos diferentes**.

*Estes dados são únicos para cada processo.*

Colunas:
- numero: Texto (numero do processo)
- ano: Texto (ano do processo)
- nome: Texto (nome da empresa)
- area_ha: Numero Decimal (área do processo, em hectares)
- subs: Texto (substância envolvida no processo)
- uso: Texto (tipo de uso associado ao processo)
- cod_ult: Texto (código ult)
- desc_ult: Texto (descrição da ult)

### Tabela fases

Armazena os estados e as fases de cada processo.

*Cada processo pode abranger diversos estados e possuir diversas fases.*

Colunas:
- numero: Texto (numero do processo)
- ano: Texto (ano do processo)
- mes: Texto (mês da criação da fase)
- dia: Texto (dia da criação da fase)
- fase: Texto (fase do processo)
- estado: Texto (estado do processo)