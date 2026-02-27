Desafio Técnico – Dados e Analytics | Senac RN
Introdução
Repositório que visa atender à demanda técnica do Desafio Senac de Dados e Analytics. Nele, estão contidos scripts python, pipelines utilizados e arquivos SQL de consulta. Ao final, responde-se questões como: Municípios mais populosos, a população por estado e estado com maior média de população baseando-se em APIs  públicas do IBGE.

Visão Geral

A solução utiliza dados públicos da API do IBGE, organizados em uma arquitetura em camadas (bronze, prata e ouro), seguindo boas práticas de engenharia de dados, com foco em clareza, reprocessamento seguro e facilidade de automação.

EXTRAÇÃO (API IBGE)
- BRONZE – Dados brutos (Parquet)
- PRATA – Dados tratados e normalizados (Parquet + SQLite)
- OURO – Dados consolidados para consumo analítico (Parquet)

Estrutura do Projeto
data/
─ bronze/        
─ prata/         
─ ouro/          
src/
─ extract/       
─ transform/     
─ load/          
── pipeline.py    
 sql/            
─ database/      
─ requirements.txt
─ README.md

Fonte de Dados

Os dados utilizados neste projeto são provenientes da API pública do IBGE, incluindo:

- Lista de estados
- Lista de municípios
- População por município (Censo 2022)

Documentação oficial:
https://servicodados.ibge.gov.br/api/docs

Tecnologias Utilizadas

- Python 3.10+
- requests – Consumo da API
- pandas – Manipulação e transformação de dados
- pyarrow – Armazenamento em Parquet
- SQLite – Banco de dados analítico
- SQL padrão

Etapas do Pipeline
1- Extração – Camada Bronze
Consumo da API do IBGE
Tratamento básico de erros de requisição
Armazenamento dos dados em formato bruto, sem transformações
Persistência em arquivos Parquet
data/bronze/

2 - Transformação – Camada Prata
Normalização dos dados
Definição de chaves primárias e estrangeiras
Remoção de duplicidades
Adequação dos dados ao modelo relacional solicitado
Tabelas geradas:
estados
municipios
populacao
data/prata/

3 - Consolidação – Camada Ouro
A camada ouro contém dados prontos para consumo analítico, já integrados e consolidados.
A camada ouro foi mantida em formato Parquet, desacoplada do banco relacional, representando uma visão analítica otimizada para leitura, BI e exploração futura.
data/ouro/populacao_municipio_2022.parquet

4- Carga – Banco Analítico
Carregamento da camada prata em um banco SQLite
Criação de tabelas relacionais
Garantia de reexecução segura do pipeline
database/analytics.db

5 - Camada Analítica em SQL
As consultas SQL solicitadas no desafio estão disponíveis na pasta sql/:
Top 10 municípios mais populosos do Brasil
População total por estado
Estado com maior média de população por município
Ranking das regiões do Brasil por população total

Todas as queries utilizam SQL padrão e executam diretamente sobre o banco SQLite.

Como Executar o Projeto
1. Clonar o repositório
git clone https://github.com/ehrenato/PopulacaoSenac
cd PopulacaoSenac

2. Criar ambiente virtual (opcional)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

4. Instalar dependências
pip install -r requirements.txt

6. Executar o pipeline completo
python -m src.pipeline

Ao final da execução:
Os dados estarão organizados nas camadas bronze, prata e ouro
O banco SQLite estará pronto para consultas analíticas