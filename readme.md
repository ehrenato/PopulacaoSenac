Desafio Técnico – Dados e Analytics | Senac RN
Introdução
Repositório que visa atender à demanda técnica do Desafio Senac de Dados e Analytics. Nele, estão contidos scripts python, pipelines utilizados e arquivos SQL de consulta. Ao final, responde-se questões como: Municípios mais populosos, a população por estado e estado com maior média de população baseando-se em APIs  públicas do IBGE.

Visão Geral

Este projeto foi desenvolvido como solução para o Desafio Técnico – Dados e Analytics | Aviso 01/2026, com o objetivo de demonstrar, de forma prática, a capacidade de estruturar um pipeline de dados completo, contemplando as etapas de extração, armazenamento, transformação, consolidação analítica e disponibilização para análise via SQL.

A solução utiliza dados públicos da API do IBGE, organizados em uma arquitetura em camadas (bronze, prata e ouro), seguindo boas práticas de engenharia de dados, com foco em clareza, reprocessamento seguro e facilidade de automação.

Arquitetura da Solução

A arquitetura adotada segue o conceito de camadas de dados, amplamente utilizado em ambientes analíticos institucionais.

EXTRAÇÃO (API IBGE)
        ↓
BRONZE – Dados brutos (Parquet)
        ↓
PRATA – Dados tratados e normalizados (Parquet + SQLite)
        ↓
OURO – Dados consolidados para consumo analítico (Parquet)

Estrutura do Projeto
desafio-senac-dados/
│
├── data/
│   ├── bronze/        # Dados brutos extraídos da API
│   ├── prata/         # Dados tratados e normalizados
│   └── ouro/          # Dados consolidados para consumo analítico
│
├── src/
│   ├── extract/       # Extração de dados
│   ├── transform/     # Transformação (prata e ouro)
│   ├── load/          # Carga no banco analítico
│   └── pipeline.py    # Orquestração do pipeline
│
├── sql/               # Consultas SQL analíticas
├── database/          # Banco SQLite
├── requirements.txt
└── README.md
Fonte de Dados

Os dados utilizados neste projeto são provenientes da API pública do IBGE, incluindo:

Lista de estados

Lista de municípios

População por município (Censo 2022)

Documentação oficial:
https://servicodados.ibge.gov.br/api/docs

Tecnologias Utilizadas

Python 3.10+

requests – Consumo da API

pandas – Manipulação e transformação de dados

pyarrow – Armazenamento em Parquet

SQLite – Banco de dados analítico

SQL padrão

Etapas do Pipeline
1Extração – Camada Bronze

Consumo da API do IBGE

Tratamento básico de erros de requisição

Armazenamento dos dados em formato bruto, sem transformações

Persistência em arquivos Parquet

data/bronze/

Transformação – Camada Prata

Normalização dos dados

Definição de chaves primárias e estrangeiras

Remoção de duplicidades

Adequação dos dados ao modelo relacional solicitado

Tabelas geradas:

estados

municipios

populacao

data/prata/

Consolidação – Camada Ouro

A camada ouro contém dados prontos para consumo analítico, já integrados e consolidados.

Nesta solução, a camada ouro foi mantida em formato Parquet, desacoplada do banco relacional, representando uma visão analítica otimizada para leitura, BI e exploração futura.

data/ouro/populacao_municipio_2022.parquet

4Carga – Banco Analítico

Carregamento da camada prata em um banco SQLite

Criação de tabelas relacionais

Garantia de reexecução segura do pipeline

database/analytics.db

Camada Analítica em SQL

As consultas SQL solicitadas no desafio estão disponíveis na pasta sql/:

Top 10 municípios mais populosos do Brasil

População total por estado

Estado com maior média de população por município

Ranking das regiões do Brasil por população total

Todas as queries utilizam SQL padrão e executam diretamente sobre o banco SQLite.

Como Executar o Projeto
1. Clonar o repositório
git clone <url-do-repositorio>
cd desafio-senac-dados
2. Criar ambiente virtual (opcional)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
3. Instalar dependências
pip install -r requirements.txt
4. Executar o pipeline completo
python src/pipeline.py

Ao final da execução:

Os dados estarão organizados nas camadas bronze, prata e ouro

O banco SQLite estará pronto para consultas analíticas

Decisões Técnicas Adotadas

Arquitetura em camadas para garantir separação de responsabilidades

Uso de Parquet para eficiência e reprocessamento seguro

SQLite escolhido pela simplicidade, portabilidade e adequação ao escopo do desafio

Pipeline sequencial e claro, facilitando automação futura

Código modular, reutilizável e documentado

Possíveis Melhorias Futuras

Carga incremental de dados

Integração com SQL Server ou PostgreSQL

Agendamento com SQL Server Agent ou orquestradores (Airflow)

Testes automatizados

Criação de views analíticas no banco

Integração com ferramentas de BI

Considerações Finais

A solução proposta busca equilibrar simplicidade, clareza e boas práticas de engenharia de dados, atendendo integralmente aos requisitos do desafio e refletindo um pipeline próximo ao que seria utilizado em um ambiente institucional real.