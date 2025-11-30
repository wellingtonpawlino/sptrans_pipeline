
# 🚍 SPTrans Pipeline

**`Data Engineering | Python | Airflow | Spark | Cloud`**

![Header](https://capsule-render.vercel.app/api?type=waving&color=gradient&height=200&section=header&text=SPTrans%20Pipeline&fontSize=40&fontColor=fff&animation=fadeIn)

[![Typing SVG](https://readme-typing-svg.herokuapp.com?color=%23F7A80D&size=22&width=500&lines=Pipeline+de+Dados+SPTrans!;Airflow+%7C+Spark+%7C+MinIO+%7C+Metabase;Automacao+e+Analise+de+Dados)](https://git.io/typing-svg)

---

## 📣 Conecte-se comigo
<p align="left"> 
  <a href="https://github.com/wellingtonpawlino?tab=followers">
    <img alt="followers" title="Follow me on Github" src="https://custom-icon-badges.demolab.com/github/followers/wellingtonpawlino?color=A0522D&labelColor=D2691E&style=for-the-badge&logo=person-add&label=Follow&logoColor=white"/>
  </a>
  <a href="https://github.com/wellingtonpawlino?tab=repositories&sort=stargazers">
    <img alt="total stars" title="Total stars on GitHub" src="https://custom-icon-badges.demolab.com/github/stars/wellingtonpawlino?color=55960c&style=for-the-badge&labelColor=488207&logo=star&v=2&cache-control=no-cache"/>
  </a>
  <a href="https://www.linkedin.com/in/wellington-santos/" target="_blank">
    <img alt="LinkedIn Profile" title="Connect with me on LinkedIn" src="https://custom-icon-badges.demolab.com/badge/-LinkedIn-blue?style=for-the-badge&logo=linkedin&logoColor=white"/>
  </a>
  <a href="https://www.youtube.com/@wellingtonsantos9019/videos" target="_blank">
    <img alt="YouTube Videos" title="Confira meus vídeos no YouTube!" src="https://custom-icon-badges.demolab.com/badge/-Meus%20Vídeos-red?style=for-the-badge&logo=youtube&logoColor=white"/>
  </a>
</p>


<h2 align="center">✨ Projeto SPTrans Pipeline ✨</h2>

<p align="center">
Este projeto implementa um <strong>pipeline de dados</strong> para <em>coleta</em>, <em>processamento</em> e <em>análise</em> das informações da 
<strong>API Olho Vivo</strong> da SPTrans, permitindo <u>monitoramento em tempo real</u> da frota de ônibus da cidade de São Paulo.
</p>

---

## 📌 <span style="font-family: 'Georgia', serif;">Objetivo</span>
Automatizar a <strong>ingestão</strong> e <strong>transformação</strong> dos dados fornecidos pela SPTrans, possibilitando análises sobre:
- 🚌 <span style="font-family: 'Georgia', serif;">Localização dos veículos</span>
- 🗺️ <span style="font-family: 'Georgia', serif;">Linhas e itinerários</span>
- ⚙️ <span style="font-family: 'Georgia', serif;">Status operacional</span>


## 🏗️  <span style="font-family: 'Georgia', serif;">Arquitetura do Projeto</span>

📌 **Componentes principais:**
- ⚙️ **Airflow** → PostgreSQL (metadados e dados)
- ☁️ **Airflow** → MinIO (Data Lake)
- 🔗 **Spark** → Airflow (processamento distribuído)
- 📓 **Jupyter** → Spark e MinIO (análise exploratória)
- 📊 **Metabase** → PostgreSQL (dashboards)

📷 **Diagrama da Arquitetura:**  
`docs/arquitetura.png`

---

## 🧰 Tecnologias Utilizadas

<p>
  <img alt="Python" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg"/>
  <img alt="Airflow" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apacheairflow/apacheairflow-original.svg"/>
  <img alt="PostgreSQL" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg"/>
  <img alt="Docker" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg"/>
  <img alt="Spark" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/apache/apache-original.svg"/>
  <img alt="Linux" width="40px" src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/linux/linux-original.svg"/>
</p>

---

## ✅ Badges do Projeto
![Docker](https://img.shields.io/badge/Docker-✔-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.7.1-green)
![Python](https://img.shields.io/badge/Python-3.11-yellow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Spark](https://img.shields.io/badge/Spark-✔-orange)
![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-lightgrey)

---

## 📂 Estrutura do Projeto

```text
📦 sptrans_pipeline/
├── 📁 airflow/              # ⚙️ DAGs, logs e plugins do Airflow
├── 📁 api/                  # 🔗 Scripts para integração com API Olho Vivo
├── 📁 processors/           # ⚙️ Processadores ETL
├── 📁 notebooks/            # 📓 Jupyter Notebooks para análise
├── 📁 data/                 # 🗃️ Dados brutos e processados
├── 📄 Dockerfile.airflow    # 🐳 Imagem customizada do Airflow
├── 📄 Dockerfile.spark      # 🐳 Imagem customizada do Spark
├── 📄 Dockerfile.jupyter    # 📓 Imagem customizada do Jupyter
├── 📄 docker-compose.yml    # 🏗️ Orquestração dos serviços
├── 📄 .env                  # 🔐 Variáveis de ambiente
└── 📄 README.md             # 📘 Documentação do projeto

````

## 🚀 Como Executar o Projeto

### ✅ Pré-requisitos
- 🐳 **Docker** e **Docker Compose** instalados
- 🐍 **Python 3.11** (para scripts locais)
- 🔐 Arquivo `.env` configurado com suas credenciais e token SPTrans

---
## 🔧 Passo a Passo
**Clone o repositório:**

```bash
  git clone git@github.com:wellingtonpawlino/sptrans_pipeline.git
  docker compose up -d build
```

## 🌐 Acesse os Serviços

| Serviço                     | Valor       |
|---------------------------|------------|
| **Airflow**     | [http://localhost:8080](http://localhost:8080)|
| **PgAdmin**     | [http://localhost:5050](http://localhost:5050)|
| **Metabase**     | [http://localhost:3000](http://localhost:3000)|
| **MinIO Console**     | [http://localhost:9001](http://localhost:9001)|
| **Jupyter Notebook**     | [http://localhost:8889](http://localhost:8889)|
| **Apache Spark UI**     | [http://localhost:8081](http://localhost:8081)|


## 🛠️ Conexão com o Banco via PgAdmin 

| Campo                     | Valor       |
|---------------------------|------------|
| **Host name/address**     | `db`       |
| **Port**                  | `5432`     |
| **Maintenance database**  | `sptrans`  |
| **Username**              | `postgres` |
| **Password**              | `postgres` |

