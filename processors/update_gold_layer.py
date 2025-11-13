import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

# Carregar variáveis do .env
load_dotenv()

POSTGRES_CONN = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

def get_engine():
    """Cria e retorna uma engine SQLAlchemy para conexão com PostgreSQL."""
    conn_str = f"postgresql://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@" \
               f"{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['dbname']}"
    logging.info("✅ Conexão com PostgreSQL configurada.")
    return create_engine(conn_str)

def atualizar_camadas_gold():
    """Executa o script SQL para criar dimensões, fato, índices e view na camada GOLD."""
    logging.info("🚀 Iniciando atualização da camada GOLD...")
    engine = get_engine()

    # Script SQL completo
    sql_script = """
-- CAMADA GOLD COM IDS
-- ============================

--=============================
-- 1. Dimensão Linha
--=============================
DROP TABLE IF EXISTS gold.dim_linha;
CREATE TABLE gold.dim_linha AS
SELECT
    ROW_NUMBER() OVER (ORDER BY "CodigoLinha") AS id_linha,
    "CodigoLinha" AS codigo_linha,
    "DescricaoCompleto" AS descricao_completo
FROM (
    SELECT DISTINCT "CodigoLinha", "DescricaoCompleto"
    FROM silver.posicao_veiculos_enriquecida
) AS sub;

--=============================
-- 2. Dimensão Sentido
--=============================
DROP TABLE IF EXISTS gold.dim_sentido;
CREATE TABLE gold.dim_sentido AS
SELECT
    ROW_NUMBER() OVER (ORDER BY "Sentido") AS id_sentido,
    "Sentido" AS sentido
FROM (
    SELECT DISTINCT "Sentido"
    FROM silver.posicao_veiculos_enriquecida
) AS sub;

--=============================
-- 3. Dimensão Circular
--=============================
DROP TABLE IF EXISTS gold.dim_circular;
CREATE TABLE gold.dim_circular AS
SELECT
    ROW_NUMBER() OVER (ORDER BY "LinhaCircular") AS id_circular,
    "LinhaCircular" AS circular
FROM (
    SELECT DISTINCT "LinhaCircular"
    FROM silver.posicao_veiculos_enriquecida
) AS sub;

--=============================
-- 4. Dimensão Período do Dia
--=============================
DROP TABLE IF EXISTS gold.dim_periodo;
CREATE TABLE gold.dim_periodo AS
SELECT
    ROW_NUMBER() OVER (ORDER BY "PeriodoDia") AS id_periododia,
    "PeriodoDia" AS periododia
FROM (
    SELECT DISTINCT "PeriodoDia"
    FROM silver.posicao_veiculos_enriquecida
) AS sub;

--=============================
-- 5. Dimensão Dia da Semana
--=============================
DROP TABLE IF EXISTS gold.dim_diasemana;
CREATE TABLE gold.dim_diasemana AS
SELECT
    ROW_NUMBER() OVER (ORDER BY "DiaSemana") AS id_diasemana,
    "DiaSemana" AS diasemana
FROM (
    SELECT DISTINCT "DiaSemana"
    FROM silver.posicao_veiculos_enriquecida
) AS sub;

--=============================
-- 6. Dimensão Hora
--=============================
DROP TABLE IF EXISTS gold.dim_hora;
CREATE TABLE gold.dim_hora AS
SELECT
    ROW_NUMBER() OVER (ORDER BY "Hora") AS id_hora,
    "Hora" AS hora
FROM (
    SELECT DISTINCT "Hora"
    FROM silver.posicao_veiculos_enriquecida
) AS sub;

--=============================
-- 7. Dimensão Minuto
--=============================
DROP TABLE IF EXISTS gold.dim_minuto;
CREATE TABLE gold.dim_minuto AS
SELECT
    ROW_NUMBER() OVER (ORDER BY "Minuto") AS id_minuto,
    "Minuto" AS minuto
FROM (
    SELECT DISTINCT "Minuto"
    FROM silver.posicao_veiculos_enriquecida
) AS sub;

-- ============================
-- 8. Tabela Fato com FKs
-- ============================

DROP TABLE IF EXISTS gold.fato_posicao_veiculos;
CREATE TABLE gold.fato_posicao_veiculos AS
SELECT
    l.id_linha,
    s.id_sentido,
    c.id_circular,
    p.id_periododia,
    d.id_diasemana,
    h.id_hora,
    m.id_minuto,
    COUNT(*) AS qtd_registros
FROM silver.posicao_veiculos_enriquecida f
JOIN gold.dim_linha l ON f."CodigoLinha" = l.codigo_linha
JOIN gold.dim_sentido s ON f."Sentido" = s.sentido
JOIN gold.dim_circular c ON f."LinhaCircular" = c.circular
JOIN gold.dim_periodo p ON f."PeriodoDia" = p.periododia
JOIN gold.dim_diasemana d ON f."DiaSemana" = d.diasemana
JOIN gold.dim_hora h ON f."Hora" = h.hora
JOIN gold.dim_minuto m ON f."Minuto" = m.minuto
GROUP BY l.id_linha, s.id_sentido, c.id_circular, p.id_periododia, d.id_diasemana, h.id_hora, m.id_minuto;

-- ============================
-- 9. Índices para Performance
-- ============================
CREATE INDEX IF NOT EXISTS idx_fato_dia_hora_minuto
ON gold.fato_posicao_veiculos(id_diasemana, id_hora, id_minuto);

CREATE INDEX IF NOT EXISTS idx_fato_linha_sentido
ON gold.fato_posicao_veiculos(id_linha, id_sentido);

CREATE INDEX IF NOT EXISTS idx_fato_periodo
ON gold.fato_posicao_veiculos(id_periododia);

-- ==================================
-- View Com a ultima localização
-- =================================
DROP VIEW IF EXISTS gold.vw_ultima_posicao;

CREATE OR REPLACE VIEW gold.vw_ultima_posicao AS
WITH tbl_posicao_atual AS (
    SELECT 
        "CodigoLinha",
        MAX("HorarioAtualizacao") AS "HorarioAtualizacao"
    FROM silver.posicao_veiculos_enriquecida
    WHERE "data_particao" = (
        SELECT MAX("data_particao") 
        FROM silver.posicao_veiculos_enriquecida
    )
    GROUP BY "CodigoLinha"
)
SELECT 
    t1."CodigoLinha",
    t1."DescricaoCompleto",
    t1."Sentido",
    t1."LinhaCircular",
    t1."Latitude",
    t1."Longitude",
    t1."HorarioAtualizacao"
FROM silver.posicao_veiculos_enriquecida t1
INNER JOIN tbl_posicao_atual t2 
    ON t1."CodigoLinha" = t2."CodigoLinha" 
   AND t1."HorarioAtualizacao" = t2."HorarioAtualizacao"
WHERE t1."data_particao" = (
    SELECT MAX("data_particao") 
    FROM silver.posicao_veiculos_enriquecida
);

CREATE INDEX IF NOT EXISTS idx_silver_data_particao
ON silver.posicao_veiculos_enriquecida("data_particao");

CREATE INDEX IF NOT EXISTS idx_silver_linha_horario
ON silver.posicao_veiculos_enriquecida("CodigoLinha", "HorarioAtualizacao");
    """

    try:
        with engine.begin() as conn:
            logging.info("🔹 Conexão estabelecida com o banco.")
            logging.info("📄 Executando script SQL...")
            conn.execute(text(sql_script))
            logging.info("✅ Script SQL executado com sucesso!")
    except Exception as e:
        logging.error(f"❌ Erro ao atualizar camada GOLD: {e}")
        raise
    finally:
        logging.info("🏁 Processo finalizado.")

if __name__ == "__main__":
    atualizar_camadas_gold()
