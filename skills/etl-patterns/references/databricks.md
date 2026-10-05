# Exemplos em Databricks e Delta Lake

Use estes trechos como ponto de partida e adapte nomes de tabelas, catálogos e caminhos. Sintaxe e nomes de recursos do Databricks mudam com frequência (por exemplo, o recurso antes chamado Delta Live Tables hoje aparece como Lakeflow Spark Declarative Pipelines). Em caso de dúvida sobre a sintaxe atual, confira a documentação oficial.

Conteúdo:
1. Full Load
2. Incremental com marca d'água
3. CDC: mudanças dentro do Delta (Change Data Feed) e vindas de bancos externos
4. Upsert / Merge
5. Append-only com Auto Loader
6. Snapshot
7. SCD Type 2
8. Partition-based (replaceWhere e liquid clustering)
9. Micro-batch
10. Backfill

---

## 1. Full Load

```sql
-- Substitui a tabela inteira de forma atômica (o consumidor nunca vê tabela vazia)
CREATE OR REPLACE TABLE silver.produtos AS
SELECT * FROM origem.produtos;
```

Alternativa que mantém a definição da tabela: `INSERT OVERWRITE silver.produtos SELECT * FROM origem.produtos;`

---

## 2. Incremental com marca d'água

Guarde a marca d'água em uma tabela de controle e atualize só depois do sucesso da carga.

```sql
-- Tabela de controle: ctrl.watermarks (tabela STRING, ultimo_valor TIMESTAMP)

-- 1) Extrai com janela de segurança de 2 horas para não perder registros atrasados
CREATE OR REPLACE TEMP VIEW stg_pedidos AS
SELECT *
FROM origem.pedidos
WHERE updated_at > (
  SELECT ultimo_valor - INTERVAL 2 HOURS
  FROM ctrl.watermarks
  WHERE tabela = 'pedidos'
);

-- 2) Aplica no destino com MERGE (idempotente, ver seção 4)

-- 3) Só depois do sucesso, avança a marca d'água
UPDATE ctrl.watermarks
SET ultimo_valor = (SELECT MAX(updated_at) FROM stg_pedidos)
WHERE tabela = 'pedidos';
```

Lembrete: este padrão não enxerga deletes na origem.

---

## 3. CDC

Há dois cenários diferentes que costumam ser confundidos.

**a) Mudanças dentro de uma tabela Delta (Change Data Feed).** Útil para propagar o que mudou da silver para a gold sem reler a tabela toda.

```sql
ALTER TABLE silver.clientes
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Lê mudanças entre duas versões da tabela
SELECT * FROM table_changes('silver.clientes', 12, 15);
-- Colunas extras: _change_type (insert, update_preimage, update_postimage, delete),
-- _commit_version, _commit_timestamp
```

**b) CDC vindo de um banco transacional externo.** Isso é feito por uma ferramenta de captura (Debezium com Kafka, AWS DMS, Datastream, ou conectores gerenciados do Databricks). Os eventos chegam em arquivos ou tópicos e são gravados no bronze (append-only), contendo normalmente: chave, tipo da operação (I, U, D), valores novos e uma coluna de sequência (LSN, offset ou timestamp do commit). A aplicação no destino usa Merge ou o fluxo de CDC dos pipelines declarativos (seção 7).

---

## 4. Upsert / Merge

Sempre deduplique a origem por chave antes do merge. Se houver duas linhas da mesma chave no lote, o merge falha ou escolhe uma de forma imprevisível.

```sql
MERGE INTO silver.clientes AS t
USING (
  SELECT *
  FROM bronze.clientes_stg
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT *;
```

Pontos de atenção:
- A condição `s.updated_at > t.updated_at` evita que um registro antigo chegando atrasado sobrescreva um novo.
- `UPDATE SET *` e `INSERT *` assumem colunas com os mesmos nomes entre origem e destino. Se a origem trouxer colunas de controle (como `op`), selecione explicitamente as colunas, ou configure evolução de schema conscientemente.
- Para tabelas grandes, o ON deve incluir um filtro que ajude a podar arquivos (por exemplo, uma coluna de partição ou de clusterização).

---

## 5. Append-only com Auto Loader

```python
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/catalogo/schema/vol/_schemas/eventos")
    .load("/Volumes/catalogo/schema/vol/landing/eventos")
    .writeStream
    .option("checkpointLocation", "/Volumes/catalogo/schema/vol/_checkpoints/eventos")
    .trigger(availableNow=True)   # processa o que chegou e encerra; agende em um Job
    .toTable("bronze.eventos"))
```

Boas práticas do bronze: manter o dado cru, adicionar colunas de rastreio (`_ingestion_ts`, `_source_file`) e uma chave de evento para permitir deduplicação downstream.

---

## 6. Snapshot

```sql
-- Snapshot diário do estoque, idempotente: rodar duas vezes no mesmo dia substitui o mesmo snapshot
INSERT INTO gold.estoque_snapshot
REPLACE WHERE snapshot_date = DATE'2026-10-04'
SELECT *, DATE'2026-10-04' AS snapshot_date
FROM silver.estoque;
```

Consulta "como estava em 31/01?":

```sql
SELECT * FROM gold.estoque_snapshot WHERE snapshot_date = DATE'2026-01-31';
```

Não use time travel do Delta como substituto de snapshot de negócio: a retenção é limitada e o VACUUM remove versões antigas.

---

## 7. SCD Type 2

O jeito mais seguro é usar o fluxo de CDC dos pipelines declarativos, que cuida de ordenação, duplicatas e fechamento de linhas.

```python
import dlt
from pyspark.sql.functions import col, expr

dlt.create_streaming_table("silver_clientes_scd2")

dlt.apply_changes(
    target="silver_clientes_scd2",
    source="bronze_clientes_cdc",
    keys=["customer_id"],
    sequence_by=col("seq"),                    # ordem dos eventos, vinda do log
    apply_as_deletes=expr("op = 'D'"),
    except_column_list=["op", "seq"],
    stored_as_scd_type=2
)
```

O resultado traz as colunas `__START_AT` e `__END_AT` (a linha vigente tem `__END_AT` nulo). Nas versões mais novas o recurso aparece com o nome "AUTO CDC" (por exemplo, `create_auto_cdc_flow`); verifique o nome vigente na documentação.

Para SCD Type 2 escrito à mão com MERGE, o raciocínio é: (1) fechar a linha vigente da chave que mudou (`valid_to` = data da mudança, `is_current` = false); (2) inserir a nova versão com `valid_from` = data da mudança e `is_current` = true. É mais frágil com eventos fora de ordem; prefira o fluxo gerenciado quando possível.

Join de fato com dimensão SCD2 (versão válida na data do fato):

```sql
SELECT f.*, d.city
FROM gold.fato_pedidos f
JOIN silver.dim_clientes d
  ON f.customer_id = d.customer_id
 AND f.data_pedido >= d.valid_from
 AND (f.data_pedido < d.valid_to OR d.valid_to IS NULL);
```

---

## 8. Partition-based

Substituir só a fatia do dia, sem tocar no resto:

```sql
INSERT INTO silver.transacoes
REPLACE WHERE data_transacao = DATE'2026-08-21'
SELECT * FROM stg.transacoes
WHERE data_transacao = DATE'2026-08-21';
```

Em Python: `df.write.mode("overwrite").option("replaceWhere", "data_transacao = '2026-08-21'").saveAsTable("silver.transacoes")`.

Organização física de uma tabela nova com liquid clustering:

```sql
CREATE TABLE silver.transacoes (
  id BIGINT,
  data_transacao DATE,
  valor DECIMAL(18,2)
)
CLUSTER BY (data_transacao);
```

Para tabelas muito grandes com filtro previsível por data, o particionamento tradicional (`PARTITIONED BY`) ainda é opção. Evite particionar por colunas de alta cardinalidade.

---

## 9. Micro-batch

Duas formas comuns de rodar a cada 15 minutos:

**Streaming com intervalo fixo (cluster ligado):**

```python
(spark.readStream.table("bronze.eventos")
    .writeStream
    .option("checkpointLocation", "/Volumes/catalogo/schema/vol/_checkpoints/silver_eventos")
    .trigger(processingTime="15 minutes")
    .foreachBatch(aplicar_lote)      # função que faz o MERGE, ver abaixo
    .start())
```

**Job agendado a cada 15 minutos com `availableNow=True`:** processa o acumulado e desliga o compute. Normalmente mais barato quando a latência de 15 minutos é aceitável.

Função de aplicação do lote:

```python
def aplicar_lote(batch_df, batch_id):
    batch_df.createOrReplaceTempView("lote")
    batch_df.sparkSession.sql("""
        MERGE INTO silver.clientes AS t
        USING (
          SELECT * FROM lote
          QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY seq DESC) = 1
        ) AS s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.op = 'D' THEN DELETE
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT *
    """)
```

Compacte arquivos pequenos periodicamente (`OPTIMIZE silver.clientes`) ou use otimização automática, para o micro-batch não degradar a leitura.

---

## 10. Backfill

Escreva o job normal já parametrizado por intervalo de datas. O backfill passa a ser o mesmo código com parâmetros diferentes.

```python
from datetime import date, timedelta

def processar_dia(dia: date):
    d = dia.isoformat()
    (spark.table("bronze.transacoes_raw")
        .where(f"data_transacao = '{d}'")
        # ... transformação versionada e idempotente ...
        .write.mode("overwrite")
        .option("replaceWhere", f"data_transacao = '{d}'")
        .saveAsTable("silver.transacoes"))

def rodar(data_inicio: date, data_fim: date):
    dia = data_inicio
    while dia <= data_fim:
        processar_dia(dia)
        dia += timedelta(days=1)

# Execução normal: rodar(hoje, hoje)
# Backfill de jan a mar: rodar(date(2026, 1, 1), date(2026, 3, 31))
```

Checklist antes de disparar um backfill:
1. A transformação é idempotente (replaceWhere ou merge, nunca append cego).
2. O dado bruto do período existe e não foi alterado.
3. As tabelas dependentes (gold, relatórios) serão reprocessadas na ordem.
4. Não há carga normal escrevendo nas mesmas partições ao mesmo tempo.
5. Há uma validação pronta (contagens e totais antes e depois).
