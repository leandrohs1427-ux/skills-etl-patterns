# Exemplos em Databricks e Delta Lake

Conferido contra a documentação oficial do Databricks em 05/10/2026. Nomes de recursos mudam com frequência: o Delta Live Tables hoje aparece como Lakeflow Spark Declarative Pipelines, e o `APPLY CHANGES` foi substituído pelo `AUTO CDC`. Em caso de dúvida sobre sintaxe, confira a documentação e a versão do Databricks Runtime do usuário antes de afirmar.

Use estes trechos como ponto de partida e adapte nomes de catálogos, schemas e caminhos. Os que gravam dados assumem que as tabelas de destino já existem.

Conteúdo:
1. Full Load
2. Incremental
3. CDC (AUTO CDC, Change Data Feed e captura externa)
4. Upsert / Merge
5. Append-only com Auto Loader
6. Snapshot
7. SCD Type 2
8. Partition-based (liquid clustering, REPLACE WHERE e REPLACE USING)
9. Micro-batch
10. Backfill e Replay

---

## 1. Full Load

```sql
-- Substitui a tabela inteira em uma única transação.
-- O histórico de versões é mantido (dá para voltar com RESTORE) e quem lê a tabela
-- durante a troca continua lendo a versão anterior, sem erro.
CREATE OR REPLACE TABLE catalogo.schema.produtos AS
SELECT * FROM origem.produtos;
```

Regras da documentação:
- Para trocar o conteúdo inteiro, use sempre `CREATE OR REPLACE TABLE`.
- Evite `DROP TABLE` seguido de `CREATE TABLE` com o mesmo nome em produção. Com leituras ou escritas simultâneas, isso pode gerar erro, registros perdidos ou resultado corrompido.
- `INSERT OVERWRITE` substitui só os dados e mantém o schema. Se o schema muda junto, use `CREATE OR REPLACE TABLE`.

---

## 2. Incremental

O Databricks não tem um recurso único chamado "incremental". O caminho depende da origem:

| Origem | Caminho recomendado |
|--------|---------------------|
| Arquivos novos em storage na nuvem | Auto Loader (seção 5) |
| Tabela Delta | Structured Streaming com `trigger(availableNow=True)` |
| Consulta SQL sobre tabelas Delta | Materialized view com refresh incremental (exige compute serverless) |
| Banco externo com `updated_at` | Marca d'água manual (abaixo) |

**Marca d'água manual.** Guarde o último valor lido em uma tabela de controle e só avance depois do sucesso da carga.

```sql
-- Tabela de controle: ctrl.watermarks (tabela STRING, ultimo_valor TIMESTAMP)

-- 1) Extrai o lote para uma tabela materializada, com janela de segurança de 2 horas
--    para não perder registros atrasados.
--    Não use TEMP VIEW aqui: a view é lazy e seria reavaliada no passo 3,
--    podendo enxergar linhas novas que o MERGE não processou.
CREATE OR REPLACE TABLE stg.pedidos_lote AS
SELECT *
FROM origem.pedidos
WHERE updated_at > (
  SELECT ultimo_valor - INTERVAL 2 HOURS
  FROM ctrl.watermarks
  WHERE tabela = 'pedidos'
);

-- 2) Aplica no destino com MERGE (idempotente, ver seção 4), lendo de stg.pedidos_lote

-- 3) Só depois do sucesso, avança a marca d'água usando o MESMO lote materializado.
--    O WHERE evita gravar NULL quando o lote vier vazio (a marca d'água fica como está).
UPDATE ctrl.watermarks
SET ultimo_valor = (SELECT MAX(updated_at) FROM stg.pedidos_lote)
WHERE tabela = 'pedidos'
  AND (SELECT MAX(updated_at) FROM stg.pedidos_lote) IS NOT NULL;
```

Este padrão não enxerga deletes na origem.

**Stream sobre uma tabela Delta, como lote incremental.** O `AvailableNow` processa tudo que chegou desde a última execução e para.

```python
(spark.readStream
   .table("catalogo.schema.pedidos")
   .writeStream
   .option("checkpointLocation", "/Volumes/catalogo/schema/vol/_checkpoints/pedidos")
   .trigger(availableNow=True)
   .toTable("catalogo.schema.pedidos_silver"))
```

Regras da documentação para Delta como origem de stream:
- A origem só pode receber appends. Se alguém rodar `UPDATE`, `DELETE`, `MERGE INTO` ou overwrite nela, o stream falha. As saídas são `skipChangeCommits` (ignora essas mudanças), o Change Data Feed (propaga as mudanças) ou materialized views.
- O stream precisa rodar pelo menos uma vez dentro da retenção da origem. Por padrão são 7 dias para arquivos removidos pelo `VACUUM` e 30 dias para o log de transações. Passou disso, falha e exige full refresh.

---

## 3. CDC

A palavra CDC aparece em três lugares diferentes no Databricks.

**a) Change Data Feed (CDF) do Delta.** Registra mudanças linha a linha dentro de uma tabela Delta. Existem dois modos:
- **CDF automático:** Databricks Runtime 19 ou superior, tabelas no Unity Catalog (gerenciada Delta com row tracking, ou externa Delta com row tracking). Não exige configurar tabela por tabela.
- **CDF legado:** exige `delta.enableChangeDataFeed = true` em cada tabela. Os dois não podem ser usados juntos.

```sql
-- CDF legado: ligar na tabela
ALTER TABLE catalogo.schema.clientes
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Ler mudanças entre versões (mesma API nos dois modos)
SELECT * FROM table_changes('catalogo.schema.clientes', 12, 15);
-- Colunas extras: _change_type (insert, update_preimage, update_postimage, delete),
-- _commit_version, _commit_timestamp

-- Migrar do legado para o automático
ALTER TABLE catalogo.schema.clientes UNSET TBLPROPERTIES ('delta.enableChangeDataFeed');
```

O CDF não é um registro permanente: as versões antigas saem quando o log é limpo. Para histórico permanente, grave as mudanças de forma incremental em outra tabela.

**b) Captura em banco externo.** Quem lê o log do banco é uma ferramenta de CDC (a documentação cita Debezium, Kafka e CDC baseado em log). Os eventos chegam ao armazenamento na nuvem e o `AUTO CDC` aplica. O desenho oficial usa um fluxo `once` para a carga inicial e depois um fluxo contínuo para as mudanças.

**c) AUTO CDC nos pipelines Lakeflow.** Substitui o `APPLY CHANGES` (que continua disponível). Exige pipeline serverless ou edições Pro ou Advanced.

```sql
-- SCD Tipo 1: só o estado atual
CREATE OR REFRESH STREAMING TABLE clientes_atual;

CREATE FLOW aplica_cdc AS AUTO CDC INTO
  clientes_atual
FROM
  stream(catalogo.schema.clientes_eventos)
KEYS
  (id_cliente)
APPLY AS DELETE WHEN
  operacao = "DELETE"
SEQUENCE BY
  sequencia
COLUMNS * EXCEPT
  (operacao, sequencia)
STORED AS
  SCD TYPE 1;
```

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.view
def clientes_eventos():
    return spark.readStream.table("catalogo.schema.clientes_eventos")

dp.create_streaming_table("clientes_atual")

dp.create_auto_cdc_flow(
    target="clientes_atual",
    source="clientes_eventos",
    keys=["id_cliente"],
    sequence_by=col("sequencia"),
    apply_as_deletes=expr("operacao = 'DELETE'"),
    except_column_list=["operacao", "sequencia"],
    stored_as_scd_type=1
)
```

A coluna de sequência precisa ser ordenável e não aceita nulos. Para desempatar, use `SEQUENCE BY STRUCT(coluna_a, coluna_b)`. Quando a origem não tem CDC e só entrega fotos completas, existe o `AUTO CDC FROM SNAPSHOT`, que compara snapshots consecutivos. A documentação traz informações diferentes, em páginas diferentes, sobre o suporte desse recurso em SQL: confirme na versão do usuário.

---

## 4. Upsert / Merge

Regras da documentação:
- Só uma linha da origem pode casar com uma mesma linha do destino. Deduplique a origem por chave antes do merge. Caso contrário o merge pode falhar, porque é ambíguo qual linha usar.
- `UPDATE SET *` e `INSERT *` exigem que a origem tenha todas as colunas do destino. Se faltar alguma, a consulta dá erro. Colunas a mais na origem são ignoradas, a menos que a evolução automática de schema esteja ligada: nesse caso elas são adicionadas ao destino. Para não depender disso, liste as colunas explicitamente.
- `WHEN NOT MATCHED BY SOURCE` apaga ou atualiza linhas do destino que não aparecem na origem (Runtime 12.2 LTS ou superior). Use uma condição nessa cláusula para não reescrever a tabela inteira.

```sql
MERGE INTO catalogo.schema.clientes AS t
USING (
  SELECT customer_id, plano, updated_at, op
  FROM catalogo.schema.clientes_stg
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET t.plano = s.plano, t.updated_at = s.updated_at
WHEN NOT MATCHED AND s.op <> 'D' THEN
  INSERT (customer_id, plano, updated_at) VALUES (s.customer_id, s.plano, s.updated_at);
```

A coluna `op` só serve para decidir o `DELETE` e não é copiada para o destino, porque as atribuições são explícitas.

**Deduplicação com merge só de insert** (logs que podem chegar repetidos). Os dados novos precisam estar deduplicados entre si antes:

```sql
MERGE INTO logs
USING novos_logs_deduplicados AS n
ON logs.id_unico = n.id_unico
WHEN NOT MATCHED THEN INSERT *;
```

**Merge dentro de um stream (`foreachBatch`)** está na seção 9. Para feeds de CDC com registros fora de ordem, use `AUTO CDC` (seção 3) em vez de montar o merge à mão.

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
    .toTable("catalogo.schema.eventos_bronze"))
```

Regras da documentação:
- O Auto Loader guarda o progresso no checkpoint e dá exatamente-uma-vez ao gravar no Delta. Ele não garante a ordem em que descobre os arquivos: use o horário do evento, não a ordem de chegada.
- A documentação recomenda o Auto Loader dentro de pipelines Lakeflow para ingestão incremental. Nesse caso o pipeline gerencia schema e checkpoint automaticamente.
- No Bronze, mantenha o dado cru. A documentação recomenda guardar a maioria dos campos como string, `VARIANT` ou binário, para proteger contra mudanças de schema, e adicionar colunas de rastreio (por exemplo `_metadata.file_name`). Inclua uma chave de evento para deduplicar depois.
- `delta.appendOnly = true` faz a tabela recusar exclusão de registros e alteração de valores. Isso inclui pedidos de exclusão de dados pessoais (LGPD): planeje antes de ligar.

---

## 6. Snapshot

```sql
-- Snapshot diário idempotente: rodar duas vezes no mesmo dia substitui só aquela data
INSERT INTO catalogo.schema.estoque_snapshot
REPLACE WHERE data_snapshot = DATE'2026-10-05'
SELECT *, DATE'2026-10-05' AS data_snapshot
FROM catalogo.schema.estoque;
```

Consulta "como estava em 31/01?":

```sql
SELECT * FROM catalogo.schema.estoque_snapshot WHERE data_snapshot = DATE'2026-01-31';
```

O time travel do Delta não substitui snapshot de negócio. A documentação diz para não usar o histórico da tabela como backup de longo prazo e para contar só com os últimos 7 dias, a menos que você aumente as retenções. Padrões: `deletedFileRetentionDuration` de 7 dias (o `VACUUM` usa) e `logRetentionDuration` de 30 dias. No Runtime 18.0 ou superior (12.2 ou superior em tabelas gerenciadas do Unity Catalog), uma consulta de time travel mais antiga que `deletedFileRetentionDuration` é bloqueada.

Quando a origem só entrega fotos completas e você quer o histórico de mudanças, o `AUTO CDC FROM SNAPSHOT` (pipelines Lakeflow) compara snapshots consecutivos, gera um feed de mudanças sintético e aplica SCD Tipo 1 ou 2. Os snapshots são processados em ordem crescente de versão, e um snapshot fora de ordem é ignorado. Mudanças ocorridas entre duas fotos não aparecem.

---

## 7. SCD Type 2

O caminho recomendado é o `AUTO CDC` dos pipelines Lakeflow, que cuida de ordem, duplicatas e fechamento das linhas. A documentação nota que montar isso à mão com `MERGE INTO` exige tabelas de staging, funções de janela e suposições de ordenação difíceis de manter.

```sql
CREATE OR REFRESH STREAMING TABLE clientes_historico;

CREATE FLOW aplica_cdc_scd2 AS AUTO CDC INTO
  clientes_historico
FROM
  stream(catalogo.schema.clientes_eventos)
KEYS
  (id_cliente)
APPLY AS DELETE WHEN
  operacao = "DELETE"
SEQUENCE BY
  sequencia
COLUMNS * EXCEPT
  (operacao, sequencia)
STORED AS
  SCD TYPE 2;
```

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.view
def clientes_eventos():
    return spark.readStream.table("catalogo.schema.clientes_eventos")

dp.create_streaming_table("clientes_historico")

dp.create_auto_cdc_flow(
    target="clientes_historico",
    source="clientes_eventos",
    keys=["id_cliente"],
    sequence_by=col("sequencia"),
    apply_as_deletes=expr("operacao = 'DELETE'"),
    except_column_list=["operacao", "sequencia"],
    stored_as_scd_type="2"
)
```

Pontos da documentação:
- O resultado traz as colunas `__START_AT` e `__END_AT`, preenchidas com os valores da coluna de sequência. A versão ativa tem `__END_AT` nulo. Se você quer fazer join por data, use uma coluna de data ou timestamp como sequência.
- Por padrão, qualquer mudança de coluna cria uma nova versão. Para acompanhar só algumas colunas, use `TRACK HISTORY ON * EXCEPT (cidade)` em SQL ou `track_history_except_column_list=["cidade"]` em Python. Mudanças nas outras colunas atualizam a versão atual no lugar.

Join de um fato com a dimensão pegando a versão válida na data do fato (a sequência precisa ser data ou timestamp):

```sql
SELECT f.*, d.cidade
FROM catalogo.schema.fato_pedidos f
JOIN catalogo.schema.clientes_historico d
  ON f.id_cliente = d.id_cliente
 AND f.data_pedido >= d.__START_AT
 AND (f.data_pedido < d.__END_AT OR d.__END_AT IS NULL);
```

---

## 8. Partition-based

**Organização física.** A documentação recomenda liquid clustering para todas as tabelas novas:

```sql
CREATE TABLE catalogo.schema.transacoes (
  id BIGINT, data_transacao DATE, valor DECIMAL(18,2)
)
CLUSTER BY (data_transacao);
```

- Até quatro chaves de clustering. As chaves podem mudar depois sem reescrever os dados.
- Clustering não é compatível com particionamento nem com `ZORDER` na mesma tabela.
- Em tabelas gerenciadas do Unity Catalog, `CLUSTER BY AUTO` deixa o Databricks escolher as chaves pelo histórico de consultas.
- Tabelas sem partição já são organizadas automaticamente por tempo de ingestão.
- Regras de tamanho: menos de 1 TB, não particione. De 1 TB a 100 TB, use liquid clustering em vez de particionamento. A partir de 100 TB o particionamento pode ajudar, mas a documentação manda tentar o liquid clustering primeiro e medir. Cada partição deve ter pelo menos 1 GB. Partição funciona em colunas de cardinalidade baixa ou conhecida (data, localização), não em timestamp ou ID de cliente.
- Para tabelas com muitos inserts, agende `OPTIMIZE` a cada uma ou duas horas, ou deixe a otimização preditiva fazer isso nas tabelas gerenciadas.

**Substituir só a fatia.**

```sql
-- REPLACE WHERE (SQL: Runtime 12.2 LTS ou superior; Python e Scala: 9.1 LTS ou superior)
INSERT INTO TABLE catalogo.schema.transacoes
REPLACE WHERE data_transacao = DATE'2026-08-21'
SELECT * FROM catalogo.schema.transacoes_stg
WHERE data_transacao = DATE'2026-08-21';

-- REPLACE USING (SQL: Runtime 16.3 ou superior): substitui as linhas cujas colunas casam
INSERT INTO TABLE catalogo.schema.transacoes
REPLACE USING (id)
SELECT * FROM catalogo.schema.transacoes_stg;
```

Em Python: `df.write.mode("overwrite").option("replaceWhere", "data_transacao = '2026-08-21'").saveAsTable("catalogo.schema.transacoes")`.

Regras da documentação:
- `REPLACE WHERE` é atômico e não exige que a tabela seja particionada pelas colunas do predicado. Se a origem trouxer linhas fora do predicado, a operação falha por padrão.
- Com origem vazia, `REPLACE USING` e `REPLACE ON` não apagam nada, mas `REPLACE WHERE` pode apagar a fatia. Valide a contagem antes.
- O `partitionOverwriteMode` dinâmico é legado e não é recomendado para cargas novas. Em SQL, só funciona em compute clássico.
- Se uma fatia foi sobrescrita por engano, `RESTORE` desfaz.

---

## 9. Micro-batch

Triggers do Structured Streaming:

| Trigger | Uso |
|---------|-----|
| (não definido) | Equivale a `processingTime` com intervalo 0. Olha por dados a cada poucos milissegundos e pode gerar muitas chamadas de API ao armazenamento. A documentação recomenda sempre definir um trigger |
| `trigger(processingTime='5 minutes')` | Micro-batch em intervalo fixo, equilibrando custo e desempenho |
| `trigger(availableNow=True)` | Lote incremental agendado: processa o que chegou e para |
| `trigger(realTime='5 minutes')` | Modo de tempo real, para latência abaixo de 1 segundo |

```python
def aplica_lote(lote_df, batch_id):
    lote_df.createOrReplaceTempView("lote")
    lote_df.sparkSession.sql("""
        MERGE INTO catalogo.schema.clientes AS t
        USING (
          SELECT * FROM lote
          QUALIFY ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY seq DESC) = 1
        ) AS s
        ON t.id_cliente = s.id_cliente
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

(spark.readStream.table("catalogo.schema.clientes_bronze")
   .writeStream
   .foreachBatch(aplica_lote)
   .option("checkpointLocation", "/Volumes/catalogo/schema/vol/_checkpoints/clientes")
   .trigger(processingTime="5 minutes")   # em serverless, use availableNow=True
   .start())
```

Regras da documentação:
- O `foreachBatch` dá garantia de pelo menos uma vez. O `MERGE` dentro dele precisa ser idempotente, porque uma reinicialização pode reaplicar o mesmo lote.
- Rode streams de produção como Lakeflow Jobs em compute de jobs, nunca em compute de uso geral. Agende o Job em modo `Continuous`, que reinicia a execução quando ela falha, e não ligue autoscaling nesses jobs. Para novos pipelines de streaming, a documentação recomenda pipelines Lakeflow.
- Em compute serverless, só `Trigger.AvailableNow` e `Trigger.Once` funcionam, e a recomendação é `AvailableNow`.
- Para latência operacional abaixo de 1 segundo, use o modo de tempo real. A documentação diz para não usar `AvailableNow`, `Once` ou `Continuous` em cargas operacionais.
- Lotes pequenos geram muitos arquivos pequenos: planeje `OPTIMIZE` ou deixe a otimização preditiva cuidar disso.

---

## 10. Backfill e Replay

**Em pipelines Lakeflow:** um fluxo de append com a opção `ONCE`.

```sql
CREATE FLOW pedidos_bronze_backfill_2025
AS INSERT INTO ONCE
  pedidos_bronze BY NAME
SELECT * FROM read_files(
  "/Volumes/catalogo/schema/vol/pedidos/year=2025/*/*",
  format => "json",
  inferColumnTypes => true
);
```

Regras da documentação:
- O fluxo `ONCE` roda uma única vez, fica no grafo do pipeline em repouso e roda de novo se houver full refresh.
- Anexe o histórico à tabela Bronze: Silver e Gold pegam o dado novo dali.
- O pipeline precisa tolerar duplicatas, caso o mesmo dado seja anexado mais de uma vez, e o schema do histórico precisa ser compatível com o atual.
- Full refresh de uma streaming table trunca a tabela, apaga os checkpoints e reprocessa tudo desde o começo. Só funciona se a origem retém o histórico, é caro, e as tabelas dependentes falham até serem atualizadas também (a menos que usem `skipChangeCommits`). Não é recomendado em alto volume.
- Rewind e replay (Beta) restaura um pipeline Lakeflow a um ponto anterior e reprocessa só o dado afetado. Os pontos de rewind são gerados cerca de uma vez por hora e ficam guardados por 7 dias.

**Em jobs em lote:** parametrize o job por intervalo de datas. O backfill passa a ser o mesmo código com parâmetros diferentes. Parametrizar por intervalo é prática de engenharia, não um recurso nomeado do Databricks.

```python
from datetime import date, timedelta

def reprocessa_dia(dia: date):
    d = dia.isoformat()
    (spark.table("catalogo.schema.pedidos_bronze")        # dado bruto, imutável
        .where(f"data_pedido = '{d}'")
        .selectExpr("data_pedido", "pedido", "valor_bruto * (1 - desconto) AS receita")
        .write.mode("overwrite")
        .option("replaceWhere", f"data_pedido = '{d}'")
        .saveAsTable("catalogo.schema.pedidos_silver"))

def roda(data_inicio: date, data_fim: date):
    dia = data_inicio
    while dia <= data_fim:
        reprocessa_dia(dia)
        dia += timedelta(days=1)

# Carga normal:          roda(hoje, hoje)
# Backfill de jan a mar: roda(date(2026, 1, 1), date(2026, 3, 31))
```

Checklist antes de disparar um backfill:
1. A transformação é idempotente (`REPLACE WHERE` ou merge, nunca append cego).
2. O dado bruto do período existe e não foi alterado.
3. As tabelas dependentes (gold, relatórios) serão reprocessadas na ordem.
4. Não há carga normal escrevendo nas mesmas fatias ao mesmo tempo.
5. Há uma validação pronta (contagens e totais antes e depois).
6. Com `REPLACE WHERE`, uma fatia de origem vazia pode apagar a fatia de destino: valide a contagem.
