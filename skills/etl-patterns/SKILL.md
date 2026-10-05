---
name: etl-patterns
description: Catálogo e guia de decisão de padrões de ETL/ELT em produção (Full Load, Incremental, CDC, Upsert/Merge, Append-only, Snapshot, SCD, Partition-based, Micro-batch, Backfill/Replay), com exemplos em Databricks e Delta Lake. Use sempre que o usuário perguntar o que é um desses padrões, comparar padrões, escolher ou combinar a arquitetura de carga de uma tabela ou pipeline, decidir como manter histórico, tratar updates e deletes da origem, reduzir custo de processar tabelas grandes, definir a frequência de atualização ou reprocessar dados antigos, mesmo que o usuário não cite o nome do padrão.
---

# Padrões de ETL em produção

Esta skill tem duas funções:

1. **Explicar** cada padrão de carga de dados (catálogo).
2. **Escolher e combinar** padrões para um problema real (guia de decisão).

A segunda é a mais valiosa. O objetivo é raciocinar a partir das características do problema (volume, taxa de mudança, histórico, latência, confiabilidade) e chegar a uma arquitetura combinada, não recitar definições. Sistemas reais quase nunca usam um padrão só.

## Como usar

- **Pergunta conceitual** ("o que é CDC?", "SCD2 ou snapshot?"): leia a seção do padrão em `references/catalogo.md` e responda com definição, quando usar, quando evitar e armadilhas.
- **Problema de arquitetura** ("tenho uma tabela de X, preciso de Y"): siga o fluxo de decisão abaixo e responda no formato indicado.
- **Pedido de código ou implementação no Databricks**: leia `references/databricks.md`.

Explique o motivo de cada escolha. Quem pergunta costuma usar a resposta para aprender e para defender a decisão perante outras pessoas, então "faça X" sem "porque Y" vale pouco.

## Os 10 padrões em uma linha

| # | Padrão | Pergunta que responde |
|---|--------|-----------------------|
| 1 | Full Load | Posso simplesmente recarregar tudo? |
| 2 | Incremental Load | Como processar só o que mudou desde a última vez? |
| 3 | CDC (Change Data Capture) | Como capturar insert, update e delete direto do log do banco, com baixa latência? |
| 4 | Upsert / Merge | Como aplicar mudanças no destino sem duplicar linhas? |
| 5 | Append-only | Como guardar todo evento sem nunca alterar o que já existe? |
| 6 | Snapshot | Como estava o dataset inteiro em uma data específica? |
| 7 | SCD (Slowly Changing Dimensions) | Como manter a história dos atributos descritivos que mudam? |
| 8 | Partition-based | Como processar só a fatia relevante de um dataset gigante? |
| 9 | Micro-batch | Como reduzir a latência sem a complexidade do streaming por evento? |
| 10 | Backfill & Replay | Como corrigir o passado depois de uma falha ou mudança de lógica? |

## Confusões comuns (esclareça sempre que aparecerem)

- **Upsert é uma operação; SCD é uma estratégia de modelagem.** Upsert responde "como aplico a mudança sem duplicar". SCD responde "o que faço com o valor antigo". SCD Type 1 é na prática um upsert. SCD Type 2 é upsert mais fechamento da linha antiga e inserção de uma nova versão. Alguns materiais populares rotulam SCD como "Upsert/Merge"; trate como padrões distintos.
- **CDC é método de captura, não de carga.** Ele entrega os eventos de mudança. O destino ainda precisa aplicá-los (merge, SCD2) ou guardá-los (append-only).
- **Incremental por timestamp não enxerga DELETE.** Linha apagada na origem simplesmente não aparece. Quem precisa de deletes usa CDC, soft delete na origem ou reconciliação periódica.
- **Snapshot não é SCD2.** Snapshot guarda o dataset inteiro a cada data. SCD2 guarda uma linha só quando algo muda, com período de validade. SCD2 costuma custar menos armazenamento; snapshot é mais simples de consultar por data.
- **Micro-batch é uma frequência, não uma tecnologia.** A pergunta certa é "qual atraso o consumidor realmente tolera?". Muitas vezes a resposta é maior do que se imagina, e batch a cada hora resolve.
- **Partição é organização física, não estratégia de carga.** Ela torna os outros padrões mais baratos e o backfill viável.
- **Idempotência** significa que rodar a transformação duas vezes dá o mesmo resultado que rodar uma. Sem ela, backfill, retry e replay duplicam dados.

## Fluxo de decisão

### Passo 0: inspecionar o projeto (quando houver acesso ao workspace)

Se você está rodando dentro do Databricks ou tem acesso a ele, descubra os fatos antes de perguntar ao usuário. Pergunte só o que não dá para descobrir (por exemplo, a latência que o negócio tolera).

| O que descobrir | Como |
|-----------------|------|
| Tamanho, formato, partição ou clusterização | `DESCRIBE DETAIL catalogo.schema.tabela` (`numFiles`, `sizeInBytes`, `partitionColumns`, `clusteringColumns`) |
| Frequência de escrita e tipo de operação | `DESCRIBE HISTORY catalogo.schema.tabela` (operações WRITE, MERGE, DELETE, e o horário de cada uma) |
| Crescimento e taxa de mudança | `SELECT COUNT(*)` por dia de criação, e `numOutputRows` / `numTargetRowsUpdated` no histórico dos MERGEs |
| Colunas de rastreio de mudança | `DESCRIBE TABLE` procurando `updated_at`, `created_at`, IDs sequenciais, colunas `op`/`seq`/`_change_type` |
| Qualidade do `updated_at` | Verificar nulos e se ele realmente muda em updates: `SELECT COUNT(*) FILTER (WHERE updated_at IS NULL) ...` |
| Change Data Feed ativo | `SHOW TBLPROPERTIES tabela` (`delta.enableChangeDataFeed`) |
| Chave de negócio única | `SELECT chave, COUNT(*) ... GROUP BY chave HAVING COUNT(*) > 1` |
| Padrão já em uso | Ler os notebooks, jobs e pipelines do projeto e dizer o que já existe antes de propor mudança |
| Dado bruto retido | Existe tabela bronze ou arquivos de landing com o histórico? |

Registre o que foi medido e o que foi suposto. Em "Diagnóstico", separe **"medido"** de **"suposto"**: a recomendação depende dessa diferença.

### Passo 1: diagnosticar

Levante estas informações. Se o usuário não deu alguma e o Passo 0 não a revelou, assuma um valor razoável e declare a suposição na resposta. Faça no máximo uma pergunta de esclarecimento, e só quando a resposta mudaria a arquitetura.

1. **Volume e crescimento:** quantas linhas hoje e quanto cresce?
2. **Taxa de mudança:** que percentual muda por dia? Só inserts, ou também updates e deletes?
3. **A origem expõe mudanças?** Há log de transações acessível, coluna `updated_at` confiável, ID sequencial?
4. **Histórico:** o negócio precisa saber como era antes (por data, por versão), ou só o estado atual?
5. **Latência:** qual atraso máximo o consumidor tolera (segundos, minutos, horas, dia)?
6. **Reprocessamento:** é provável precisar corrigir períodos passados? Existe dado bruto guardado?
7. **Custo e equipe:** há capacidade de operar streaming/CDC, ou o time precisa de algo simples?

### Passo 2: mapear sinais para padrões

| Sinal no problema | Padrão indicado |
|-------------------|-----------------|
| Tabela pequena, origem sem rastreio de mudança, histórico dispensável | Full Load |
| Tabela grande, poucas linhas mudam, existe `updated_at` ou ID confiável | Incremental |
| Precisa capturar deletes, ou latência de minutos com origem OLTP pesada | CDC |
| Mudanças precisam ser aplicadas sobre uma tabela de estado atual | Upsert / Merge |
| Eventos, logs, transações, auditoria, necessidade de replay | Append-only |
| "Como estava em 31/01?" para o dataset inteiro | Snapshot |
| Histórico de atributos de cliente, produto, conta, departamento | SCD Type 2 |
| Dataset muito grande consultado e reprocessado por fatias (normalmente data) | Partition-based |
| Latência de segundos a poucos minutos sem streaming por evento | Micro-batch |
| Lógica pode mudar ou falhas podem ocorrer (sempre, em produção) | Backfill & Replay |

### Passo 2b: o recurso gerenciado do Databricks resolve?

Antes de montar o padrão à mão, verifique se um recurso gerenciado já cobre o caso. Costuma sair mais simples e mais barato de operar. Confirme nomes e limitações na documentação vigente, pois esses recursos evoluem rápido.

| Se o caso é... | Considere antes de construir à mão |
|----------------|-------------------------------------|
| Gold derivada de silver, agregações, joins, que precisa ficar atualizada | **Materialized view**: o Databricks tenta atualizar de forma incremental sozinho, sem você manter marca d'água ou merge |
| Ingestão contínua ou periódica de arquivos novos | **Auto Loader** ou **streaming table** (`availableNow` em Job agendado quando a latência permite) |
| Aplicar CDC e SCD Type 1 ou 2 | Fluxo de CDC dos pipelines declarativos (`AUTO CDC`, antes `APPLY CHANGES`), que trata ordenação e deleções |
| Trazer dados de bancos ou SaaS | **Lakeflow Connect** (conectores gerenciados) ou, para consulta sem copiar, **Lakehouse Federation** |
| Garantir qualidade no caminho | **Expectations** nos pipelines declarativos (descartar, falhar ou só registrar) |

Se o recurso gerenciado serve, recomende-o e mencione o padrão correspondente só para explicar o que ele faz por baixo. Se não serve (custo, limitação, requisito de controle), diga por quê e então siga para o Passo 3.

### Passo 3: montar a combinação por camada

Uma arquitetura de referência na lógica medalhão:

- **Bronze (captura):** CDC ou ingestão incremental gravando em **append-only**, dado bruto imutável, particionado ou clusterizado por data de ingestão. É o que viabiliza replay e backfill.
- **Silver (estado e história):** **Upsert/Merge** para o estado atual, ou **SCD Type 2** onde o histórico importa. Executado em **micro-batch** conforme a latência exigida.
- **Gold (consumo):** tabelas agregadas, **snapshots** periódicos para relatórios por data, **partition-based** para custo e velocidade.
- **Transversal:** transformações **idempotentes**, lógica versionada, job parametrizado por intervalo de datas para **backfill**.

Nem toda tabela precisa de todas as camadas. Remova o que o problema não exige. Mais padrões significam mais coisas para operar.

## Formato da resposta para problemas de arquitetura

Use esta estrutura, de forma enxuta:

1. **Diagnóstico:** as características do problema e as suposições feitas.
2. **Arquitetura recomendada:** a combinação de padrões, por camada ou etapa.
3. **Por que:** uma linha de justificativa por padrão escolhido, ligada a um sinal do diagnóstico.
4. **Riscos e armadilhas:** o que costuma dar errado nessa combinação.
5. **O que descartei e por quê:** padrões que parecem servir mas não servem aqui.
6. **Alternativa mais simples:** se existir uma versão com menos peças e perda aceitável, mencione.

## Exemplo trabalhado

**Pedido:** "Tabela com 8 bilhões de registros, 2% sofrem alteração por dia, preciso manter histórico e atualizar a cada 15 minutos. Qual arquitetura?"

**Diagnóstico:** 2% de 8 bi são cerca de 160 milhões de mudanças por dia, em média 1,7 milhão por janela de 15 minutos (96 janelas). Volume alto no total, mas pequeno por lote. Há updates, e provavelmente deletes. Histórico é requisito. Suposição: a origem é um banco transacional com log acessível.

**Arquitetura:**
- **CDC** na origem, porque captura update e delete sem varrer 8 bi de linhas e sustenta latência de minutos.
- **Append-only** no bronze com os eventos de mudança brutos, para auditoria, replay e backfill.
- **Micro-batch** a cada 15 minutos aplicando os eventos no silver.
- **SCD Type 2** no silver, porque o histórico é requisito (se bastasse o estado atual, seria Upsert simples).
- **Partition-based** (ou clusterização) por data para podar leitura e permitir reprocessar por fatia.
- **Backfill & Replay** a partir do bronze, com transformação idempotente.

**Riscos:** ordenação dos eventos (usar sequência do log, não horário de chegada), carga inicial das 8 bi de linhas antes de ligar o CDC, mudanças de schema na origem, retenção do log na origem se o consumidor ficar parado.

**Descartado:** Full Load e Snapshot diário (custo proibitivo em 8 bi), Incremental por `updated_at` (não captura deletes e sofre com atrasos de gravação).

**Alternativa mais simples:** se a origem não permitir CDC, Incremental por `updated_at` com janela de segurança e Merge, mais uma reconciliação semanal para detectar deletes.

## Anti-padrões a apontar quando aparecerem

- Full Load diário em tabela que só cresce.
- Incremental por `updated_at` sem plano para deletes e para registros atrasados.
- Merge sem deduplicar a origem por chave antes (duplicata na origem gera erro ou resultado errado).
- Atualização fora de ordem sobrescrevendo dado novo com dado antigo (falta condição por sequência ou `updated_at`).
- Partição por coluna de alta cardinalidade (milhões de arquivos pequenos).
- Guardar a marca d'água (watermark) antes de a carga terminar com sucesso.
- Pipeline sem dado bruto retido: qualquer erro de lógica vira perda definitiva.
- Escolher o padrão pela moda (streaming, CDC) sem perguntar a latência real que o negócio precisa.
