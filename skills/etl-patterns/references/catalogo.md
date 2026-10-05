# Catálogo dos 10 padrões de ETL

Cada padrão segue a mesma estrutura: o que é, como funciona, quando usar, quando evitar, armadilhas, combina com.

Conteúdo:
1. Full Load
2. Incremental Load
3. CDC
4. Upsert / Merge
5. Append-only
6. Snapshot
7. SCD (Slowly Changing Dimensions)
8. Partition-based ETL
9. Micro-batch ETL
10. Backfill & Replay

---

## 1. Full Load

**O que é:** extrai o dataset inteiro da origem e recarrega o destino a cada execução.

**Como funciona:** origem → extrai tudo → transforma todos os registros → substitui a tabela destino.

**Quando usar:**
- Dataset relativamente pequeno.
- Origem não expõe rastreio de mudança.
- Histórico de alterações não é necessário.
- Simplicidade do pipeline pesa mais que eficiência.

Exemplo: um pipeline diário baixa o catálogo completo de 50 mil produtos e substitui a tabela de ontem.

**Quando evitar:** volume cresce continuamente, a extração completa sobrecarrega a origem, é preciso histórico.

**Armadilhas:**
- O custo cresce com o tamanho do dataset, mesmo que quase nada tenha mudado.
- Se a substituição não for atômica, o consumidor pode ler a tabela vazia ou pela metade. Use overwrite atômico (no Delta, a operação já é transacional).
- Vantagem escondida: deletes na origem são refletidos naturalmente, pois a linha simplesmente não vem mais.

**Combina com:** Snapshot (guardar cada carga completa datada).

---

## 2. Incremental Load

**O que é:** processa apenas os registros adicionados ou modificados desde a última execução bem-sucedida.

**Como funciona:** consulta a última marca processada (timestamp ou ID) → extrai registros novos ou alterados → transforma → insere ou atualiza o destino → grava a nova marca.

**Campos de rastreio comuns:** `updated_at`, `created_at`, IDs auto-incrementais, números de sequência, batch IDs.

**Quando usar:**
- Tabelas de origem grandes.
- Apenas uma pequena porcentagem muda.
- Histórico de mudanças não é exigido.
- Exemplo: em vez de carregar 100 milhões de pedidos por dia, processar só os 200 mil alterados desde ontem.

**Quando evitar:** não existe coluna de mudança confiável, ou é preciso capturar deletes.

**Armadilhas:**
- **Não captura DELETE.** Linha apagada não aparece na consulta.
- ID auto-incremental só detecta inserts, não updates.
- `updated_at` que nem todo processo atualiza, ou que é preenchido com o horário de início da transação (e não do commit), faz registros "escaparem" da janela.
- Gravar a marca d'água antes do fim da carga causa perda de dados em caso de falha.
- Mitigação: reler uma janela de segurança (por exemplo, as últimas horas) e tornar a escrita idempotente (merge), além de uma reconciliação periódica por contagem ou hash.

**Combina com:** Upsert/Merge (quase sempre o passo final), Partition-based.

---

## 3. CDC (Change Data Capture)

**O que é:** captura as mudanças do banco diretamente a partir do log de transações, permitindo detectar inserts, updates e deletes com baixa latência.

**Como funciona:** banco transacional → log de transações → conector CDC → plataforma de streaming → transformações → warehouse ou lakehouse.

**Tecnologias comuns:** Debezium, Kafka, AWS DMS, Google Datastream, leitura direta do log do banco.

**No Databricks:** o `AUTO CDC` (pipelines Lakeflow) aplica o feed de mudanças e já resolve ordem, deletes e SCD Tipo 1 ou 2. Ele substitui o antigo `APPLY CHANGES`. O Change Data Feed do Delta é outra coisa: registra mudanças dentro de tabelas Delta (modo automático no Runtime 19 ou superior, ou modo legado por propriedade da tabela).

**Quando usar:**
- Aplicações que precisam de sincronização quase em tempo real sem consultar repetidamente grandes bancos operacionais.
- É necessário capturar deletes.
- A origem não pode suportar consultas pesadas de extração.

**Quando evitar:** não há acesso ao log da origem, o volume é pequeno e a latência de horas é aceitável (excesso de engenharia), ou o time não consegue operar a infraestrutura.

**Armadilhas:**
- **Ordem dos eventos:** use a sequência do log (LSN, SCN, offset), não o horário de chegada.
- **Carga inicial:** é preciso um snapshot inicial consistente antes de aplicar o log a partir do ponto correto.
- **Entrega ao menos uma vez:** eventos duplicados exigem aplicação idempotente.
- **Mudanças de schema (DDL)** na origem quebram o conector se não forem planejadas.
- **Retenção do log na origem:** se o consumidor ficar parado por mais tempo que a retenção, perde-se a posição e é preciso nova carga inicial.

**Combina com:** Append-only (guardar os eventos brutos), Upsert/Merge e SCD2 (aplicar no destino), Micro-batch.

---

## 4. Upsert / Merge

**O que é:** combina lógica de INSERT e UPDATE: registros recebidos criam novas linhas ou atualizam as existentes.

**Lógica:** registro chega → a chave primária existe? Não: INSERT. Sim: UPDATE.

**Exemplo:** cliente existente com `Customer_ID = 102` e `Plan = Basic`; chega registro com `Customer_ID = 102` e `Plan = Premium`. O pipeline atualiza o cliente existente em vez de criar duplicata.

**Quando usar:** dados mestres de clientes, catálogos de produtos, registros de contas, datasets operacionais de mudança lenta.

**Requisito essencial:** chaves primárias ou de negócio confiáveis.

**Armadilhas:**
- **Duplicata na origem:** deduplique por chave (mantendo o registro mais recente) antes do merge. Caso contrário o merge pode falhar, porque é ambíguo qual linha usar.
- **Colunas:** `UPDATE SET *` e `INSERT *` exigem que a origem tenha todas as colunas do destino. Colunas a mais na origem são ignoradas, a menos que a evolução automática de schema esteja ligada (aí são adicionadas ao destino). Na dúvida, liste as colunas explicitamente.
- **Sem histórico:** o valor antigo é sobrescrito. Se o histórico importa, use SCD Type 2.
- **Fora de ordem:** um registro antigo chegando depois pode sobrescrever um novo. Condicione o update a `updated_at` ou sequência maior.
- **Deletes:** não acontecem sozinhos. Trate com soft delete ou cláusula explícita de delete.
- **Custo:** merge em tabela grande lê muitos arquivos se a organização física (partição ou clusterização) não ajudar a podar.

**Combina com:** Incremental, CDC, Micro-batch.

---

## 5. Append-only

**O que é:** nunca modifica registros existentes. Cada novo evento é gravado como uma nova linha.

**Como funciona:** eventos da aplicação → pipeline de ingestão → append de novos eventos → armazenamento imutável. Registros existentes permanecem inalterados.

**Casos comuns:** logs de aplicação, eventos de clickstream, telemetria IoT, transações financeiras, trilhas de auditoria.

**Por que equipes usam:** preserva o histórico completo, permite replay e auditoria, simplifica a ingestão, funciona bem em sistemas distribuídos.

**Exemplo:** o clique em um site vira um evento separado em vez de atualizar o registro anterior de atividade do usuário.

**Armadilhas:**
- **Duplicatas por reenvio:** sem uma chave de evento, a deduplicação downstream fica impossível.
- **Crescimento ilimitado:** planeje particionamento e política de retenção.
- **Estado atual não é direto:** consultar "como está agora" exige pegar o último evento por chave (por exemplo, `row_number` decrescente) ou manter uma tabela derivada.
- **Correções são novos eventos** (compensação), não updates.
- **Privacidade (LGPD):** pedidos de exclusão conflitam com imutabilidade. Planeje desde o início como atender (por exemplo, pseudonimização ou rotina controlada de remoção).

**Combina com:** CDC (destino dos eventos brutos), Backfill & Replay, Partition-based. É a base do bronze.

---

## 6. Snapshot

**O que é:** captura o estado completo de um dataset em um ponto específico do tempo.

**Estrutura de exemplo:**

| Produto | Preço | Data do snapshot |
|---------|-------|------------------|
| Laptop | 900 | 1º jan |
| Laptop | 950 | 1º fev |
| Laptop | 920 | 1º mar |

**Pipeline:** estado da origem → extrai registros atuais → adiciona timestamp de snapshot → grava novo snapshot histórico.

**Quando usar:** histórico de inventário, saldos de contas, relatórios financeiros mensais, histórico de preços, headcount de funcionários.

**Pergunta que responde com facilidade:** "como o sistema estava nesta data?"

**Armadilhas:**
- **Armazenamento** cresce com tamanho do dataset × frequência dos snapshots.
- **Só enxerga os instantes das fotos.** Mudanças entre duas fotos se perdem.
- A **data do snapshot** deve fazer parte da chave da tabela.
- **Time travel do Delta não substitui snapshot:** a documentação diz para não usar o histórico da tabela como backup de longo prazo. Com as configurações padrão, os arquivos de dados são mantidos por 7 dias (o `VACUUM` remove os antigos) e o log por 30 dias. Para histórico de negócio, grave snapshots explícitos.
- Para tabelas grandes, considere SCD Type 2 ou snapshot apenas do que mudou.

**Combina com:** Full Load (cada carga completa vira um snapshot), Partition-based (particionar por data do snapshot).

---

## 7. SCD (Slowly Changing Dimensions)

**O que é:** gerencia mudanças em dados descritivos de negócio ao longo do tempo.

**Type 1:** sobrescreve o valor existente. Exemplo: cidade Londres → Berlim, sem registro do valor anterior.

**Type 2:** cria uma nova linha, mantendo o estado histórico disponível.

| Customer | City | Valid From | Current |
|----------|--------|------------|---------|
| 101 | London | 2024 | No |
| 101 | Berlin | 2026 | Yes |

Na prática, inclua também `valid_to` (ou use `null` / data muito distante para a linha vigente) e `is_current`.

**Type 3 (raro):** guarda o valor anterior em uma coluna extra. Só mantém uma versão de história.

**Usos comuns:** atributos de cliente, departamentos de funcionários, categorias de produtos, classificações de contas. Especialmente comum em data warehouses analíticos.

**No Databricks:** o `AUTO CDC` cobre os Tipos 1 e 2 (`STORED AS SCD TYPE 1` ou `2`). No Tipo 2 ele cria as colunas `__START_AT` e `__END_AT`, preenchidas com a coluna de sequência, e a versão ativa tem `__END_AT` nulo.

**Armadilhas:**
- **Escolha quais atributos disparam nova versão.** Rastrear todos gera explosão de linhas.
- **Joins com fatos** devem usar a versão válida na data do fato (join por intervalo de validade), não apenas a chave.
- **Mudanças fora de ordem ou retroativas** exigem tratamento: reabrir e recortar intervalos.
- Convencione o fim de validade da linha vigente (null ou 9999-12-31) e use sempre a mesma.
- Type 1 é essencialmente um upsert; Type 2 é upsert + fechar a linha antiga + inserir a nova.

**Combina com:** CDC (fornece os eventos de mudança), Micro-batch, Backfill.

---

## 8. Partition-based ETL

**O que é:** datasets grandes são divididos em partições lógicas, permitindo que o pipeline processe apenas as porções relevantes.

**Chaves de partição comuns:** data, hora, região, cliente, tipo de evento.

**Exemplo:** em vez de processar 5 TB de histórico de transações, processar apenas `transactions/date=2026-08-21`.

**Pipeline:** identificar partição necessária → extrair dados da partição → transformar → gravar ou substituir a partição.

**Benefícios:** processamento mais rápido, menor custo de computação, recuperação mais fácil, melhor paralelização, domínios de falha menores. Extremamente comum em data lakes e lakehouses.

**Armadilhas:**
- **Alta cardinalidade** (por exemplo, partição por cliente) gera milhões de arquivos pequenos e piora tudo.
- **Partições desbalanceadas** deixam uma tarefa lenta carregando quase todo o dado.
- **Dados atrasados** caem em partições antigas: reprocesse uma janela dos últimos N dias, não só a partição de hoje.
- A chave de partição deve coincidir com o filtro mais comum de consulta e de reprocessamento.
- **No Databricks, a documentação recomenda liquid clustering** para tabelas novas (`CLUSTER BY`). Menos de 1 TB: não particione. De 1 TB a 100 TB: liquid clustering em vez de particionamento. A partir de 100 TB o particionamento pode ajudar, mas teste o liquid clustering primeiro. Cada partição deve ter pelo menos 1 GB. Os números podem mudar: confirme na documentação vigente.

**Combina com:** Incremental, Append-only, Backfill (reprocessar partição por partição).

---

## 9. Micro-batch ETL

**O que é:** processa pequenos grupos de registros a cada poucos segundos ou minutos, ficando entre o batch tradicional e o streaming verdadeiro.

**Comparação:**
- Batch tradicional: processa a cada 24 horas.
- Micro-batch: processa a cada 1 a 5 minutos (ou intervalo escolhido).
- Streaming: processa continuamente.

**Fluxo:** eventos chegam → coleta um pequeno lote → transforma → carrega → repete.

**Melhor para:** dashboards operacionais, relatórios de atividade de usuário, monitoramento de fraude, analytics quase em tempo real.

**Vantagem:** latência menor que o batch, mais fácil de operar que streaming totalmente orientado a eventos.

**Armadilhas:**
- **Latência mínima = tamanho do lote.** Não é tempo real.
- **Muitos arquivos pequenos:** compacte periodicamente (por exemplo, `OPTIMIZE` no Delta).
- **Custo:** cluster ligado o dia todo versus disparos agendados que processam o acumulado e desligam. Compare antes de decidir.
- **Atraso em cascata:** se um lote demora mais que o intervalo, a fila cresce.
- **Exatamente-uma-vez** depende de checkpoint e destino idempotente.
- Defina o intervalo pela necessidade real do consumidor, não pelo mínimo tecnicamente possível.
- **No Databricks:** sem trigger definido, o stream olha por dados a cada poucos milissegundos e pode gerar custo de API ao armazenamento. Em compute serverless só `AvailableNow` e `Once` funcionam. Streams de produção devem rodar como Lakeflow Jobs em compute de jobs. Para latência abaixo de 1 segundo, existe o modo de tempo real.

**Combina com:** CDC, Upsert/Merge, SCD2.

---

## 10. Backfill & Replay

**O que é:** pipelines de produção eventualmente falham, schemas mudam ou a lógica de transformação melhora. Backfill reprocessa dados históricos para que períodos anteriores possam ser corrigidos.

**Exemplo:** um cálculo de receita estava errado de janeiro a março. Em vez de reconstruir tudo, reprocesse a partição de janeiro, depois fevereiro, depois março.

**Backfills confiáveis exigem:**
- Dados brutos imutáveis.
- Transformações idempotentes.
- Datasets particionados.
- Lógica de transformação versionada.
- Dependências do pipeline claras.

**Replay:** arquiteturas baseadas em eventos podem reexecutar eventos históricos pelo mesmo pipeline de processamento. Por isso reter dados brutos é tão valioso.

**Armadilhas:**
- **Sem idempotência**, o reprocessamento duplica dados.
- **Cascata:** reprocessar uma tabela exige reprocessar as que dependem dela, na ordem certa.
- **Sem versionar a lógica**, não dá para explicar por que o número antigo era diferente do novo.
- **Concorrência** entre backfill e carga normal na mesma partição causa conflito. Coordene ou isole.
- **Custo:** limite por intervalos de datas e valide antes de disparar o período inteiro.
- **Validação:** compare totais antes e depois (contagem, somas de controle) e documente o que mudou.

Dica de projeto: parametrize todo job de carga por intervalo de datas (`data_inicio`, `data_fim`) desde o primeiro dia. Assim o job normal e o backfill são o mesmo código.

**Combina com:** Append-only (fonte do reprocessamento), Partition-based, tudo o mais.
