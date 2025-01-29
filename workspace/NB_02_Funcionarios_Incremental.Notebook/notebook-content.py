# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "11540624-ed9f-41e1-9b0e-6bd60e915153",
# META       "default_lakehouse_name": "LK_01",
# META       "default_lakehouse_workspace_id": "cb84b88f-2b8c-480e-9c0b-2e37850e3d4b",
# META       "known_lakehouses": [
# META         {
# META           "id": "11540624-ed9f-41e1-9b0e-6bd60e915153"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Insere novos valores de exemplo
# MAGIC INSERT INTO tb_log_eventos (Matricula, Nome, Filial, Departamento, Cargo, DataAdmissao, DataDesligamento, Operacao, DataLog) VALUES 
# MAGIC (98, 'FLAVIO FAGUNDES', 'CAMPINAS', 'PRODUCAO', 'SUPERVISOR DE PRODUCAO', '2021-12-07', '2025-01-28', 'DESLIGAMENTO', '2025-01-28 10:00:00'),
# MAGIC (143, 'JOSE DA SILVA', 'JUNDIAI', 'MANUTENCAO', 'ELETRICISTA DE MANUTENCAO XP', '2022-02-05', NULL, 'ALTERACAO', '2025-01-29 09:00:00');


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Maiores 'DataLog' e 'MatriculaSk' na tabela 'tb_funcionarios'
# MAGIC DROP TABLE IF EXISTS staging_ultima_carga;
# MAGIC CREATE TABLE staging_ultima_carga AS
# MAGIC SELECT 
# MAGIC     MAX(DataLog) AS UltimaDataLog, 
# MAGIC     MAX(MatriculaSk) AS UltimaMatriculaSk 
# MAGIC FROM tb_funcionarios;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Tabela temporária com o log após o último 'DataLog' e quando 'Operacao' <> 'DESLIGAMENTO'
# MAGIC DROP TABLE IF EXISTS staging_logs;
# MAGIC CREATE TABLE staging_logs AS 
# MAGIC SELECT 
# MAGIC     Matricula,
# MAGIC     Nome, 
# MAGIC     Filial,
# MAGIC     Departamento,
# MAGIC     Cargo,
# MAGIC     DataAdmissao,
# MAGIC     DataDesligamento,
# MAGIC     DataLog,
# MAGIC     DataLog AS DataVigenciaInicial,
# MAGIC 
# MAGIC     -- Window function para trazer o próximo 'DataLog' por 'Matricula'
# MAGIC     LEAD(DataLog) OVER (PARTITION BY Matricula ORDER BY DataLog) AS ProximoDataLog,
# MAGIC     
# MAGIC     -- Window function para criar a coluna 'MatriculaSk' a partir da ordem do 'DataLog'
# MAGIC     -- partindo do maior já existente
# MAGIC     (SELECT COALESCE(UltimaMatriculaSk, 0) FROM staging_ultima_carga) + 
# MAGIC     ROW_NUMBER() OVER (ORDER BY DataLog) AS MatriculaSk  
# MAGIC 
# MAGIC FROM tb_log_eventos
# MAGIC WHERE DataLog > (SELECT UltimaDataLog FROM staging_ultima_carga)
# MAGIC     AND Operacao <> 'DESLIGAMENTO';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Tabela temporária contendo os novos eventos
# MAGIC DROP TABLE IF EXISTS staging_novos_eventos;
# MAGIC CREATE TABLE staging_novos_eventos AS
# MAGIC SELECT 
# MAGIC     MatriculaSk,  
# MAGIC     Matricula,
# MAGIC     Nome, 
# MAGIC     Filial,
# MAGIC     Departamento,
# MAGIC     Cargo,
# MAGIC     DataAdmissao,
# MAGIC     DataDesligamento,
# MAGIC     DataLog,
# MAGIC 
# MAGIC     -- 'DataVigenciaInicial' é a data de alteração
# MAGIC     CAST(DataVigenciaInicial AS DATE) AS DataVigenciaInicial,
# MAGIC 
# MAGIC     -- Coluna 'DataVigenciaFinal'
# MAGIC     -- Quando o 'ProximoDataLog' não estiver vazio, pega o
# MAGIC     -- 'ProximoDataLog' e subtrai um dia
# MAGIC     -- Caso estiver vazio então quer dizer que é o último
# MAGIC     -- Logo adiciona '9999-12-31'
# MAGIC     CASE 
# MAGIC         WHEN ProximoDataLog IS NOT NULL 
# MAGIC         THEN CAST(DATEADD(DAY, -1, ProximoDataLog) AS DATE)
# MAGIC         ELSE CAST('9999-12-31' AS DATE)
# MAGIC     END AS DataVigenciaFinal,
# MAGIC 
# MAGIC     -- 'EstaAtivo' traz 1 quando for o último evento
# MAGIC     CASE 
# MAGIC         WHEN ProximoDataLog IS NULL THEN 1 
# MAGIC         ELSE 0 
# MAGIC     END AS EstaAtivo,
# MAGIC     
# MAGIC     -- Criado o número da linha para capturar a menor 'DataLog' por Matricula
# MAGIC     ROW_NUMBER() OVER (PARTITION BY Matricula ORDER BY DataLog ASC) AS RN 
# MAGIC 
# MAGIC FROM staging_logs;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Tabela temporária com os desligamentos após a última carga
# MAGIC DROP TABLE IF EXISTS staging_desligamentos;
# MAGIC CREATE TABLE staging_desligamentos AS
# MAGIC SELECT 
# MAGIC     Matricula,
# MAGIC     Nome, 
# MAGIC     Filial,
# MAGIC     Departamento,
# MAGIC     Cargo,
# MAGIC     DataAdmissao,
# MAGIC     DataDesligamento,
# MAGIC     DataLog,
# MAGIC     CAST(DataLog AS DATE) AS DataVigenciaInicial
# MAGIC FROM tb_log_eventos
# MAGIC WHERE DataLog > (SELECT UltimaDataLog FROM staging_ultima_carga)
# MAGIC     AND Operacao = 'DESLIGAMENTO';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Atualiza a tabela 'tb_funcionarios'
# MAGIC MERGE INTO tb_funcionarios AS f
# MAGIC 
# MAGIC -- Filtra apenas a menor 'DataLog' de cada 'Matricula' de 'staging_novos_eventos'
# MAGIC USING (SELECT * FROM staging_novos_eventos WHERE RN = 1) AS n  
# MAGIC 
# MAGIC -- nas linhas onde as matriculas correspondem e estão ativas
# MAGIC ON f.Matricula = n.Matricula AND f.EstaAtivo = 1
# MAGIC 
# MAGIC -- Atualizando os registros que já existem
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC 
# MAGIC         -- Atribuindo a data anterior a 'DataVigenciaInicial' de 'staging_novos_eventos'
# MAGIC         -- em 'DataVigenciaFinal' da tabela 'tb_funcionarios'
# MAGIC         f.DataVigenciaFinal = CAST(DATEADD(DAY, -1, n.DataVigenciaInicial) AS DATE),
# MAGIC         
# MAGIC         -- E coloca 'EstaAtivo' como 0
# MAGIC         f.EstaAtivo = 0;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Insere na tabela 'tb_funcionarios' todas as novas linhas em 'staging_novos_eventos'
# MAGIC INSERT INTO tb_funcionarios(
# MAGIC     MatriculaSk, 
# MAGIC     Matricula, 
# MAGIC     Nome, 
# MAGIC     Filial, 
# MAGIC     Departamento, 
# MAGIC     Cargo, 
# MAGIC     DataAdmissao, 
# MAGIC     DataDesligamento, 
# MAGIC     DataLog, 
# MAGIC     DataVigenciaInicial, 
# MAGIC     DataVigenciaFinal, 
# MAGIC     EstaAtivo
# MAGIC )
# MAGIC SELECT 
# MAGIC     MatriculaSk, 
# MAGIC     Matricula, 
# MAGIC     Nome, 
# MAGIC     Filial, 
# MAGIC     Departamento, 
# MAGIC     Cargo, 
# MAGIC     DataAdmissao, 
# MAGIC     DataDesligamento, 
# MAGIC     DataLog, 
# MAGIC     DataVigenciaInicial, 
# MAGIC     DataVigenciaFinal, 
# MAGIC     EstaAtivo
# MAGIC FROM staging_novos_eventos;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Atualiza na tabela 'tb_funcionarios'
# MAGIC MERGE INTO tb_funcionarios AS f
# MAGIC 
# MAGIC -- Com os dados de 'staging_desligamentos'
# MAGIC USING (SELECT * FROM staging_desligamentos) AS d  
# MAGIC 
# MAGIC -- nas linhas onde as matriculas correspondem e estão ativas
# MAGIC ON f.Matricula = d.Matricula AND f.EstaAtivo = 1
# MAGIC 
# MAGIC -- Atualizando os registros que já existem
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC 
# MAGIC         -- As colunas 'DataDesligamento' e 'DataVigenciaFinal' em tb_funcionarios
# MAGIC         -- recebem o dia anterior da 'DataVigenciaInicial' da 'staging_desligamentos'
# MAGIC         f.DataDesligamento = CAST(DATEADD(DAY, -1, d.DataVigenciaInicial) AS DATE),
# MAGIC         f.DataVigenciaFinal = CAST(DATEADD(DAY, -1, d.DataVigenciaInicial) AS DATE),
# MAGIC 
# MAGIC         -- E coloca 'EstaAtivo' como 0
# MAGIC         f.EstaAtivo = 0;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Exclui as tabelas temporárias
# MAGIC DROP TABLE IF EXISTS staging_ultima_carga;
# MAGIC DROP TABLE IF EXISTS staging_logs;
# MAGIC DROP TABLE IF EXISTS staging_novos_eventos;
# MAGIC DROP TABLE IF EXISTS staging_desligamentos;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Exibe os resultados
# MAGIC SELECT * FROM tb_funcionarios
# MAGIC ORDER BY DataLog;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
