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
# MAGIC -- Apaga a tabelas se existirem
# MAGIC DROP TABLE IF EXISTS tb_log_eventos;
# MAGIC DROP TABLE IF EXISTS tb_funcionarios;
# MAGIC 
# MAGIC -- Cria a tabela 'tb_log_eventos'
# MAGIC CREATE TABLE IF NOT EXISTS tb_log_eventos(
# MAGIC     Matricula INT, 
# MAGIC     Nome VARCHAR(200),
# MAGIC     Filial VARCHAR(100),
# MAGIC     Departamento VARCHAR(150),
# MAGIC     Cargo VARCHAR(150),
# MAGIC     DataAdmissao DATE,
# MAGIC     DataDesligamento DATE,
# MAGIC     Operacao STRING,
# MAGIC     DataLog TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Insere dados de exemplo na 'tb_log_eventos'
# MAGIC INSERT INTO tb_log_eventos(Matricula, Nome, Filial, Departamento, Cargo, DataAdmissao, DataDesligamento, Operacao, DataLog) VALUES 
# MAGIC (98, 'FLAVIO FAGUNDES', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA III', '2021-12-07', NULL, 'ADMISSAO', '2021-12-07 08:40:23'),
# MAGIC (100, 'MARIA PEREIRA', 'JUNDIAI', 'PRODUCAO', 'AJUDANTE DE PRODUCAO', '2022-01-17', NULL, 'ADMISSAO', '2022-01-17 09:10:17'),
# MAGIC (143, 'JOSE DA SILVA', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA III', '2022-02-05', NULL, 'ADMISSAO', '2022-02-05 13:00:00'),
# MAGIC (100, 'MARIA PEREIRA', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA I', '2022-01-17', NULL, 'ALTERACAO', '2023-02-20 15:00:00'),
# MAGIC (98, 'FLAVIO FAGUNDES', 'CAMPINAS', 'PRODUCAO', 'OPERADOR DE MAQUINA III', '2021-12-07', NULL, 'ALTERACAO', '2023-03-01 14:00:00'),
# MAGIC (143, 'JOSE DA SILVA', 'JUNDIAI', 'MANUTENCAO', 'ELETRICISTA DE MANUTENCAO PL', '2022-02-05', NULL, 'ALTERACAO', '2023-04-07 15:00:00'),
# MAGIC (202, 'MATEUS SOUZA', 'PIRACICABA', 'PRODUCAO', 'OPERADOR DE MAQUINA I', '2023-04-29', NULL, 'ADMISSAO', '2023-04-29 10:00:00'),
# MAGIC (298, 'MARCOS TEIXEIRA', 'CAMPINAS', 'QUALIDADE', 'ANALISTA DE QUALIDADE JR', '2023-07-14', NULL, 'ADMISSAO', '2023-07-14 15:00:00'),
# MAGIC (100, 'MARIA PEREIRA', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA II', '2022-01-17', NULL, 'ALTERACAO', '2024-03-05 08:00:00'),
# MAGIC (202, 'MATEUS SOUZA', 'CAMPINAS', 'PRODUCAO', 'OPERADOR DE MAQUINA II', '2023-04-29', NULL, 'ALTERACAO', '2024-09-19 11:00:00'),
# MAGIC (100, 'MARIA PEREIRA', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA III', '2022-01-17', NULL, 'ALTERACAO', '2024-10-10 09:00:00'),
# MAGIC (98, 'FLAVIO FAGUNDES', 'CAMPINAS', 'PRODUCAO', 'SUPERVISOR DE PRODUCAO', '2021-12-07', NULL, 'ALTERACAO', '2024-11-05 10:00:00'),
# MAGIC (298, 'MARCOS TEIXEIRA', 'PIRACICABA', 'QUALIDADE', 'ANALISTA DE QUALIDADE JR', '2023-07-14', '2024-11-10', 'DESLIGAMENTO', '2024-11-10 08:00:00'),
# MAGIC (100, 'MARIA PEREIRA', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA III', '2022-01-17', '2024-12-13', 'DESLIGAMENTO', '2024-12-13 13:00:00'),
# MAGIC (143, 'JOSE DA SILVA', 'JUNDIAI', 'MANUTENCAO', 'ELETRICISTA DE MANUTENCAO SR', '2022-02-05', NULL, 'ALTERACAO', '2024-12-15 09:00:00');


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Criação da tabela 'tb_funcionario' a partir do 'tb_log_eventos' 
# MAGIC 
# MAGIC -- Cria ou subtitui a tabela 'tb_funcionarios'
# MAGIC CREATE OR REPLACE TABLE tb_funcionarios USING DELTA AS 
# MAGIC 
# MAGIC -- CTE com o log apenas das linhas onde 'Operacao' não é 'DESLIGAMENTO'
# MAGIC WITH CTE_Log AS (
# MAGIC     SELECT 
# MAGIC         Matricula,
# MAGIC         Nome, 
# MAGIC         Filial,
# MAGIC         Departamento,
# MAGIC         Cargo,
# MAGIC         DataAdmissao,
# MAGIC         DataDesligamento,
# MAGIC         DataLog,
# MAGIC         DataLog AS DataVigenciaInicial,
# MAGIC         
# MAGIC         -- Window function para trazer o próximo 'DataLog' por 'Matricula'
# MAGIC         LEAD(DataLog) OVER (PARTITION BY Matricula ORDER BY DataLog) AS ProximoDataLog,
# MAGIC         
# MAGIC         -- Window function para criar a coluna 'MatriculaSk' a partir da ordem do 'DataLog'
# MAGIC         ROW_NUMBER() OVER (ORDER BY DataLog) AS MatriculaSk
# MAGIC     
# MAGIC     FROM tb_log_eventos
# MAGIC     WHERE Operacao <> 'DESLIGAMENTO'
# MAGIC )
# MAGIC 
# MAGIC -- Seleciona a partir da 'CTE_Log'
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
# MAGIC     END AS EstaAtivo
# MAGIC 
# MAGIC FROM CTE_Log
# MAGIC ORDER BY DataLog;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Update das operações de desligamento
# MAGIC 
# MAGIC -- Cria CTE com as operações de desligamento
# MAGIC WITH CTE_Desligamentos AS (
# MAGIC     SELECT 
# MAGIC         Matricula,
# MAGIC         Nome, 
# MAGIC         Filial,
# MAGIC         Departamento,
# MAGIC         Cargo,
# MAGIC         DataAdmissao,
# MAGIC         DataDesligamento,
# MAGIC         DataLog,
# MAGIC         CAST(DataLog AS DATE) AS DataVigenciaInicial
# MAGIC     FROM tb_log_eventos
# MAGIC     WHERE Operacao = 'DESLIGAMENTO'
# MAGIC )
# MAGIC 
# MAGIC -- Realiza o merge na tabela 'tb_funcionarios'
# MAGIC MERGE INTO tb_funcionarios AS f
# MAGIC 
# MAGIC -- a partir da 'CTE_Desligamentos'
# MAGIC USING (SELECT * FROM CTE_Desligamentos) AS d  
# MAGIC 
# MAGIC -- nas linhas onde as matriculas correspondem e estão ativos
# MAGIC ON f.Matricula = d.Matricula AND f.EstaAtivo = 1
# MAGIC 
# MAGIC -- Atualizando os registros que já existem
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC 
# MAGIC         -- As colunas 'DataDesligamento' e 'DataVigenciaFinal' em tb_funcionarios
# MAGIC         -- recebem o dia anterior da 'DataVigenciaInicial' da 'CTE_Desligamentos'
# MAGIC         f.DataDesligamento = CAST(DATEADD(DAY, -1, d.DataVigenciaInicial) AS DATE),
# MAGIC         f.DataVigenciaFinal = CAST(DATEADD(DAY, -1, d.DataVigenciaInicial) AS DATE),
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
# MAGIC -- Exibe os resultados carregados em 'tb_funcionarios'
# MAGIC SELECT * FROM tb_funcionarios
# MAGIC ORDER BY DataLog;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
