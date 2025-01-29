--Exclui as tabelas caso existam
DROP TABLE IF EXISTS dbo.admissoes;
DROP TABLE IF EXISTS dbo.alteracoes;
DROP TABLE IF EXISTS dbo.desligamentos;

---Cria as tabelas com o schema
CREATE TABLE dbo.admissoes (
      matricula INT
    , nome VARCHAR(200)
    , filial VARCHAR(100)
    , departamento VARCHAR(100)
    , cargo VARCHAR(100)
    , data_admissao DATE
);
CREATE TABLE dbo.alteracoes (
      matricula INT
    , nome VARCHAR(200)
    , filial VARCHAR(100)
    , departamento VARCHAR(100)
    , cargo VARCHAR(100)
    , data_alteracao DATE
);
CREATE TABLE dbo.desligamentos (
      matricula INT
    , nome VARCHAR(200)
    , filial VARCHAR(100)
    , departamento VARCHAR(100)
    , cargo VARCHAR(100)
    , data_desligamento DATE
);

-- Popula as tabelas com dados de exemplo
INSERT INTO dbo.admissoes(matricula, nome, filial, departamento, cargo, data_admissao)
VALUES 
  (98, 'FLAVIO FAGUNDES', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA III', '2021-12-07')
, (100, 'MARIA PEREIRA', 'JUNDIAI', 'PRODUCAO', 'AJUDANTE DE PRODUCAO', '2022-01-17')
, (143, 'JOSE DA SILVA', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA III', '2022-02-05')
, (202, 'MATEUS SOUZA', 'PIRACICABA', 'PRODUCAO', 'OPERADOR DE MAQUINA I', '2023-04-29')
, (298, 'MARCOS TEIXEIRA', 'CAMPINAS', 'QUALIDADE', 'ANALISTA DE QUALIDADE JR', '2023-07-14');

INSERT INTO dbo.alteracoes(matricula, nome, filial, departamento, cargo, data_alteracao)
VALUES 
      (100, 'MARIA PEREIRA', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA I', '2023-02-20')
    , (98, 'FLAVIO FAGUNDES', 'CAMPINAS', 'PRODUCAO', 'OPERADOR DE MAQUINA III', '2023-03-01')
    , (143, 'JOSE DA SILVA', 'JUNDIAI', 'MANUTENCAO', 'ELETRICISTA DE MANUTENCAO PL', '2023-04-07')
    , (100, 'MARIA PEREIRA', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA II', '2024-03-05')
    , (202, 'MATEUS SOUZA', 'CAMPINAS', 'PRODUCAO', 'OPERADOR DE MAQUINA II', '2024-09-19')
    , (100, 'MARIA PEREIRA', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA III', '2024-10-10')
    , (98, 'FLAVIO FAGUNDES', 'CAMPINAS', 'PRODUCAO', 'SUPERVISOR DE PRODUCAO', '2024-11-05')
    , (143, 'JOSE DA SILVA', 'JUNDIAI', 'MANUTENCAO', 'ELETRICISTA DE MANUTENCAO SR', '2024-12-15');

INSERT INTO dbo.desligamentos(matricula, nome, filial, departamento, cargo, data_desligamento)
VALUES 
    (298, 'MARCOS TEIXEIRA', 'PIRACICABA', 'QUALIDADE', 'ANALISTA DE QUALIDADE JR', '2024-11-10'),
    (100, 'MARIA PEREIRA', 'JUNDIAI', 'PRODUCAO', 'OPERADOR DE MAQUINA III', '2024-12-13');


-- Exibe as tabelas criadas e populadas
SELECT matricula, nome, filial, departamento, cargo, data_admissao
FROM dbo.admissoes;

SELECT matricula, nome, filial, departamento, cargo, data_alteracao
FROM dbo.alteracoes;

SELECT matricula, nome, filial, departamento, cargo, data_desligamento
FROM dbo.desligamentos;




