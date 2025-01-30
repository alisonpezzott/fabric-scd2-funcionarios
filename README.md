# fabric-scd2-funcionarios

Este repositório tem o objetivo de auxiliar o processo de aprendizagem com o Microsoft Fabric sobre o assunto Slowly Changing Dimensions (SCD) especialmente do tipo 2.


## Agenda

- Criaremos uma tabela `dim_funcionarios` com estrutura SCD do tipo 2 a partir de um log de alterações;  
- Criaremos uma tabela `fact_horas_trabalhadas` que receberá a _Surrogate Key_ da `dim_funcionarios`;  
- Utilizaremos Notebooks com spark e sql atrelado a uma Lakehouse;  
- Vamos elaborar dois tipos de rotinas: Full e Incremental;  
- Construiremos um modelo semântico em _Direct Lake_ e um relatório do Power BI;  
- Como exemplo, vamos adicionar as medidas DAX relacionadas aos indicadores mais utilizados em People Analytics: `Absenteismo` e `Turnover`.  


## Instruções  

1. Crie um workspace com Capacidade da malha ou Trial do Fabric;
2. Adicione um Lakehouse e dê o nome de `LK_01`;
3. Faça o download da pasta [notebooks](notebooks) e faça o upload dos notebooks para o workspace criado;
4. Importe o arquivo [horas_trabalhadas.csv](horas_trabalhadas.csv) para a seção Files do Lakehouse;
5. Abra cada um dos notebooks importados e adicione o Lakehouse `LK_01` recém-criado como Lakehouse default do Notebook;
6. Crie dois Data Pipelines;
   1. O primeiro chamado `PL_01_Full` você incluirá as atividades de **Notebooks full** e o **Calendario**, onde primeiro atualiza os Funcionarios e caso sucesso atualiza as HorasTrabalhadas;
   2. No segundo chamado `PL_02_Incremental` você incluirá as atividades de **Notebooks incrementais** e o **Calendario**, da mesma forma, onde primeiro atualiza os Funcionarios e caso sucesso atualiza as HorasTrabalhadas;  
7. Execute o Data Pipeline `PL_01_Full` e verifique o resultado no Lakehouse `LK_01`;
8. Execute o Data Pipeline `PL_02_Incremental` e verifique o resultado no Lakehouse `LK_01`;
9. De dentro do Lakehouse `LK_01` crie um novo modelo semântico, selecionando as tabelas:
  - dim_calendario
  - dim_funcionarios
  - fact_horas
  - 


## Medidas DAX

### Medida \[Absenteismo]

> Vamos calcular o absenteísmo que é o total de horas faltantes sobre o total de horas disponíveis, ou horas normais como aqui chamadas.   

```DAX
Absenteismo = 
VAR __Normais = SUM(fact_horas_trabalhadas[HorasNormais])
VAR __Trabalhadas = SUM(fact_horas_trabalhadas[HorasTrabalhadas])
VAR __Faltantes = __Normais - __Trabalhadas
RETURN
    DIVIDE(__Faltantes, __Normais)
```



### Medida \[Admissoes]

> Está medida calcula as admissões pela data. Note que há um filtro onde apenas retornem as linhas onde a 'DataAdmissao' é igual a 'DataVigenciaInicial', isto para trazer exatamente as outras características do funcionário que foi admitido.   

```DAX
Admissoes = 
    CALCULATE(
        DISTINCTCOUNT(dim_funcionarios[Matricula]),
        USERELATIONSHIP(
            dim_calendario[Data],
            dim_funcionarios[DataAdmissao]
        ),
        FILTER(
            dim_funcionarios,
            dim_funcionarios[DataAdmissao] = 
            dim_funcionarios[DataVigenciaInicial]
        )
    )
```  

### Medida \[Desligamentos]  

> Esta medida calcula as demissões pela data. Note que há um filtro para que retornem apenas as linhas onde não está em branco a coluna 'DataDesligamento'.  

```DAX
Desligamentos = 
    CALCULATE(
        DISTINCTCOUNT(dim_funcionarios[Matricula]),
        USERELATIONSHIP(
            dim_calendario[Data],
            dim_funcionarios[DataDesligamento]
        ),
        NOT ISBLANK( dim_funcionarios[DataDesligamento] )
    )
```  

### Medida \[Ativos]  

> Esta medida calcula o total de funcionários ativos no período pelas colunas 'DataVigenciaInicial' e 'DataVigenciaFinal'.  

```DAX
Ativos = 
    CALCULATE(
        DISTINCTCOUNT(dim_funcionarios[Matricula]),
        FILTER(
            dim_funcionarios,
            dim_funcionarios[DataVigenciaInicial] <= MAX(dim_calendario[Data]) 
                && dim_funcionarios[DataVigenciaFinal] >= MIN(dim_calendario[Data])
        )
    )
```

### Medida \[Turnover]

> Esta medida calcula a média entre \[Admissoes] e \[Demissoes] e divide pelo total de \[Ativos].  

```DAX
Turnover = 
    DIVIDE(
        AVERAGEX({[Admissoes], [Desligamentos]}, [Value]),
        [Ativos]
    )
``` 




