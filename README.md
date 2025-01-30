# fabric-scd2-funcionarios



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




