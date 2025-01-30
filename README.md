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
7. Rode o Data Pipeline `PL_01_Full` e verifique o resultado no Lakehouse `LK_01`;
    Você pode usar a query abaixo no ponto de extremidade SQL.  
    ```sql
    SELECT * FROM dim_funcionarios
    ORDER BY DataLog;
    ```  
8. Rode o Data Pipeline `PL_02_Incremental` e verifique o resultado no Lakehouse `LK_01`;
9.  De dentro do Lakehouse `LK_01` crie um novo modelo semântico, selecionando as tabelas:
  - dim_calendario
  - dim_funcionarios
  - fact_horas
  - medidas  
10. Abra o Power BI Desktop, faça o login em sua conta caso não estiver. Obtenha dados do OneLake e escolha `Modelos Semânticos do Power BI`. Escolha o modelo recém-criado e ao lado do botão conectar clique em `editar`.  
11. Abra a ferramenta externa `Tabular Editor` efetue o login se solicitado, copie o código abaixo e cole na área de scripts C#. Rode e Salve. Feche o `Tabular Editor`.
    
```csharp
// Este script realiza as seguintes operações:
// 1. Faz a ordenação das colunas de texto pelas colunas numéricas
// 2. Organiza as colunas em pastas por granularidade
// 3. Aplica o formato short date para colunas do tipo data
// 4. Remove agregações das colunas numéricas
// 5. Marca a tabela como tabela de data

// Acessa a tabela calendario. O nome da tabela é case-sensitive
var calendario = Model.Tables["dim_calendario"];  

// Cria um mapeamento das colunas de texto e suas respectivas colunas numéricas para ordenação
var columnPairs = new Dictionary<string, string>
{
    {"AnoAtual", "Ano"}, 
    {"DataAtual", "Data"}, 
    {"DiaSemanaNome", "DiaSemanaNum"}, 
    {"DiaSemanaNomeAbrev", "DiaSemanaNum"},
    {"MesNome", "MesNum"},
    {"MesNomeAbrev", "MesNum"},
    {"SemanaAnoIsoNome", "SemanaAnoIsoNum"},
    {"SemanaAtual", "SemanaAnoIsoNum"},
    {"TrimestreAnoNome", "TrimestreAnoNum"},
    {"TrimestreAtual", "TrimestreAnoNum"},
    {"MesAnoNome", "MesAnoNum"}, 
    {"MesAtual", "MesAnoNum"}, 
    {"MesFiscalNome", "MesFiscalNum"},
    {"MesFiscalNomeAbrev", "MesFiscalNum"}
};

// Aplica a ordenação para cada coluna de texto
foreach (var pair in columnPairs)
{
    var textColumn = calendario.Columns[pair.Key];  // Coluna de texto
    var sortColumn = calendario.Columns[pair.Value];  // Coluna numérica correspondente

    // Verifica se ambas as colunas existem e aplica a ordenação
    if (textColumn != null && sortColumn != null)
    {
        textColumn.SortByColumn = sortColumn;  // Ordena a coluna de texto pela coluna numérica
    }
}

// Dicionário para associar as colunas às pastas correspondentes
var displayFolders = new Dictionary<string, string[]>
{
    { "Ano", new[] { "Ano", "AnoAtual", "AnoFiscal", "AnoIso" } },
    { "Dia", new[] { "Data", "DataAtual", "Dia", "DiaSemanaNome", "DiaSemanaNomeAbrev", "DiaSemanaNum" } },
    { "Dias Úteis / Feriados", new[] { "E_DiaUtil", "E_Feriado", "E_FinalSemana", "Feriado" } },
    { "Meses", new[] { "MesAnoNome", "MesAnoNum", "MesAtual", "MesFiscalNome", "MesFiscalNomeAbrev", "MesFiscalNum", "MesNome", "MesNomeAbrev", "MesNum" } },
    { "Semanas", new[] { "SemanaAnoIsoNome", "SemanaAnoIsoNum", "SemanaAtual", "SemanaIsoNum" } },
    { "Trimestres", new[] { "TrimestreAnoNome", "TrimestreAnoNum", "TrimestreAtual", "TrimestreFiscal", "TrimestreNum" } }
};

// Itera sobre as pastas e aplica o DisplayFolder a cada coluna associada
foreach (var folder in displayFolders)
{
    var folderName = folder.Key;
    var columns = folder.Value;

    foreach (var columnName in columns)
    {
        var column = calendario.Columns[columnName];
        if (column != null)
        {
            column.DisplayFolder = folderName; // Atribue as colunas à pasta correspondente
        }
    }
}

// Desabilitar agregações para todas as colunas da tabela
foreach (var column in calendario.Columns)
{
    column.SummarizeBy = AggregateFunction.None;  // Desabilitar agregação
}

// Definir o formato para as colunas do tipo Data
var dateColumns = new[] { "Data" };  // Colunas que contêm datas
foreach (var columnName in dateColumns)
{
    var column = calendario.Columns[columnName];
    if (column != null)
    {
        column.FormatString = "Short Date";  // Aplica o formato de data curta
    }
}

// Marcar como uma tabela de data
calendario.DataCategory = "Time";
calendario.Columns["Data"].IsKey = true; 
```

12. Retornando ao Power BI Desktop atualize o modelo na guia página inicial. Feche qualquer aviso.
13.  Efetue os seguintes relacionamentos:  

| De                     | Para                                     | Cardinalidade | Ativo? |
|------------------------|------------------------------------------|---------------|--------|
|'dim_calendario'\[Data] | 'fact_horas'\[Data]                      | 1:N           | Y      |
|'dim_calendario'\[Data] | 'dim_funcionarios'\[DataVigenciaInicial] | 1:N           | N      |
|'dim_calendario'\[Data] | 'dim_funcionarios'\[DataVigenciaFinal]   | 1:N           | N      |

14. Crie as medidas DAX:

### Medida \[Absenteismo]

> Vamos calcular o absenteísmo que é o total de horas faltantes sobre o total de horas disponíveis, ou horas Disponiveis como aqui chamadas.   

```DAX
Absenteismo = 
VAR __Disponiveis = SUM(fact_horas_trabalhadas[HorasDisponiveis])
VAR __Trabalhadas = SUM(fact_horas_trabalhadas[HorasTrabalhadas])
VAR __Faltantes = __Disponiveis - __Trabalhadas
RETURN
    DIVIDE(__Faltantes, __Disponiveis)
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

15. Lembre-se de ajustar os formatos de números das colunas e medidas se necessário. 
16. Clique para atualizar na guia `Página inicial` novamente e encerre o Power BI Desktop.  
17. Abra novamente o Power BI Desktop, escolha obter dados do OneLake, escolha o modelo semântico e clique em conectar.  
18. Crie o relatório conforme explicado no vídeo, salve em localmente e publique o relatório no Workspace.  




