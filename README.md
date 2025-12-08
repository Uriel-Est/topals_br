# Pipeline TOPALS – Esperança de vida municipal

Este repositório implementa um pipeline completo para estimar e analisar a esperança de vida ao nascer (e₀) em nível municipal no Brasil usando TOPALS + *shrinkage* para a UF e indicadores derivados.

A lógica geral é:

1. **00B – preparação das bases (`bases_topals_preparadas.RData`)**
2. **`pipeline_unico_*` – estimação TOPALS + e₀ municipal (00B + 01 + 02 + 03 + 05B)**
3. **`06_analises_avancadas_e0.R` – indicadores avançados de mortalidade e longevidade**

Tudo é parametrizado principalmente por:

- `UF_ALVO` – sigla da unidade da federação (`"PB"`, `"SP"`, …)  
- `SEXO_ALVO` – sexo de interesse (`"b"` para ambos; outros códigos conforme base)

---

## Estrutura mínima de pastas

Sugerido (pode adaptar, desde que ajuste `BASE_DIR` nos scripts):

```
.
├── R/
│   ├── 00B.R
│   ├── topals_pi_ibge_pipeline_unico.R
│   └── topals_indicadores_avancados.R
└── data/
    ├── raw/              # microdados SIM, populações, etc.
    ├── 00_mx_reconstrucao/
    │   └── bases_topals_preparadas.RData
    ├── 05B_mx_post_e0_<UF>/
    │   ├── tabelas/
    │   └── figuras/
    └── 06_analises_avancadas/
        └── <UF>/
            ├── tabelas/
            └── figuras/
```

Nos scripts eu uso um `BASE_DIR` que aponta para a pasta do projeto. A partir dele, cada etapa cria suas próprias subpastas.

---

## Dependências de software

* R (>= 4.x)
* Pacotes principais:

  * `dplyr`, `tidyr`, `purrr`, `readr`, `tibble`, `stringr`, `ggplot2`
  * `sf`, `geobr`, `viridis`
* Pacotes opcionais (blocos correspondentes são pulados se não estiverem instalados):

  * `spdep` – suavização espacial e LISA
  * `DemoDecomp` – decomposição de Horiuchi
  * `plotly` + `htmlwidgets` – superfície 3D interativa de log(mx)

Instalação rápida:

```r
pkgs <- c(
  "dplyr", "tidyr", "purrr", "readr", "tibble", "stringr",
  "ggplot2", "sf", "geobr", "viridis",
  "spdep", "DemoDecomp", "plotly", "htmlwidgets"
)
install.packages(setdiff(pkgs, rownames(installed.packages())))
```

---

## Etapa 0 – 00B: preparação das bases (`00B_prep_topals.R`)

**Objetivo:** construir as bases padronizadas de mortalidade e população por município, ano, idade (e sexo) a partir dos microdados brutos.

**Entradas típicas (em `data/raw/`, nomes podem variar):**

* Óbitos – SIM / outra fonte de mortalidade por município, ano, idade e sexo
* Populações / exposições – POPSVS / projeções / IBGE
* Tabelas auxiliares:

  * códigos de municípios (`code_muni6` / `code_muni`)
  * pesos para 80+ (se usados no ajuste da cauda)

**Saídas principais:**

* Arquivo RData com o conjunto de bases preparadas:

  ```
  data/00_mx_reconstrucao/bases_topals_preparadas.RData
  ```

  contendo, pelo menos:

  * `base_muni` – mortes e população por `uf_sigla`, `code_muni6`, `ano`, `idade`, `sexo` (ou `pop_ambos`)
  * (outros objetos auxiliares usados pelo pipeline)

**Como rodar:**

```r
# dentro de R/
source("R/00B_prep_topals.R")
```

Essa etapa precisa ser executada **uma vez** (ou sempre que a base bruta for atualizada).

---

## Etapa 1 – pipeline único TOPALS (`pipeline_unico_topals.R`)

**Objetivo:** executar toda a sequência de estimação TOPALS + *shrinkage* para uma UF específica, produzindo séries de `mx` pós-ajuste e `e₀` municipal.

O script incorpora as etapas originais:

* 00B – leitura de `bases_topals_preparadas.RData`
* 01 – construção das tábuas “alvo” (IBGE / referência)
* 02 – ajuste TOPALS por município
* 03 – *shrinkage* / suavização para a UF
* 05B – cálculo de `e0` e geração de saídas

**Parâmetros principais dentro do script:**

```r
BASE_DIR  <- "path/para/o/projeto"
UF_ALVO   <- "PB"   # mudar para "SP", "MG", ...
SEXO_ALVO <- "b"    # ambos, ou conforme nomenclatura usada
ANOS_ANALISE <- 2000:2023
```

**Entradas obrigatórias:**

* `data/00_mx_reconstrucao/bases_topals_preparadas.RData`
* (opcional) tábuas de vida IBGE / referência, caso venham de arquivo externo

**Saídas principais (por UF):**

Cria a pasta:

```
data/05B_mx_post_e0_<UF>/
├── tabelas/
│   ├── e0_municipios_post_shrink.csv
│   ├── mx_post_municipios_*.csv      # formatos específicos do pipeline
│   └── ...
└── figuras/
    ├── mapas_e0_*.png
    ├── curvas_topals_*.png
    └── ...
```

O arquivo-chave para as análises avançadas é:

* `e0_municipios_post_shrink.csv`

com colunas como:

* `uf`, `code_muni6`, `nome_muni`, `ano`, `sexo`, `nivel`
* `e0_p50_post` – esperança de vida pós-*shrinkage* (mediana posterior)
* `e0_ibge` – referência de e₀ da UF
* outros parâmetros do modelo.

**Como rodar:**

```r
# dentro de R/
source("R/pipeline_unico_topals.R")
```

Repetir mudando `UF_ALVO` para gerar resultados de outros estados.

---

## Etapa 2 – indicadores avançados de e₀ (`06_analises_avancadas_e0.R`)

**Objetivo:** a partir das saídas da etapa 1 (e das bases de mx da etapa 0), produzir um conjunto de indicadores e visualizações “de produto final”.

**Entradas obrigatórias:**

* `data/05B_mx_post_e0_<UF>/tabelas/e0_municipios_post_shrink.csv`
* `data/00_mx_reconstrucao/bases_topals_preparadas.RData`

**Parâmetros principais no topo do script:**

```r
BASE_DIR   <- "path/para/o/projeto"
UF_ALVO    <- "PB"
SEXO_ALVO  <- "b"

ANOS_ANALISE <- 2000:2023
ANO_INI_GAIN <- 2000L
ANO_FIM_GAIN <- 2023L
```

**Saídas:**

Cria uma pasta por UF em:

```
data/06_analises_avancadas/<UF>/
├── tabelas/
│   ├── munis_foco_<UF>_<sexo>.csv
│   ├── decomp_e0_horiuchi_*.csv
│   ├── gini_morte_e0_<UF>.csv
│   ├── avp_municipios_vs_uf_<UF>.csv
│   ├── deficit_superavit_e0_vs_uf_<UF>.csv
│   └── ...
└── figuras/
    ├── mapa_ganho_abs_e0_<UF>_2000_2023.png
    ├── mapa_e0_suavizado_<UF>_2023.png
    ├── mapa_lisa_e0_post_<UF>_2023.png
    ├── tendencia_logmx_por_faixa_UF_<UF>_<sexo>.png
    ├── tendencia_logmx_por_faixa_muni_<UF>_<sexo>_<code>.png
    ├── curvas_logmx_por_idade_muni_<UF>_<sexo>_<code>.png
    ├── decomp_e0_horiuchi_UF_<UF>_2000_2023.png
    ├── decomp_e0_horiuchi_muni_<UF>_<sexo>_<code>_2000_2023.png
    ├── curvas_lx_l0_UF_<UF>.png
    ├── curvas_lx_l0_muni_vs_uf_<UF>_<sexo>_<code>_2000_2023.png
    ├── serie_e0_gini_morte_<UF>.png
    ├── piramide_mortalidade_dx_<UF>.png
    ├── superficie_logmx_<UF>.png
    ├── superficie_logmx_<UF>_3D.html
    ├── mapa_avp_vs_uf_<UF>_2023.png
    └── mapa_deficit_e0_vs_uf_<UF>_2023.png
```

### 2.1. Seleção dos municípios foco (`munis_foco`)

O script constrói automaticamente um conjunto de municípios de interesse (`munis_foco`) que serão usados em todos os gráficos **não espaciais**:

* séries de log(mx) por faixa etária,
* curvas de log(mx) por idade,
* decomposições de e₀,
* curvas de sobrevivência lx/l₀ comparando UF × município etc.

Por padrão, a seleção combina:

1. **Extremos de nível de e₀**

   * e₀ **baixa / alta** no início do período (`ANO_INI_GAIN`)
   * e₀ **baixa / alta** no final do período (`ANO_FIM_GAIN`)

2. **Extremos de ganho em e₀**

   * municípios com **baixo ganho** absoluto em e₀
   * municípios com **alto ganho** absoluto em e₀

A seleção é feita via quantis (10% inferior / 10% superior) e, para cada subconjunto, são escolhidos até `n_top_por_tipo` municípios (default = 3). Um mesmo município pode aparecer em mais de um tipo, e os tipos são concatenados na coluna `tipo`.

O resultado é salvo em:

```
tabelas/munis_foco_<UF>_<sexo>.csv
```

#### Seleção manual / por LISA

O script também permite (ver cabeçalho do arquivo):

* **adicionar uma lista manual** de `code_muni6` a serem forçados em `munis_foco`;
* **importar clusters LISA** (High-High, Low-Low etc.) calculados no próprio script e usar apenas municípios de certos tipos de cluster.

A ideia é que você possa alternar entre:

* seleção **automática via quantis**;
* seleção **guiada por clusters LISA**;
* seleção **totalmente manual** (lista de municípios / RGIs).

Os parâmetros de controle estão definidos no topo de `06_analises_avancadas_e0.R` e são comentados dentro do próprio script.

---

## Fluxo de execução resumido

Uma sequência típica para um novo estado:

```r
# 0) Preparar bases (uma vez para o BR ou sempre que atualizar dados)
source("R/00B_prep_topals.R")

# 1) Rodar pipeline TOPALS para a UF desejada
UF_ALVO <- "PB"
source("R/pipeline_unico_topals.R")

# 2) Gerar indicadores avançados para a mesma UF / sexo
UF_ALVO   <- "PB"
SEXO_ALVO <- "b"
source("R/06_analises_avancadas_e0.R")
```

Para outro estado, basta alterar `UF_ALVO` (e eventualmente `SEXO_ALVO`) e repetir as etapas 1 e 2.

---

## Notas finais

* Vários blocos do script 06 são opcionais e protegidos por `if (has_spdep)`, `if (has_DemoDecomp)` e `if (has_plotly)`. Se você não tiver esses pacotes instalados, os mapas LISA, a decomposição de Horiuchi e a superfície 3D simplesmente serão pulados.
* Os scripts foram escritos pensando em **reprodutibilidade estadual**: sempre que possível, tudo é parametrizado por `UF_ALVO` e `SEXO_ALVO`, de forma que o mesmo código funcione para qualquer estado do Brasil, desde que as bases de entrada sigam o padrão esperado.
* Em caso de dúvida sobre o formato das bases de entrada, confira diretamente o script `00B_prep_topals.R`, onde a estrutura de `base_muni` e demais objetos é construída.
