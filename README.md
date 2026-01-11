# Projeto TOPALS - Mortalidade Municipal no Brasil

## 📊 Visão Geral

Este projeto implementa uma metodologia estatística avançada para estimar a mortalidade municipal no Brasil, utilizando o método **TOPALS (TOP** **A**djustment of **L**og-**S**chedules) com âncora nas tábuas oficiais do IBGE. O sistema produz estimativas de:

- **Esperança de vida ao nascer (e0)** para municípios brasileiros (2000-2023)
- **Taxas específicas de mortalidade (mx)** por idade simples (0-100 anos)
- **Indicadores derivados**: e60, APVP, desigualdade na longevidade, decomposição de mudanças
- **Mapas nacionais e estaduais** da esperança de vida

O pipeline é **sensível ao sexo** (masculino, feminino, ambos) e inclui ajustes pós-estimação (shrink) para alinhar as estimativas municipais às referências estaduais.

## 🛠️ Pré-requisitos

### Software Requerido
- **R** (versão ≥ 4.1.0 recomendada)
- **RStudio** (opcional, mas recomendado para desenvolvimento)
- **Git** (para controle de versão)

### Pacotes R Principais
O sistema utiliza os seguintes pacotes (instalação automática pode ser configurada):

```r
# Pacotes essenciais
install.packages(c(
  "dplyr", "tidyr", "purrr", "stringr", "readr", "tibble",
  "ggplot2", "sf", "geobr", "viridis", "grid",
  "arrow", "janitor", "readxl", "splines", "rstan"
))

# Para decomposição demográfica (opcional)
install.packages("DemoDecomp")

# Para análises espaciais (opcional)
install.packages("spdep")
```

### Dependências Específicas
- **rstan**: Requer compilador C++ compatível (RTools no Windows)
- **geobr**: Faz download automático de shapes do IBGE
- **arrow**: Para leitura/escrita eficiente de dados em Parquet

## 📁 Estrutura de Pastas

```
TOPALS/
├── 00_prep_topals.R                         # Script de preparação dos dados
├── 00_prep_topals_OTIMIZADO.R               # Versão otimizada (opcional)
├── 00b_build_tabua_ibge_uf.R                # Constrói tábuas IBGE (contido no pipeline)
├── pipeline_topals_pi_ibge_unico.R          # Pipeline principal (00b+01+02+03+05B)
├── 06_analises_avancadas_e0.R               # Análises avançadas
├── 07_mapa_e0_brasil.R                      # Mapas nacionais
├── 00_prep_topals_output/                   # Saídas do passo 00
│   └── bases_topals_preparadas.RData        # Dados preparados
├── projecoes_2024_tab5_tabuas_mortalidade.xlsx  # Tábuas IBGE (fonte externa)
├── resultados/                              # Todos os resultados
│   ├── BRASIL/                              # Agregado nacional
│   │   ├── bancos_de_dados/
│   │   └── figuras/
│   └── [UF]/                                # Por estado (ex.: PB, SP, MG)
│       └── sexo_[b|m|f]/                    # Por sexo (ambos, masculino, feminino)
│           ├── figuras/
│           ├── bancos_de_dados/
│           └── indicadores_avancados/
└── dados/                                   # Dados brutos (estrutura sugerida)
    ├── pops_municipio_idade_sexo.parquet    # População (input)
    └── obitos_municipio_idade_sexo.parquet  # Óbitos (input)
```

## 📥 Dados de Entrada

### 1. Dados Demográficos
Dois arquivos principais (formato Parquet recomendado):

**População:**
- Colunas requeridas: `ano`, `code_muni6` (ou similar), `idade`, `sexo`, `pop`
- Idades: 0-100 anos (preferencialmente idade simples)
- Sexo: "m"/"f" ou "1"/"2" (será normalizado para "m"/"f"/"b")

**Óbitos:**
- Colunas requeridas: `ano`, `code_muni6`, `idade`, `sexo`, `obitos`
- Opcional: `cobertura_sim` (cobertura do SIM)

### 2. Tábuas de Mortalidade IBGE
- Arquivo: `projecoes_2024_tab5_tabuas_mortalidade.xlsx`
- Fonte: IBGE (projeções 2024)
- Contém: Tábuas completas de mortalidade por UF, sexo e ano (2000-2070)

### 3. Formato dos Códigos Municipais
- **6 dígitos** (ex: 250750 = João Pessoa/PB)
- O sistema aceita 6 ou 7 dígitos (com dígito verificador)

## 🔄 Fluxo de Trabalho

### Passo 1: Preparação dos Dados
```r
# Ajuste os caminhos no script
source("00_prep_topals.R")
```
**O que faz:**
- Lê dados de população e óbitos
- Normaliza sexo (m/f/b)
- Adiciona informações geográficas (UF, região, RGI)
- Salva `bases_topals_preparadas.RData`

**Saída:**
- `00_prep_topals_output/bases_topals_preparadas.RData`

### Passo 2: Pipeline Principal (Estado + Sexo)
```r
# Configure no início do script:
# UF_ALVO <- "PB"  # Estado desejado
# SEXO_ALVO <- "b" # "b" (ambos), "m" (masculino), "f" (feminino)

source("pipeline_topals_pi_ibge_unico.R")
```

**Etapas internas:**
1. **00b**: Constrói tábuas IBGE com sexo
2. **01**: Ajusta modelos TOPALS+pi+IBGE para cada ano
3. **02**: Extrai e0 e pi (cobertura) das estimativas
4. **03**: Prepara função de reconstrução de mx
5. **05B**: Shrink ex-post + NMX final + mapas e tabelas

**Saídas (por UF/sexo):**
- `resultados/[UF]/sexo_[b|m|f]/bancos_de_dados/`
  - `nmx_final_municipios_idade_simples.parquet`
  - `tabela_vida_municipios_idade_simples.parquet`
  - `e0_municipios_post_shrink.parquet`
- `resultados/[UF]/sexo_[b|m|f]/figuras/`
  - Mapas de e0 e e60
  - Séries temporais para municípios foco
  - Curvas de log(mx)

### Passo 3: Análises Avançadas (Opcional)
```r
# Configure UF_ALVO e SEXO_ALVO
source("06_analises_avancadas_e0.R")
```

**Análises geradas:**
- Mapas de ganho absoluto em e0 (2000-2023)
- Decomposição de ∆e0 por idade (método Horiuchi)
- APVP (Anos Potenciais de Vida Perdidos)
- Gini da morte (desigualdade na longevidade)
- Clusters LISA de mortalidade
- Curvas de sobrevivência comparativas

### Passo 4: Mapas Nacionais
```r
source("07_mapa_e0_brasil.R")
```

**Requisito:** Ter executado o pipeline para **todas as UFs** (pelo menos para sexo="b")

**Saídas em `resultados/BRASIL/`:**
- Mapas municipais de e0 (2000 e 2023) - versões bruta e ajustada
- Mapa 2x2 comparativo (2000/2023 × bruto/ajustado)
- Curvas nacionais de log(mx) e e0 mediana

## 📊 Saídas Principais

### 1. Indicadores Municipais
- `e0_p50_post`: Esperança de vida ao nascer (pós-shrink)
- `e0_raw`: e0 bruta (sem ajuste)
- `e60_post`: Esperança de vida aos 60 anos
- `mx_nmx_final`: Taxas específicas de mortalidade suavizadas

### 2. Tabelas de Vida Completas
Por município, ano e sexo:
- `lx`, `dx`, `qx`, `Lx`, `Tx`, `ex`

### 3. Figuras e Mapas
- **Mapas estaduais**: e0, e60, APVP, déficit vs UF
- **Séries temporais**: e0 municipal vs estadual
- **Curvas de mortalidade**: log(mx) por idade
- **Mapas nacionais**: e0 municipal para 2000 e 2023

### 4. Estatísticas Avançadas
- Decomposição da mudança em e0 por idade
- Anos Potenciais de Vida Perdidos (APVP)
- Índice de Gini da idade ao óbito
- Clusters espaciais (LISA) de mortalidade

## ⚙️ Configurações Importantes

### No Pipeline Principal:
```r
# Configure no início do script pipeline_topals_pi_ibge_unico.R
BASE_DIR <- "C:/seu/caminho/para/TOPALS"  # Ajuste obrigatório
UF_ALVO <- "PB"      # Estado a ser processado
SEXO_ALVO <- "b"     # "b", "m" ou "f"
ANOS_FIT <- 2000:2023 # Anos para estimação
NIVEIS_FIT <- "municipio" # Nível geográfico
```

### Na Preparação de Dados:
```r
# Em 00_prep_topals.R, ajuste:
POP_INPUT <- "caminho/para/populacao.parquet"
OBITOS_INPUT <- "caminho/para/obitos.parquet"
UF_FILTER <- NULL  # NULL para todas UFs, ou c("PB", "PE") para filtrar
```

## 🚀 Execução em Lote

Para processar múltiplos estados/sexos:

```r
# Exemplo: processar PB, PE e CE para ambos os sexos
estados <- c("PB", "PE", "CE")
sexos <- c("b", "m", "f")

for(uf in estados) {
  for(sexo in sexos) {
    # 1. Configurar UF_ALVO e SEXO_ALVO no script
    # 2. Executar pipeline_topals_pi_ibge_unico.R
    # 3. Executar 06_analises_avancadas_e0.R (opcional)
  }
}

# Após todos estados, executar 07_mapa_e0_brasil.R
```

## 🐛 Solução de Problemas

### Problema: Erro na compilação do Stan
**Solução:** Verifique instalação do RTools (Windows) ou compilador C++. Tente:
```r
install.packages("rstan", repos = "https://cloud.r-project.org/", dependencies = TRUE)
rstan::rstan_options(auto_write = TRUE)
```

### Problema: Dados geográficos não carregam
**Solução:** O geobr requer internet para download. Verifique conexão ou use cache:
```r
options(geobr.use_cache = TRUE)
```

### Problema: Memória insuficiente
**Solução:** Para estados grandes (SP, MG), processe por subconjuntos:
```r
UF_FILTER <- c("SP")  # No 00_prep_topals.R para filtrar apenas SP
```

### Problema: Arquivos de entrada não encontrados
**Solução:** Verifique:
1. Caminhos absolutos em `00_prep_topals.R`
2. Existência dos arquivos Parquet
3. Permissões de leitura

## 📈 Exemplos de Uso

### 1. Obter e0 municipal para João Pessoa (2023)
```r
library(arrow)
e0_pb <- read_parquet("resultados/PB/sexo_b/bancos_de_dados/e0_municipios_post_shrink.parquet")
joao_pessoa <- e0_pb %>% 
  filter(code_muni6 == 250750, ano == 2023) %>%
  select(e0_p50_post, e0_ibge)
```

### 2. Criar mapa personalizado de e0
```r
library(sf)
library(ggplot2)

dados <- read_parquet("resultados/PB/sexo_b/bancos_de_dados/e0_municipios_post_shrink.parquet")
mapa_pb <- geobr::read_municipality(code_muni = "PB", year = 2020)

mapa_pb <- mapa_pb %>%
  mutate(code_muni6 = as.integer(substr(code_muni, 1, 6))) %>%
  left_join(dados %>% filter(ano == 2023), by = "code_muni6")

ggplot(mapa_pb) +
  geom_sf(aes(fill = e0_p50_post), color = NA) +
  scale_fill_viridis_c(option = "magma") +
  theme_void()
```

## 📚 Referências Métodológicas

1. **TOPALS**: 
   - De Beer, J., & van der Gaag, N. (2015). TOPALS: A tool for projecting age-specific rates using linear splines.
   - Schmertmann, C., & Gonzaga, M. (2018). Bayesian estimation of age-specific mortality and life expectancy for small areas.

2. **Âncora IBGE**:
   - IBGE. (2024). Tábuas Completas de Mortalidade - Projeções 2024.

3. **Shrinkage Bayesiano**:
   - Gelman, A., et al. (2013). Bayesian Data Analysis.

4. **Decomposição Demográfica**:
   - Horiuchi, S., et al. (2008). Decomposing change in life expectancy.

## 🤝 Contribuições

Contribuições são bem-vindas! Por favor:

1. Fork o repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-analise`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova análise'`)
4. Push para a branch (`git push origin feature/nova-analise`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo LICENSE para detalhes.

## 🙋‍♂️ Suporte

Para questões ou problemas:
1. Verifique a seção de Solução de Problemas acima
2. Abra uma issue no GitHub
3. Contate: [ubh@academico.ufpb.br]

---

**Nota**: Este README descreve a versão do pipeline que inclui separação por sexo e ajuste pós-estimação (shrink). Para a versão sem sexo ou sem ajuste, consulte branches anteriores do repositório.

**Última atualização**: Novembro 2024  
**Versão do Pipeline**: 2.0 (com sexo e shrink)  
**Compatibilidade**: R ≥ 4.1.0, dados SIM/Demografia 2000-2023
