###############################################################################
# 00b_build_tabua_ibge_uf.R
#
# Objetivo:
#   Ler o arquivo de tábuas de mortalidade do IBGE (projeções 2024, Tab 5),
#   extrair mx por UF x ano x idade (ambos os sexos) e construir o objeto
#   `tabua_ibge_uf`, usado como padrão (std_schedule) nos scripts TOPALS.
#
# Saídas:
#   - Objeto `tabua_ibge_uf` com colunas:
#       * uf_sigla
#       * ano
#       * idade
#       * mx_ibge
#   - Atualização de `bases_topals_preparadas.RData` para passar a incluir
#     `tabua_ibge_uf` junto com base_muni_raw, base_muni, pesos_80mais, etc.
###############################################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(readxl)
  library(stringr)
  library(janitor)
})

# ----------------- 0. Caminhos básicos --------------------------------------

BASE_DIR <- "C:/Users/uriel/Documents/UFPB Estatística/Demografia 1/Estudos Demográficos - PB/TOPALS"

FILE_REF <- file.path(
  BASE_DIR,
  "projecoes_2024_tab5_tabuas_mortalidade.xlsx"
)

OUTPUT_DIR <- file.path(BASE_DIR, "00_prep_topals_output")
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

rdata_path <- file.path(OUTPUT_DIR, "bases_topals_preparadas.RData")

if (!file.exists(FILE_REF)) {
  stop("Arquivo de referência do IBGE não encontrado em:\n  ", FILE_REF)
}

# ----------------- 1. Escolher planilha (sheet) -----------------------------

sheets <- readxl::excel_sheets(FILE_REF)

# Na sua planilha, é "5) TÁBUAS DE MORTALIDADE"
sheet_candidates <- sheets[grepl("5", sheets) & grepl("T[ÁA]BUAS", sheets, ignore.case = TRUE)]

if (length(sheet_candidates) == 0L) {
  sheet_use <- sheets[1]
  message("Não encontrei aba com '5' e 'TÁBUAS' no nome. Usando a primeira aba: ", sheet_use)
} else {
  sheet_use <- sheet_candidates[1]
  message("Usando aba da tábua IBGE: ", sheet_use)
}

# ----------------- 2. Detectar a linha de cabeçalho real --------------------

# Lê sem nomes pra enxergar as linhas brutas
tmp0 <- readxl::read_excel(
  FILE_REF,
  sheet     = sheet_use,
  col_names = FALSE
)

# A linha de cabeçalho tem "IDADE" na primeira coluna (pelo seu preview)
header_row_candidates <- which(
  !is.na(tmp0[[1]]) &
    stringr::str_detect(tmp0[[1]], regex("^IDADE$", ignore_case = TRUE))
)

if (length(header_row_candidates) == 0L) {
  stop("Não encontrei uma linha com 'IDADE' na primeira coluna para usar como cabeçalho.")
}

header_row <- header_row_candidates[1]
message("Linha de cabeçalho detectada: ", header_row)

# ----------------- 3. Releitura com cabeçalho correto ----------------------

tab_raw <- readxl::read_excel(
  FILE_REF,
  sheet     = sheet_use,
  skip      = header_row - 1L,  # pula até a linha anterior à de cabeçalho
  col_names = TRUE
)

tab_raw <- janitor::clean_names(tab_raw)

nm <- names(tab_raw)
message("Colunas detectadas na tábua IBGE (após detecção de cabeçalho):")
print(nm)

# ----------------- 4. Função auxiliar para escolher colunas -----------------

pick_col <- function(patterns) {
  hits <- Reduce(
    `|`,
    lapply(patterns, function(p) grepl(p, nm, ignore.case = TRUE))
  )
  pos <- which(hits)
  if (length(pos) == 0L) return(NA_character_)
  nm[pos[1]]
}

# Sabendo da estrutura (IDADE, SEXO, ANO, CÓD., SIGLA, LOCAL, nMx,...):
col_ano   <- pick_col(c("^ano$"))
col_sexo  <- pick_col(c("^sexo$"))
col_uf    <- pick_col(c("^sigla$", "^sigla_uf$", "^uf$"))
col_idade <- pick_col(c("^idade$"))
col_mx    <- pick_col(c("^nmx$", "mx"))

needed <- c(col_ano, col_sexo, col_uf, col_idade, col_mx)
names(needed) <- c("ano", "sexo", "uf", "idade", "mx")

if (any(is.na(needed))) {
  stop(
    "Não consegui identificar todas as colunas necessárias na tábua IBGE.\n",
    "Verifique os nomes retornados por names(tab_raw) e ajuste os padrões.\n\n",
    "Mapeamento atual:\n",
    paste0(names(needed), " -> ", needed, collapse = "\n")
  )
}

message("\nMapeamento de colunas IBGE -> script:")
print(needed)

# ----------------- 5. Tabela de UFs (igual ao 00) ---------------------------

uf_table <- function() {
  tibble::tibble(
    uf_code  = c("11","12","13","14","15","16","17",
                 "21","22","23","24","25","26","27","28","29",
                 "31","32","33","35",
                 "41","42","43",
                 "50","51","52","53"),
    uf_sigla = c("RO","AC","AM","RR","PA","AP","TO",
                 "MA","PI","CE","RN","PB","PE","AL","SE","BA",
                 "MG","ES","RJ","SP",
                 "PR","SC","RS",
                 "MS","MT","GO","DF")
  )
}

ufs <- uf_table()

# ----------------- 6. Construir tabua_ibge_uf --------------------------------

tabua_ibge_uf <- tab_raw %>%
  # Filtra sexo = ambos os sexos (qualquer variação)
  dplyr::filter(
    stringr::str_detect(.data[[col_sexo]], regex("ambos", ignore_case = TRUE))
  ) %>%
  dplyr::transmute(
    uf_sigla = as.character(.data[[col_uf]]),
    ano      = as.integer(.data[[col_ano]]),
    idade    = as.integer(.data[[col_idade]]),
    mx_ibge  = as.numeric(.data[[col_mx]])
  ) %>%
  # Garante apenas UFs válidas (tira Brasil, regiões etc.)
  dplyr::inner_join(ufs, by = "uf_sigla") %>%
  dplyr::select(uf_sigla, ano, idade, mx_ibge) %>%
  dplyr::filter(
    !is.na(ano),
    !is.na(idade),
    !is.na(mx_ibge)
  ) %>%
  # Limita ao intervalo que usamos na modelagem
  dplyr::filter(ano >= 2000, ano <= 2070) %>%
  dplyr::arrange(uf_sigla, ano, idade)

message("\nResumo rápido de tabua_ibge_uf (UF == 'PB'):")
print(
  tabua_ibge_uf %>%
    dplyr::filter(uf_sigla == "PB") %>%
    dplyr::count(ano, name = "n_idades") %>%
    head()
)

# ----------------- 7. Atualizar bases_topals_preparadas.RData ----------------

if (file.exists(rdata_path)) {
  message("\nCarregando ", rdata_path, " para atualizar com tabua_ibge_uf...")
  load(rdata_path)  # deveria trazer base_muni_raw, base_muni, pesos_80mais
  
  # salva de novo incluindo o novo objeto
  save(
    base_muni_raw,
    base_muni,
    pesos_80mais,
    tabua_ibge_uf,
    file = rdata_path
  )
  
  message("Atualizei ", basename(rdata_path),
          " para incluir o objeto 'tabua_ibge_uf'.")
} else {
  message("\nATENÇÃO: ", basename(rdata_path), " ainda não existe.")
  message("Vou criar um novo .RData apenas com tabua_ibge_uf.")
  save(tabua_ibge_uf, file = rdata_path)
}

# Também salvo separado se quiser inspecionar isolado
tab_ibge_rdata <- file.path(OUTPUT_DIR, "tabua_ibge_uf_only.RData")
save(tabua_ibge_uf, file = tab_ibge_rdata)

# ----------------- 8. Mensagens finais --------------------------------------

cat("\n✅ Construção de 'tabua_ibge_uf' concluída com sucesso!\n")
cat("   - Linhas: ", nrow(tabua_ibge_uf), " (UF x ano x idade)\n", sep = "")
cat("   - Intervalo de anos em tabua_ibge_uf:",
    min(tabua_ibge_uf$ano), "a", max(tabua_ibge_uf$ano), "\n")
cat("   - UF exemplo (PB) -> anos disponíveis:\n")
print(sort(unique(tabua_ibge_uf$ano[tabua_ibge_uf$uf_sigla == "PB"])))
cat("\n📁 Arquivos atualizados em:\n")
cat("   • ", rdata_path, "\n", sep = "")
cat("   • ", tab_ibge_rdata, "\n\n", sep = "")
cat("➡️ Próximo passo: rodar o 01 com std_schedule vindo de tabua_ibge_uf.\n\n")

###############################################################################
# Fim do script 00b_build_tabua_ibge_uf.R
###############################################################################
