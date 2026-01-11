###############################################################################
# 00_prep_topals.R  (VERSÃO COM SEXO)
#
# ÚNICA mudança conceitual vs seu 00_prep antigo:
# - Não agrega sexo no input. Mantém sexo="m"/"f" e cria sexo="b" = m+f.
#
# Saída (compatível com o pipeline principal):
#   BASE_DIR/00_prep_topals_output/bases_topals_preparadas.RData
#   Objetos: base_muni_raw, base_muni
#
###############################################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(janitor)
  library(arrow)
  library(sf)
  library(geobr)
})

# =========================
# 1) CONFIGURAÇÕES
# =========================
BASE_DIR <- "C:/Users/uriel/Documents/UFPB Estatística/Demografia 1/Estudos Demográficos - PB/TOPALS"

# >>> AJUSTE ESTES DOIS CAMINHOS <<<
# Pode ser um arquivo .parquet ou um diretório com múltiplos parquet (dataset Arrow)
POP_INPUT    <- file.path(BASE_DIR, "popsvs_muni_idade_simples_2000_2024.parquet")     # <-- ajuste
OBITOS_INPUT <- file.path(BASE_DIR, "sim_municipio_idade_simples_sexo_2000_2023.parquet") # <-- ajuste

# (opcional) filtrar UFs para ficar leve (ex.: c("SC")); NULL = tudo que existir
UF_FILTER <- NULL

# Saída esperada pelo pipeline principal
OUTPUT_00_DIR <- file.path(BASE_DIR, "00_prep_topals_output")
dir.create(OUTPUT_00_DIR, showWarnings = FALSE, recursive = TRUE)
RDATA_PATH <- file.path(OUTPUT_00_DIR, "bases_topals_preparadas.RData")

# =========================
# 2) HELPERS
# =========================
pick_col <- function(cols, cands) {
  for (cc in cands) if (cc %in% cols) return(cc)
  NA_character_
}

norm_sexo <- function(x) {
  # Aceita: 1/2/0/9, "M/F", "Masculino/Feminino", "m/f/b"
  s <- tolower(trimws(as.character(x)))
  out <- dplyr::case_when(
    s %in% c("1","m","masc","masculino","male","homem","homens") ~ "m",
    s %in% c("2","f","fem","feminino","female","mulher","mulheres") ~ "f",
    s %in% c("b","both","ambos","total") ~ "b",
    TRUE ~ NA_character_
  )
  out
}

read_arrow_any <- function(path) {
  if (dir.exists(path)) return(arrow::open_dataset(path))
  if (file.exists(path)) return(arrow::open_dataset(path))
  stop("Não encontrei path: ", path)
}

muni6_from_any <- function(x) {
  # aceita 7 dígitos (com DV) ou 6 dígitos; devolve inteiro 6 dígitos
  s <- as.character(x)
  s <- gsub("\\D", "", s)
  s <- ifelse(nchar(s) >= 6, substr(s, 1, 6), NA_character_)
  as.integer(s)
}

# =========================
# 3) CARREGAR POPULAÇÃO
# =========================
message("[00_prep] Lendo POP: ", POP_INPUT)
pop_ds <- read_arrow_any(POP_INPUT)

pop_cols <- names(pop_ds)
pop_ano   <- pick_col(pop_cols, c("ano","year","exercicio"))
pop_muni  <- pick_col(pop_cols, c("code_muni6","code_muni7","code_muni","codmun","codmun_residencia","cod_mun","id_municipio"))
pop_idade <- pick_col(pop_cols, c("idade","age","idade_simples","idade_ano"))
pop_sexo  <- pick_col(pop_cols, c("sexo","sex"))
pop_pop   <- pick_col(pop_cols, c("pop","pop_total","populacao","pop_ambos","N","pop_final","pop_corrigida"))

need_pop <- c(pop_ano, pop_muni, pop_idade, pop_sexo, pop_pop)
if (any(is.na(need_pop))) {
  stop("[POP] Não consegui identificar colunas necessárias. Achei:\n",
       "ano=", pop_ano, "\nmuni=", pop_muni, "\nidade=", pop_idade,
       "\nsexo=", pop_sexo, "\npop=", pop_pop, "\n\n",
       "Colunas disponíveis:\n", paste(pop_cols, collapse = ", "))
}

pop_raw <- pop_ds %>%
  dplyr::select(
    ano   = all_of(pop_ano),
    muni  = all_of(pop_muni),
    idade = all_of(pop_idade),
    sexo  = all_of(pop_sexo),
    pop   = all_of(pop_pop)
  ) %>%
  dplyr::collect()

pop_clean <- pop_raw %>%
  dplyr::mutate(
    ano   = as.integer(ano),
    code_muni6 = muni6_from_any(muni),
    idade = as.integer(idade),
    idade = dplyr::if_else(is.na(idade), NA_integer_, pmin(pmax(idade, 0L), 100L)),
    sexo  = norm_sexo(sexo),
    pop   = as.numeric(pop)
  ) %>%
  dplyr::filter(!is.na(ano), !is.na(code_muni6), !is.na(idade), !is.na(sexo)) %>%
  dplyr::filter(sexo %in% c("m","f")) %>%
  dplyr::group_by(ano, code_muni6, idade, sexo) %>%
  dplyr::summarise(pop_ambos = sum(pop, na.rm = TRUE), .groups = "drop")

# cria sexo="b" como soma m+f (compatível com seu 00_prep antigo)
pop_both <- pop_clean %>%
  dplyr::group_by(ano, code_muni6, idade) %>%
  dplyr::summarise(pop_ambos = sum(pop_ambos, na.rm = TRUE), .groups = "drop") %>%
  dplyr::mutate(sexo = "b")

pop_all <- dplyr::bind_rows(pop_clean, pop_both) %>%
  dplyr::arrange(ano, code_muni6, sexo, idade)

# =========================
# 4) CARREGAR ÓBITOS
# =========================
message("[00_prep] Lendo ÓBITOS: ", OBITOS_INPUT)
ob_ds <- read_arrow_any(OBITOS_INPUT)

ob_cols <- names(ob_ds)
ob_ano   <- pick_col(ob_cols, c("ano","year","exercicio"))
ob_mes   <- pick_col(ob_cols, c("mes","month")) # opcional
ob_muni  <- pick_col(ob_cols, c("code_muni6","code_muni7","code_muni","codmun","codmun_residencia","cod_mun","id_municipio"))
ob_idade <- pick_col(ob_cols, c("idade","age","idade_simples","idade_ano"))
ob_sexo  <- pick_col(ob_cols, c("sexo","sex"))
ob_obit  <- pick_col(ob_cols, c("obitos","D","deaths","obitos_total"))
ob_cov   <- pick_col(ob_cols, c("cobertura_sim","pi_prior","pi_mean")) # opcional

need_ob <- c(ob_ano, ob_muni, ob_idade, ob_sexo, ob_obit)
if (any(is.na(need_ob))) {
  stop("[ÓBITOS] Não consegui identificar colunas necessárias. Achei:\n",
       "ano=", ob_ano, "\nmuni=", ob_muni, "\nidade=", ob_idade,
       "\nsexo=", ob_sexo, "\nobitos=", ob_obit, "\n\n",
       "Colunas disponíveis:\n", paste(ob_cols, collapse = ", "))
}

sel_ob <- c(ob_ano, ob_muni, ob_idade, ob_sexo, ob_obit, ob_cov, ob_mes)
sel_ob <- sel_ob[!is.na(sel_ob)]

ob_raw <- ob_ds %>%
  dplyr::select(all_of(sel_ob)) %>%
  dplyr::collect() %>%
  janitor::clean_names()

# nomes após clean_names
nm <- names(ob_raw)
col_ano   <- pick_col(nm, c("ano","year","exercicio"))
col_mes   <- pick_col(nm, c("mes","month"))
col_muni  <- pick_col(nm, c("code_muni6","code_muni7","code_muni","codmun","codmun_residencia","cod_mun","id_municipio","muni"))
col_idade <- pick_col(nm, c("idade","age","idade_simples","idade_ano"))
col_sexo  <- pick_col(nm, c("sexo","sex"))
col_obit  <- pick_col(nm, c("obitos","d","deaths","obitos_total"))
col_cov2  <- pick_col(nm, c("cobertura_sim","pi_prior","pi_mean"))

ob_clean <- ob_raw %>%
  dplyr::transmute(
    ano   = as.integer(.data[[col_ano]]),
    code_muni6 = muni6_from_any(.data[[col_muni]]),
    idade = as.integer(.data[[col_idade]]),
    idade = dplyr::if_else(is.na(idade), NA_integer_, pmin(pmax(idade, 0L), 100L)),
    sexo  = norm_sexo(.data[[col_sexo]]),
    obitos = as.integer(.data[[col_obit]]),
    cobertura_sim = if (!is.na(col_cov2)) as.numeric(.data[[col_cov2]]) else NA_real_,
    mes = if (!is.na(col_mes)) as.integer(.data[[col_mes]]) else NA_integer_
  ) %>%
  dplyr::filter(!is.na(ano), !is.na(code_muni6), !is.na(idade), !is.na(sexo)) %>%
  dplyr::filter(sexo %in% c("m","f")) %>%
  dplyr::group_by(ano, code_muni6, idade, sexo) %>%
  dplyr::summarise(
    obitos = sum(obitos, na.rm = TRUE),
    cobertura_sim = if (all(is.na(cobertura_sim))) NA_real_ else mean(cobertura_sim, na.rm = TRUE),
    .groups = "drop"
  )

# cria sexo="b" como soma m+f (compatível com seu 00_prep antigo)
ob_both <- ob_clean %>%
  dplyr::group_by(ano, code_muni6, idade) %>%
  dplyr::summarise(
    obitos = sum(obitos, na.rm = TRUE),
    cobertura_sim = if (all(is.na(cobertura_sim))) NA_real_ else mean(cobertura_sim, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  dplyr::mutate(sexo = "b")

ob_all <- dplyr::bind_rows(ob_clean, ob_both) %>%
  dplyr::arrange(ano, code_muni6, sexo, idade)

# se cobertura_sim continuar NA, cria default (não muda nada estrutural)
if (all(is.na(ob_all$cobertura_sim))) {
  ob_all$cobertura_sim <- 0.90
}

# =========================
# 5) LABELS IBGE (município/UF/região + (opcional) RGI)
# =========================
message("[00_prep] Montando labels geográficos (geobr)...")

# município (use UF_FILTER se quiser leve)
muni_sf <- if (is.null(UF_FILTER)) {
  geobr::read_municipality(code_muni = "all", year = 2020)
} else {
  # aceita "SC" etc
  geobr::read_municipality(code_muni = UF_FILTER, year = 2020)
}

muni_labs <- muni_sf %>%
  sf::st_drop_geometry() %>%
  dplyr::transmute(
    code_muni6 = as.integer(substr(code_muni, 1, 6)),
    nome_muni  = name_muni,
    uf_sigla   = abbrev_state,
    nome_uf    = name_state,
    code_state = as.integer(code_state)
  )

# estado -> região
uf_labs <- geobr::read_state(code_state = "all", year = 2020) %>%
  sf::st_drop_geometry() %>%
  dplyr::transmute(
    uf_sigla    = abbrev_state,
    regiao_code = as.integer(code_region),
    regiao_nome = name_region
  )

# (opcional) RGI imediata/intermediária via spatial join (se falhar, fica NA)
build_rgi <- function(muni_sf) {
  out <- tibble(
    code_muni6 = muni_labs$code_muni6,
    rgi_imediata_code = NA_character_,
    rgi_imediata_nome = NA_character_,
    rgi_intermed_code = NA_character_,
    rgi_intermed_nome = NA_character_
  )
  
  try({
    im_sf <- geobr::read_immediate_region(year = 2017)
    it_sf <- geobr::read_intermediate_region(year = 2017)
    
    # centroid do muni para join (rápido e estável)
    pts <- sf::st_centroid(muni_sf)
    
    im_join <- sf::st_join(pts, im_sf, left = TRUE)
    it_join <- sf::st_join(pts, it_sf, left = TRUE)
    
    out$rgi_imediata_code <- as.character(im_join$code_immediate)
    out$rgi_imediata_nome <- as.character(im_join$name_immediate)
    
    out$rgi_intermed_code <- as.character(it_join$code_intermediate)
    out$rgi_intermed_nome <- as.character(it_join$name_intermediate)
  }, silent = TRUE)
  
  out
}

rgi_labs <- build_rgi(muni_sf)

# =========================
# 6) MERGE POP + ÓBITOS  -> base_muni_raw / base_muni
# =========================
message("[00_prep] Fazendo merge pop+óbitos (com sexo)...")

base_muni_raw <- pop_all %>%
  dplyr::full_join(ob_all, by = c("ano","code_muni6","idade","sexo")) %>%
  dplyr::mutate(
    pop_ambos = dplyr::if_else(is.na(pop_ambos), 0, pop_ambos),
    obitos    = dplyr::if_else(is.na(obitos), 0L, obitos),
    cobertura_sim = dplyr::if_else(is.na(cobertura_sim), 0.90, cobertura_sim)
  ) %>%
  dplyr::left_join(muni_labs, by = "code_muni6") %>%
  dplyr::left_join(uf_labs,   by = "uf_sigla") %>%
  dplyr::left_join(rgi_labs,  by = "code_muni6") %>%
  dplyr::mutate(
    mx_raw = dplyr::if_else(pop_ambos > 0, obitos / pop_ambos, NA_real_)
  ) %>%
  dplyr::arrange(ano, uf_sigla, code_muni6, sexo, idade)

# se você quiser filtrar UF aqui (pra não salvar Brasil todo), use UF_FILTER
if (!is.null(UF_FILTER)) {
  base_muni_raw <- base_muni_raw %>% dplyr::filter(uf_sigla %in% UF_FILTER)
}

# base_muni (mantém igual ao raw — a metodologia TOPALS fica no principal)
base_muni <- base_muni_raw

# =========================
# 7) SALVAR
# =========================
save(base_muni_raw, base_muni, file = RDATA_PATH)

message("[00_prep] OK! Salvei: ", RDATA_PATH)
message("[00_prep] Checagem rápida de sexo:")
print(table(base_muni$sexo, useNA = "ifany"))