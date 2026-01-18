###############################################################################
# TOPALS + pi + IBGE – PIPELINE ÚNICO (00b + 01 + 02 + 03 + 05B) — COM SEXO
#
# ✅ Metodologia e aplicação: IGUAL ao seu pipeline atual.
# 🆕 ÚNICA novidade: roda para qualquer sexo ("b","m","f"), lendo:
#   - base_muni com coluna sexo (vinda do 00_prep_topals.R atualizado)
#   - tábua IBGE com sexo (ambos/masc/fem) e usando a âncora correta por sexo
# ✅ Saídas organizadas: .../resultados/UF_ALVO/sexo_{b|m|f}/...
#
# PRÉ-REQUISITO:
#   Rodar o 00_prep_topals.R NOVO (que gera base_muni/base_muni_raw com coluna sexo).
#   Ele deve salvar em: 00_prep_topals_output/bases_topals_preparadas.RData
#   contendo, no mínimo:
#     - base_muni_raw (com sexo)  e base_muni (com sexo)
#     - (opcional) pesos_80mais
#
# COMO USAR:
#   1) Ajuste BASE_DIR, UF_ALVO, SEXO_ALVO, ANOS_FIT etc. e rode o script inteiro.
#   2) Para rodar masculino: SEXO_ALVO <- "m"
#      feminino: SEXO_ALVO <- "f"
#      ambos:    SEXO_ALVO <- "b"
###############################################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(readr)
  library(purrr)
  library(stringr)
  library(tibble)
  library(janitor)
  library(readxl)
  library(splines)
  library(rstan)
  library(ggplot2)
  library(sf)
  library(geobr)
  library(viridis)
  library(grid)
  library(arrow)
})

###############################################################################
# 0) CONFIGURAÇÃO GERAL (SÓ MEXE AQUI)
###############################################################################

BASE_DIR <- "C:/Users/uriel/Documents/UFPB Estatística/Demografia 1/Estudos Demográficos - PB/TOPALS"

UF_ALVO    <- "SP"          # UF que você quer rodar
SEXO_ALVO  <- "m"           # "b" (ambos), "m" (masc), "f" (fem)
ANOS_FIT   <- 2000:2023
NIVEIS_FIT <- "municipio"
ANOS_DIAG  <- c(2000L, 2005L, 2010L, 2015L, 2020L, 2023L)

# Normaliza sexo do usuário (aceita "M/F/B", "masc/fem/ambos")
norm_sexo_user <- function(x) {
  s <- stringr::str_to_lower(stringr::str_trim(as.character(x)))
  dplyr::case_when(
    s %in% c("b","ambos","ambas","both","total") ~ "b",
    s %in% c("m","masc","masculino","male","homens") ~ "m",
    s %in% c("f","fem","feminino","female","mulheres") ~ "f",
    TRUE ~ NA_character_
  )
}
SEXO_ALVO <- norm_sexo_user(SEXO_ALVO)
if (is.na(SEXO_ALVO) || !SEXO_ALVO %in% c("b","m","f")) stop("SEXO_ALVO inválido.")

# Caminhos padrão
OUTPUT_00_DIR <- file.path(BASE_DIR, "00_prep_topals_output")
dir.create(OUTPUT_00_DIR, showWarnings = FALSE, recursive = TRUE)

RDATA_PATH <- file.path(OUTPUT_00_DIR, "bases_topals_preparadas.RData")
if (!file.exists(RDATA_PATH)) {
  stop("Não encontrei ", RDATA_PATH,
       ". Rode antes o 00_prep_topals.R (versão COM sexo).")
}

# Carrega base(s)
load(RDATA_PATH)
# --- FORCE REBUILD tabua_ibge_uf if legacy (no sexo column) ---
if (exists("tabua_ibge_uf") && !("sexo" %in% names(tabua_ibge_uf))) {
  message("[00b] tabua_ibge_uf encontrada SEM coluna 'sexo' (versão antiga). Vou reconstruir do Excel...")
  rm(tabua_ibge_uf)
}

# Diretórios de RESULTADOS padronizados (por UF + sexo)
RESULTS_DIR <- file.path(BASE_DIR, "resultados", UF_ALVO, paste0("sexo_", SEXO_ALVO))
RES_FIG_DIR <- file.path(RESULTS_DIR, "figuras")
RES_DB_DIR  <- file.path(RESULTS_DIR, "bancos_de_dados")
RES_IND_DIR <- file.path(RESULTS_DIR, "indicadores_avancados")

dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(RES_FIG_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(RES_DB_DIR,  showWarnings = FALSE, recursive = TRUE)
dir.create(RES_IND_DIR, showWarnings = FALSE, recursive = TRUE)

###############################################################################
# 0.1) Seleciona base_muni por sexo (sem mudar metodologia)
###############################################################################

if (!exists("base_muni") || !exists("base_muni_raw")) {
  stop("RData não contém base_muni/base_muni_raw. Rode o 00_prep_topals.R novo.")
}

# exige coluna sexo na base (para rodar m/f)
if (SEXO_ALVO != "b") {
  if (!("sexo" %in% names(base_muni)) || !("sexo" %in% names(base_muni_raw))) {
    stop("Sua base_muni/base_muni_raw não tem coluna 'sexo'. Rode o 00_prep_topals.R COM sexo.")
  }
}

# filtra por sexo, mantendo nomes de colunas iguais
base_muni_use <- base_muni
base_muni_raw_use <- base_muni_raw

if ("sexo" %in% names(base_muni_use)) {
  base_muni_use <- base_muni_use %>% dplyr::filter(sexo == SEXO_ALVO)
}
if ("sexo" %in% names(base_muni_raw_use)) {
  base_muni_raw_use <- base_muni_raw_use %>% dplyr::filter(sexo == SEXO_ALVO)
}

# remove coluna sexo pra não “vazar” em joins/agrupamentos (metodologia igual)
if ("sexo" %in% names(base_muni_use)) base_muni_use <- base_muni_use %>% dplyr::select(-sexo)
if ("sexo" %in% names(base_muni_raw_use)) base_muni_raw_use <- base_muni_raw_use %>% dplyr::select(-sexo)

###############################################################################
# 1) 00b_build_tabua_ibge_uf.R  – Construir tabua_ibge_uf (com sexo) e salvar
###############################################################################

# tabela UFs (mantida)
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

norm_sexo_ibge <- function(x) {
  s <- stringr::str_to_lower(stringr::str_trim(as.character(x)))
  dplyr::case_when(
    stringr::str_detect(s, "amb|tot|both") ~ "b",
    stringr::str_detect(s, "masc|hom|male") ~ "m",
    stringr::str_detect(s, "fem|mul|female") ~ "f",
    TRUE ~ NA_character_
  )
}

if (!exists("tabua_ibge_uf")) {
  
  FILE_REF <- file.path(BASE_DIR, "projecoes_2024_tab5_tabuas_mortalidade.xlsx")
  if (!file.exists(FILE_REF)) stop("Arquivo IBGE não encontrado em:\n  ", FILE_REF)
  
  norm_sexo_ibge <- function(x) {
    s <- stringr::str_to_lower(stringr::str_trim(as.character(x)))
    dplyr::case_when(
      stringr::str_detect(s, "amb|tot|both") ~ "b",
      stringr::str_detect(s, "masc|hom|male") ~ "m",
      stringr::str_detect(s, "fem|mul|female") ~ "f",
      s %in% c("1") ~ "m",
      s %in% c("2") ~ "f",
      TRUE ~ NA_character_
    )
  }
  
  infer_sexo_from_sheet <- function(sheet) {
    s <- stringr::str_to_lower(sheet)
    dplyr::case_when(
      stringr::str_detect(s, "masc|hom") ~ "m",
      stringr::str_detect(s, "fem|mul")  ~ "f",
      stringr::str_detect(s, "amb|tot")  ~ "b",
      TRUE ~ NA_character_
    )
  }
  
  pick_col <- function(nm, patterns) {
    hits <- Reduce(`|`, lapply(patterns, function(p) grepl(p, nm, ignore.case = TRUE)))
    pos <- which(hits)
    if (length(pos) == 0L) return(NA_character_)
    nm[pos[1]]
  }
  
  read_one_sheet <- function(sheet) {
    tmp0 <- readxl::read_excel(FILE_REF, sheet = sheet, col_names = FALSE)
    
    header_row_candidates <- which(
      !is.na(tmp0[[1]]) &
        stringr::str_detect(tmp0[[1]], regex("^IDADE$", ignore_case = TRUE))
    )
    if (length(header_row_candidates) == 0L) return(NULL)
    header_row <- header_row_candidates[1]
    
    tab_raw <- readxl::read_excel(FILE_REF, sheet = sheet, skip = header_row - 1L, col_names = TRUE) |>
      janitor::clean_names()
    
    nm <- names(tab_raw)
    
    col_ano   <- pick_col(nm, c("^ano$"))
    col_uf    <- pick_col(nm, c("^sigla$", "^sigla_uf$", "^uf$"))
    col_idade <- pick_col(nm, c("^idade$"))
    col_sexo  <- pick_col(nm, c("^sexo$"))
    
    # mx candidates (sometimes multiple columns)
    mx_cols <- nm[grepl("(^nmx$|nmx|mx)", nm, ignore.case = TRUE)]
    
    if (is.na(col_ano) || is.na(col_uf) || is.na(col_idade) || length(mx_cols) == 0L) return(NULL)
    
    sexo_sheet <- infer_sexo_from_sheet(sheet)
    
    out <- tab_raw |>
      dplyr::transmute(
        uf_sigla = as.character(.data[[col_uf]]),
        ano      = as.integer(.data[[col_ano]]),
        idade    = as.integer(.data[[col_idade]]),
        sexo_raw = if (!is.na(col_sexo)) .data[[col_sexo]] else NA,
        dplyr::across(dplyr::all_of(mx_cols), as.numeric)
      )
    
    if (!is.na(col_sexo)) {
      # Strategy 1: explicit sexo column
      out <- out |>
        dplyr::mutate(sexo = norm_sexo_ibge(sexo_raw)) |>
        dplyr::select(-sexo_raw)
      
      mx_col_use <- pick_col(names(out), c("^nmx$", "nmx", "mx"))
      if (is.na(mx_col_use)) return(NULL)
      
      out <- out |>
        dplyr::rename(mx_ibge = dplyr::all_of(mx_col_use)) |>
        dplyr::filter(sexo %in% c("b","m","f"))
      
    } else if (!is.na(sexo_sheet)) {
      # Strategy 2: sexo inferred from sheet name (one mx column)
      mx_col_use <- mx_cols[1]
      out <- out |>
        dplyr::mutate(sexo = sexo_sheet, mx_ibge = .data[[mx_col_use]]) |>
        dplyr::select(uf_sigla, ano, idade, sexo, mx_ibge)
      
    } else {
      # Strategy 3: multiple mx columns -> pivot_longer and infer sexo by column name
      out <- out |>
        tidyr::pivot_longer(
          cols = dplyr::all_of(mx_cols),
          names_to = "mx_name",
          values_to = "mx_ibge"
        ) |>
        dplyr::mutate(sexo = dplyr::case_when(
          stringr::str_detect(mx_name, "masc|hom") ~ "m",
          stringr::str_detect(mx_name, "fem|mul")  ~ "f",
          stringr::str_detect(mx_name, "amb|tot|total") ~ "b",
          TRUE ~ NA_character_
        )) |>
        dplyr::filter(sexo %in% c("b","m","f")) |>
        dplyr::select(uf_sigla, ano, idade, sexo, mx_ibge)
    }
    
    out |>
      dplyr::filter(!is.na(ano), !is.na(idade), !is.na(mx_ibge)) |>
      dplyr::filter(ano >= 2000, ano <= 2070)
  }
  
  sheets <- readxl::excel_sheets(FILE_REF)
  tab_list <- purrr::map(sheets, read_one_sheet)
  tab_list <- tab_list[!vapply(tab_list, is.null, logical(1))]
  
  if (length(tab_list) == 0L) stop("Não consegui extrair nenhuma tábua do Excel. Confira o arquivo/sheet.")
  
  tabua_ibge_uf <- dplyr::bind_rows(tab_list) |>
    dplyr::mutate(
      uf_sigla = as.character(uf_sigla),
      sexo    = as.character(sexo),
      ano     = as.integer(ano),
      idade   = as.integer(idade),
      mx_ibge = as.numeric(mx_ibge)
    ) |>
    dplyr::filter(sexo %in% c("b","m","f")) |>
    dplyr::arrange(uf_sigla, sexo, ano, idade)
  
  # salva no RData
  objs_to_save <- c("base_muni_raw","base_muni","tabua_ibge_uf")
  if (exists("pesos_80mais")) objs_to_save <- c(objs_to_save, "pesos_80mais")
  save(list = objs_to_save, file = RDATA_PATH)
  
  message("[00b] tabua_ibge_uf reconstruída e salva com coluna sexo em: ", RDATA_PATH)
}

# Recarrega para garantir tudo em memória
load(RDATA_PATH)

###############################################################################
# 2) Funções auxiliares globais (TOPALS + IBGE)
###############################################################################

# Base de splines em idade
make_B_matrix <- function(ages = 0:100) {
  splines::bs(ages, knots = c(0, 1, 10, 20, 40, 70), degree = 1)
}

# e0 a partir de log(mx)
e0_from_logmx <- function(logmx) {
  mx <- exp(logmx)
  px <- exp(-mx)
  lx <- c(1, cumprod(px))
  sum(head(lx, -1) + tail(lx, -1)) / 2
}

# ex(a) a partir de log(mx)
ex_from_logmx <- function(logmx) {
  A  <- length(logmx)
  mx <- exp(logmx)
  px <- exp(-mx)
  
  lx <- numeric(A + 1L)
  lx[1] <- 1
  for (a in 1:A) lx[a + 1] <- lx[a] * px[a]
  
  Lx <- 0.5 * (lx[1:A] + lx[2:(A + 1L)])
  Tx <- rev(cumsum(rev(Lx)))
  ex <- Tx / lx[1:A]
  ex
}

# std_schedule (log mx IBGE UF-ano-SEXO, suavizado 0:100)
make_std_schedule_ibge <- function(uf, ano_std, sexo_std = SEXO_ALVO, ages = 0:100) {
  
  df_uf <- tabua_ibge_uf %>%
    dplyr::filter(uf_sigla == uf, sexo == sexo_std, ano == ano_std) %>%
    dplyr::arrange(idade)
  
  if (nrow(df_uf) == 0L) {
    stop("Sem dados em tabua_ibge_uf para uf=", uf, ", ano=", ano_std, ", sexo=", sexo_std)
  }
  
  ages_src   <- df_uf$idade
  log_mx_src <- log(df_uf$mx_ibge)
  
  log_mx_interp <- approx(x = ages_src, y = log_mx_src, xout = ages, rule = 2)$y
  
  B_std <- make_B_matrix(ages = ages)
  Proj  <- B_std %*% solve(crossprod(B_std)) %*% t(B_std)
  
  as.vector(Proj %*% log_mx_interp)
}

ages_vec <- 0:100

e0_ibge_from_tabua <- function(ano_target, uf = UF_ALVO, sexo_std = SEXO_ALVO) {
  df <- tabua_ibge_uf %>%
    dplyr::filter(uf_sigla == uf, sexo == sexo_std, ano == ano_target) %>%
    dplyr::arrange(idade)
  
  if (nrow(df) == 0L) stop("Sem tábua IBGE para uf=", uf, ", ano=", ano_target, ", sexo=", sexo_std)
  
  log_mx_interp <- approx(x = df$idade, y = log(df$mx_ibge), xout = ages_vec, rule = 2)$y
  e0_from_logmx(log_mx_interp)
}

e60_ibge_from_tabua <- function(ano_target, uf = UF_ALVO, sexo_std = SEXO_ALVO, idade_ref = 60L) {
  df <- tabua_ibge_uf %>%
    dplyr::filter(uf_sigla == uf, sexo == sexo_std, ano == ano_target) %>%
    dplyr::arrange(idade)
  
  if (nrow(df) == 0L) stop("Sem tábua IBGE para uf=", uf, ", ano=", ano_target, ", sexo=", sexo_std)
  
  log_mx_interp <- approx(x = df$idade, y = log(df$mx_ibge), xout = ages_vec, rule = 2)$y
  ex_vec <- ex_from_logmx(log_mx_interp)
  ex_vec[which.min(abs(ages_vec - idade_ref))]
}

###############################################################################
# 3) Modelo Stan – TOPALS + pi + âncora em e0 (IBGE)
###############################################################################

stanModelText_pi <- "
functions {
  real e0_from_logmx(vector logmx) {
    int A = num_elements(logmx);
    vector[A] mx;
    vector[A] px;
    vector[A + 1] lx;
    real e0;

    for (a in 1:A) {
      mx[a] = exp(logmx[a]);
      px[a] = exp(-mx[a]);
    }

    lx[1] = 1;
    for (a in 1:A) {
      lx[a + 1] = lx[a] * px[a];
    }

    e0 = 0;
    for (a in 1:A) {
      e0 += 0.5 * (lx[a] + lx[a + 1]);
    }
    return e0;
  }
}

data {
  int<lower=1> R;
  int<lower=1> S;
  int<lower=1> A;
  int<lower=1> K;

  matrix[A,K] B;
  vector[A]   std_schedule;

  matrix<lower=0>[A,R] N;
  int<lower=0>          D[A,R];

  vector[R] pi_prior_mean;
  real<lower=0> sigma_logit_pi;

  real e0_target;
  real<lower=0> sigma_e0_target;
}

parameters {
  matrix[K,R] alpha;
  vector[R]   logit_pi_raw;
}

transformed parameters {
  matrix[A,R] lambda;
  vector[R]   pi;

  for (r in 1:R) {
    pi[r] = inv_logit(logit_pi_raw[r]);
  }

  lambda = rep_matrix(std_schedule, R) + B * alpha;
}

model {
  matrix[K-1,R] alpha_diff;
  vector[R] logit_pi_prior;

  for (r in 1:R) {
    logit_pi_prior[r] = logit(pi_prior_mean[r]);
  }

  alpha_diff = alpha[2:K, ] - alpha[1:(K-1), ];

  to_vector(alpha)      ~ normal(0, 4);
  to_vector(alpha_diff) ~ normal(0, 1 / sqrt(2));

  logit_pi_raw ~ normal(logit_pi_prior, sigma_logit_pi);

  for (r in 1:R) {
    D[, r] ~ poisson_log( log(N[, r]) + log(pi[r]) + lambda[, r] );
  }

  {
    vector[R] e0_reg;
    real e0_mean;

    for (r in 1:R) {
      e0_reg[r] = e0_from_logmx( lambda[, r] );
    }
    e0_mean = mean(e0_reg);

    target += normal_lpdf(e0_mean | e0_target, sigma_e0_target);
  }
}

generated quantities {
}
"

message("\nCompilando modelo Stan (TOPALS + pi + IBGE + âncora e0) ...")
topals_pi_model <- rstan::stan_model(model_code = stanModelText_pi)

rstan::rstan_options(auto_write = TRUE)
options(mc.cores = max(1L, parallel::detectCores() - 1L))

N_CHAINS <- 4L
N_ITER   <- 1200L
N_WARMUP <- 200L

###############################################################################
# 4) Funções auxiliares para um caso (ano x nível x UF) — COM SEXO (via base_muni_use)
###############################################################################

pick_col <- function(cols, cands) {
  for (cc in cands) if (cc %in% cols) return(cc)
  NA_character_
}

get_pop_col <- function(df) {
  cols <- names(df)
  # base_muni_use já foi filtrada por sexo; então qualquer "pop_*" serve, mas mantemos prioridade
  pick_col(cols, c("pop_ambos","pop","pop_total","N","populacao","pop_final","pop_corrigida"))
}

get_obitos_col <- function(df) {
  cols <- names(df)
  pick_col(cols, c("obitos","D","obitos_total","deaths"))
}

build_stan_data_case <- function(base_muni_df,
                                 ano_target,
                                 nivel = c("municipio", "imediata", "intermediaria"),
                                 uf,
                                 sexo_std = SEXO_ALVO,
                                 ages = 0:100) {
  
  nivel <- match.arg(nivel)
  
  df <- base_muni_df %>%
    dplyr::filter(ano == ano_target, uf_sigla == uf)
  
  if (nrow(df) == 0L) stop("Nenhum dado para ano=", ano_target, " e UF=", uf)
  
  pop_col    <- get_pop_col(df)
  obitos_col <- get_obitos_col(df)
  if (is.na(pop_col)) stop("Não encontrei coluna de população na base (ex.: pop_ambos/pop).")
  if (is.na(obitos_col)) stop("Não encontrei coluna de óbitos na base (ex.: obitos).")
  
  region_var <- switch(
    nivel,
    "municipio"     = "code_muni6",
    "imediata"      = "rgi_imediata_code",
    "intermediaria" = "rgi_intermed_code"
  )
  
  df_agg <- df %>%
    dplyr::filter(!is.na(.data[[region_var]])) %>%
    dplyr::group_by(.data[[region_var]], idade) %>%
    dplyr::summarise(
      D = sum(.data[[obitos_col]], na.rm = TRUE),
      N = sum(.data[[pop_col]],    na.rm = TRUE),
      .groups = "drop"
    )
  
  if (nrow(df_agg) == 0L) stop("Nenhum dado agregado para ano=", ano_target, ", nível=", nivel)
  
  regions <- sort(unique(df_agg[[region_var]]))
  R <- length(regions)
  A <- length(ages)
  
  D_mat <- matrix(0L, nrow = A, ncol = R)
  N_mat <- matrix(0,  nrow = A, ncol = R)
  
  for (j in seq_along(regions)) {
    sub <- df_agg %>%
      dplyr::filter(.data[[region_var]] == regions[j]) %>%
      dplyr::select(idade, D, N)
    
    sub_full <- tibble::tibble(idade = ages) %>%
      dplyr::left_join(sub, by = "idade") %>%
      tidyr::replace_na(list(D = 0L, N = 0)) %>%
      dplyr::arrange(idade)
    
    D_mat[, j] <- as.integer(sub_full$D)
    N_mat[, j] <- sub_full$N
  }
  
  B_age <- make_B_matrix(ages = ages)
  
  std_logmx <- make_std_schedule_ibge(
    uf       = uf,
    ano_std  = ano_target,
    sexo_std = sexo_std,
    ages     = ages
  )
  
  e0_target <- e0_from_logmx(std_logmx)
  
  cov_prior <- df %>% dplyr::distinct(cobertura_sim) %>% dplyr::pull()
  if (length(cov_prior) == 0L || all(is.na(cov_prior))) {
    warning("cobertura_sim ausente/NA; usando prior pi≈0.9 com logit sd 0.7.")
    cov_prior <- 0.9
  } else {
    cov_prior <- mean(cov_prior, na.rm = TRUE)
  }
  cov_prior <- cov_prior / ifelse(cov_prior > 1.5, 100, 1)
  cov_prior <- min(max(cov_prior, 0.01), 0.99)
  
  stanDataList <- list(
    R = R,
    S = 1L,
    A = A,
    K = ncol(B_age),
    B = B_age,
    std_schedule    = std_logmx,
    N               = pmax(N_mat, 0.01),
    D               = D_mat,
    pi_prior_mean   = rep(cov_prior, R),
    sigma_logit_pi  = 0.7,
    e0_target       = e0_target,
    sigma_e0_target = 1.0
  )
  
  list(stanData = stanDataList, regions = regions, ages = ages)
}

fit_one_case <- function(this_ano,
                         this_nivel,
                         uf        = UF_ALVO,
                         sexos     = SEXO_ALVO,
                         out_dir,
                         nchains   = N_CHAINS,
                         niter     = N_ITER,
                         warmup    = N_WARMUP) {
  
  message("Rodando TOPALS+pi+IBGE | ano=", this_ano,
          " | nível=", this_nivel,
          " | UF=", uf,
          " | sexo=", sexos)
  
  ages_vec <- 0:100
  
  stan_case <- build_stan_data_case(
    base_muni_df = base_muni_use,
    ano_target   = this_ano,
    nivel        = this_nivel,
    uf           = uf,
    sexo_std     = sexos,
    ages         = ages_vec
  )
  
  stanDataList <- stan_case$stanData
  R <- stanDataList$R
  if (R == 0L) {
    warning("Nenhuma região para ano=", this_ano, ", nível=", this_nivel, ".")
    return(NULL)
  }
  
  fit <- rstan::sampling(
    object  = topals_pi_model,
    data    = stanDataList,
    seed    = 6447100 + this_ano + ifelse(sexos == "m", 10, ifelse(sexos == "f", 20, 0)),
    iter    = niter,
    warmup  = warmup,
    chains  = nchains,
    control = list(max_treedepth = 12),
    thin    = 1
  )
  
  case_meta <- list(
    ano     = this_ano,
    nivel   = this_nivel,
    sexo    = sexos,
    uf      = uf,
    regiao  = NA_character_,
    regions = stan_case$regions,
    ages    = stan_case$ages
  )
  
  time_stamp <- format(Sys.time(), "%Y%m%d-%H%M%S")
  area_label <- paste0("UF", uf)
  
  fitfile <- file.path(
    out_dir,
    paste0(
      "topals_pi_ibge_",
      area_label, "_",
      this_nivel, "_",
      "sex", sexos, "_",
      this_ano, "_",
      time_stamp,
      ".RData"
    )
  )
  
  tmpfile <- paste0(fitfile, ".tmp")
  save(fit, stanDataList, case_meta, file = tmpfile)
  ok <- file.rename(tmpfile, fitfile)
  if (!ok) stop("Falhou renomear tmp -> fitfile (possível lock/sync/antivírus): ", fitfile)
  
  tibble::tibble(
    ano       = this_ano,
    nivel     = this_nivel,
    sexo      = sexos,
    uf        = uf,
    regiao    = NA_character_,
    n_regioes = length(stan_case$regions),
    fitfile   = fitfile
  )
}

###############################################################################
# 5) 01_fit_topals_pi_ibge – Ajustar modelos para UF_ALVO e ANOS_FIT
###############################################################################

TOPALS_FIT_DIR <- file.path(
  BASE_DIR,
  "01_topals_fits_pi_ibge",
  sprintf("%s_%s_municipio_imediata_pi_ibge", UF_ALVO, paste0("sex", SEXO_ALVO))
)
dir.create(TOPALS_FIT_DIR, showWarnings = FALSE, recursive = TRUE)

# Reconstrói resumo de fits existentes (se houver)
fitfiles <- list.files(TOPALS_FIT_DIR, pattern = "^topals_pi_ibge_.*\\.RData$", full.names = TRUE)

rebuild_one_fit_safe <- function(ff) {
  tryCatch({
    env <- new.env()
    load(ff, envir = env)
    if (!exists("case_meta", envir = env)) stop("Objeto 'case_meta' não encontrado.")
    cm <- env$case_meta
    tibble(
      ano       = cm$ano,
      nivel     = cm$nivel,
      sexo      = cm$sexo,
      uf        = cm$uf,
      regiao    = cm$regiao,
      n_regioes = length(cm$regions),
      fitfile   = ff
    )
  }, error = function(e) {
    message("⚠️ Pulando arquivo com erro: ", ff, " -> ", conditionMessage(e))
    NULL
  })
}

if (length(fitfiles) == 0L) {
  results <- tibble::tibble(
    ano       = integer(),
    nivel     = character(),
    sexo      = character(),
    uf        = character(),
    regiao    = character(),
    n_regioes = integer(),
    fitfile   = character()
  )
} else {
  results <- purrr::map_dfr(fitfiles, rebuild_one_fit_safe)
  if (nrow(results) == 0L) {
    results <- tibble::tibble(
      ano       = integer(),
      nivel     = character(),
      sexo      = character(),
      uf        = character(),
      regiao    = character(),
      n_regioes = integer(),
      fitfile   = character()
    )
  } else {
    results <- results %>% dplyr::arrange(ano, nivel)
  }
}

# Salva resumo (também em RESULTS)
resumo_fits_parquet_fitdir <- file.path(
  TOPALS_FIT_DIR,
  sprintf("resumo_fits_%s_sex%s_municipio_imediata_pi_ibge.parquet", UF_ALVO, SEXO_ALVO)
)
resumo_fits_rds_fitdir <- file.path(
  TOPALS_FIT_DIR,
  sprintf("resumo_fits_%s_sex%s_municipio_imediata_pi_ibge.rds", UF_ALVO, SEXO_ALVO)
)
arrow::write_parquet(results, resumo_fits_parquet_fitdir)
saveRDS(results, resumo_fits_rds_fitdir)

# Rodar casos ANOS_FIT x NIVEIS_FIT (mesma lógica)
cases <- tidyr::expand_grid(ano = ANOS_FIT, nivel = NIVEIS_FIT) %>% dplyr::arrange(ano, nivel)

message("\n===== [01] Rodando ", nrow(cases),
        " casos TOPALS+pi+IBGE para UF = ", UF_ALVO,
        " | sexo = ", SEXO_ALVO, " =====")

results_new <- tibble::tibble(
  ano       = integer(),
  nivel     = character(),
  sexo      = character(),
  uf        = character(),
  regiao    = character(),
  n_regioes = integer(),
  fitfile   = character()
)

for (i in seq_len(nrow(cases))) {
  row <- cases[i, ]
  res_i <- tryCatch(
    fit_one_case(
      this_ano   = row$ano,
      this_nivel = row$nivel,
      uf         = UF_ALVO,
      sexos      = SEXO_ALVO,
      out_dir    = TOPALS_FIT_DIR
    ),
    error = function(e) {
      message("❌ ERRO no caso ano=", row$ano,
              " nivel=", row$nivel,
              " UF=", UF_ALVO,
              " sexo=", SEXO_ALVO,
              "\n   -> ", conditionMessage(e))
      NULL
    }
  )
  
  if (is.null(res_i)) next
  results_new <- dplyr::bind_rows(results_new, res_i)
  
}

# Salva resumo final em RESULTS (bancos_de_dados)
res_pb_parquet_path <- file.path(
  RES_DB_DIR,
  sprintf("resumo_fits_%s_sex%s_municipio_imediata_pi_ibge.parquet", UF_ALVO, SEXO_ALVO)
)
res_pb_rds_path <- file.path(
  RES_DB_DIR,
  sprintf("resumo_fits_%s_sex%s_municipio_imediata_pi_ibge.rds", UF_ALVO, SEXO_ALVO)
)
arrow::write_parquet(results_new, res_pb_parquet_path)
saveRDS(results_new, res_pb_rds_path)

message("Resumo dos fits salvo em:")
message(" - ", res_pb_parquet_path)
message(" - ", res_pb_rds_path)

###############################################################################
# 6) 02_e0_pi_from_topals_ibge – Extrair e0/pi para UF_ALVO (sexo fixo)
###############################################################################

message("\n===== [02] Extraindo e0/pi dos fits TOPALS+pi+IBGE | sexo=", SEXO_ALVO, " =====")

if (!file.exists(res_pb_parquet_path)) stop("Não encontrei resumo dos fits: ", res_pb_parquet_path)

res_pb <- arrow::read_parquet(res_pb_parquet_path) |>
  dplyr::mutate(
    ano   = as.integer(ano),
    nivel = as.character(nivel),
    uf    = as.character(uf),
    sexo  = as.character(sexo)
  )

valid_cases <- res_pb %>%
  dplyr::filter(
    sexo == SEXO_ALVO,
    uf == UF_ALVO,
    nivel %in% c("municipio", "imediata"),
    !is.na(fitfile),
    file.exists(fitfile)
  ) %>%
  dplyr::arrange(ano, nivel)

if (nrow(valid_cases) == 0L) stop("Nenhum caso válido encontrado (UF/sexo).")

compute_e0_pi_case <- function(row_case) {
  row_case <- as.list(row_case)
  
  fitfile    <- row_case$fitfile
  this_ano   <- row_case$ano
  this_nivel <- row_case$nivel
  this_sexo  <- row_case$sexo
  this_uf    <- row_case$uf
  
  message("Calculando e0 & pi | ano=", this_ano,
          " | nivel=", this_nivel,
          " | sexo=", this_sexo,
          " | UF=", this_uf,
          "\n  Arquivo: ", fitfile)
  
  load(fitfile)  # fit, stanDataList, case_meta
  
  post_alpha <- rstan::extract(fit, pars = "alpha")$alpha
  dims <- dim(post_alpha)
  K <- dims[2]; R <- dims[3]
  post_pi <- rstan::extract(fit, pars = "pi")$pi
  
  B            <- stanDataList$B
  std_schedule <- stanDataList$std_schedule
  ages         <- case_meta$ages
  regions      <- case_meta$regions
  
  df_label <- base_muni_use %>%
    dplyr::filter(ano == this_ano, uf_sigla == this_uf) %>%
    dplyr::distinct(
      code_muni6, nome_muni,
      uf_sigla, nome_uf,
      regiao_code, regiao_nome,
      rgi_imediata_code, rgi_imediata_nome
    )
  
  region_var <- dplyr::case_when(
    this_nivel == "municipio"  ~ "code_muni6",
    this_nivel == "imediata"   ~ "rgi_imediata_code",
    TRUE                       ~ "code_muni6"
  )
  
  out_list <- vector("list", R)
  
  for (j in seq_len(R)) {
    alpha_j <- post_alpha[, , j]
    
    L <- B %*% t(alpha_j)
    L <- sweep(L, 1, std_schedule, "+")
    
    e0_s <- apply(L, 2, e0_from_logmx)
    pi_s <- post_pi[, j]
    
    e0_qs <- as.numeric(quantile(e0_s, probs = c(0.10, 0.50, 0.90), na.rm = TRUE))
    pi_qs <- as.numeric(quantile(pi_s, probs = c(0.10, 0.50, 0.90), na.rm = TRUE))
    
    code_j <- regions[j]
    
    lab_j <- df_label %>%
      dplyr::filter(.data[[region_var]] == code_j) %>%
      dplyr::slice(1)
    
    if (nrow(lab_j) == 0L) {
      lab_j <- tibble(
        uf_sigla          = this_uf,
        nome_uf           = NA_character_,
        regiao_code       = NA_integer_,
        regiao_nome       = NA_character_,
        code_muni6        = if (this_nivel == "municipio") as.integer(code_j) else NA_integer_,
        nome_muni         = NA_character_,
        rgi_imediata_code = if (this_nivel == "imediata") as.character(code_j) else NA_character_,
        rgi_imediata_nome = NA_character_
      )
    }
    
    out_list[[j]] <- tibble(
      ano         = this_ano,
      nivel       = this_nivel,
      sexo        = this_sexo,
      uf          = this_uf,
      uf_sigla    = lab_j$uf_sigla[1],
      nome_uf     = lab_j$nome_uf[1],
      regiao_code = lab_j$regiao_code[1],
      regiao_nome = lab_j$regiao_nome[1],
      code_muni6  = lab_j$code_muni6[1],
      nome_muni   = lab_j$nome_muni[1],
      rgi_imediata_code = lab_j$rgi_imediata_code[1],
      rgi_imediata_nome = lab_j$rgi_imediata_nome[1],
      region_code = as.character(code_j),
      region_name = dplyr::case_when(
        this_nivel == "municipio" ~ lab_j$nome_muni[1],
        this_nivel == "imediata"  ~ lab_j$rgi_imediata_nome[1],
        TRUE ~ NA_character_
      ),
      e0_p10 = e0_qs[1],
      e0_p50 = e0_qs[2],
      e0_p90 = e0_qs[3],
      pi_p10 = pi_qs[1],
      pi_p50 = pi_qs[2],
      pi_p90 = pi_qs[3]
    )
  }
  
  dplyr::bind_rows(out_list)
}

e0_pi_all <- purrr::map_dfr(seq_len(nrow(valid_cases)), ~ compute_e0_pi_case(valid_cases[.x, ]))

# Saídas
e0_pi_all_path  <- file.path(RES_DB_DIR, sprintf("e0_pi_regioes_municipio_imediata_sex%s_%s_2000_2023_ibge.parquet", SEXO_ALVO, UF_ALVO))
e0_pi_muni_path <- file.path(RES_DB_DIR, sprintf("e0_pi_municipios_sex%s_%s_2000_2023_ibge.parquet", SEXO_ALVO, UF_ALVO))
e0_pi_imed_path <- file.path(RES_DB_DIR, sprintf("e0_pi_rgi_imediata_sex%s_%s_2000_2023_ibge.parquet", SEXO_ALVO, UF_ALVO))

arrow::write_parquet(e0_pi_all, e0_pi_all_path)
arrow::write_parquet(e0_pi_all %>% dplyr::filter(nivel == "municipio"), e0_pi_muni_path)
arrow::write_parquet(e0_pi_all %>% dplyr::filter(nivel == "imediata"),  e0_pi_imed_path)

message("Arquivos e0/pi salvos em PARQUET:")
message(" - ", e0_pi_all_path)
message(" - ", e0_pi_muni_path)
message(" - ", e0_pi_imed_path)

###############################################################################
# 7) 03_diag_mx_topals_ibge – reconstruct_mx_muni (usado no shrink)
###############################################################################

message("\n===== [03] Definindo reconstruct_mx_muni | sexo=", SEXO_ALVO, " =====")

# Reusa res_pb (resumo) para localizar fit por ano
res_pb_rds_path <- file.path(RES_DB_DIR, sprintf("resumo_fits_%s_sex%s_municipio_imediata_pi_ibge.rds", UF_ALVO, SEXO_ALVO))
if (!file.exists(res_pb_rds_path)) stop("Não achei resumo fits RDS: ", res_pb_rds_path)
res_pb_local <- readRDS(res_pb_rds_path)

reconstruct_mx_muni <- function(ano_target, code_muni6_target) {
  
  row_case <- res_pb_local %>%
    dplyr::filter(ano == ano_target, nivel == "municipio", uf == UF_ALVO, sexo == SEXO_ALVO) %>%
    dplyr::slice(1)
  
  if (nrow(row_case) == 0L) stop("Não achei fit para ano=", ano_target, " UF=", UF_ALVO, " sexo=", SEXO_ALVO)
  
  fitfile <- row_case$fitfile
  if (!file.exists(fitfile)) stop("fitfile não existe: ", fitfile)
  
  load(fitfile)  # fit, stanDataList, case_meta
  
  post_alpha <- rstan::extract(fit, pars = "alpha")$alpha  # S x K x R
  B          <- stanDataList$B
  std_sched  <- stanDataList$std_schedule
  N_mat      <- stanDataList$N
  D_mat      <- stanDataList$D
  ages       <- case_meta$ages
  regions    <- case_meta$regions
  
  j <- which(regions == code_muni6_target)
  if (length(j) == 0L) stop("Municipio code_muni6=", code_muni6_target, " não encontrado em case_meta$regions.")
  if (length(j) > 1L) j <- j[1]
  
  alpha_j <- post_alpha[, , j]
  
  lambda_samples <- B %*% t(alpha_j)
  lambda_samples <- sweep(lambda_samples, 1, std_sched, "+")
  
  logmx_topals_med <- apply(lambda_samples, 1, median)
  mx_topals_med    <- exp(logmx_topals_med)
  
  mx_ibge <- exp(std_sched)
  
  N_j <- N_mat[, j]
  D_j <- D_mat[, j]
  mx_raw <- ifelse(N_j > 0, D_j / N_j, NA_real_)
  
  e0_ibge   <- e0_from_logmx(log(mx_ibge))
  e0_raw    <- e0_from_logmx(log(ifelse(is.na(mx_raw), 1e-9, pmax(mx_raw, 1e-9))))
  e0_topals <- e0_from_logmx(log(mx_topals_med))
  
  schedules <- tibble(
    ano        = ano_target,
    code_muni6 = code_muni6_target,
    idade      = ages,
    mx_ibge    = mx_ibge,
    mx_raw     = mx_raw,
    mx_topals  = mx_topals_med
  )
  
  e0_summary <- tibble(
    ano        = ano_target,
    code_muni6 = code_muni6_target,
    e0_ibge    = e0_ibge,
    e0_raw     = e0_raw,
    e0_topals  = e0_topals
  )
  
  list(schedules = schedules, e0_summary = e0_summary)
}

###############################################################################
# 8) 05B_mx_post_e0 – Shrink ex-post + NMX final + mapas + tabela de vida
###############################################################################

message("\n===== [05B] Shrink ex-post do e0 + NMX final + mapas e0/e60 | sexo=", SEXO_ALVO, " =====")

e0_muni_path <- e0_pi_muni_path
e0_muni <- arrow::read_parquet(e0_muni_path)
anos_alvo <- sort(unique(e0_muni$ano))

# e0 alvo do IBGE (UF_ALVO + SEXO_ALVO)
tab_e0_ibge <- tibble(
  ano     = anos_alvo,
  e0_ibge = purrr::map_dbl(anos_alvo, ~ e0_ibge_from_tabua(.x, UF_ALVO, SEXO_ALVO))
)

e0_muni_base <- e0_muni %>%
  dplyr::left_join(tab_e0_ibge, by = "ano") %>%
  dplyr::mutate(delta_e0 = e0_p50 - e0_ibge)

# Diretórios da etapa 05B (já dentro de UF/sexo)
FIG_DIR_05B <- file.path(RES_FIG_DIR, "05B_mx_post_e0")
DB_DIR_05B  <- file.path(RES_DB_DIR,  "05B_mx_post_e0")
IND_DIR_05B <- file.path(RES_IND_DIR, "05B_mx_post_e0")

dir.create(FIG_DIR_05B, showWarnings = FALSE, recursive = TRUE)
dir.create(DB_DIR_05B,  showWarnings = FALSE, recursive = TRUE)
dir.create(IND_DIR_05B, showWarnings = FALSE, recursive = TRUE)

# Função de e0 a partir de mx (igual à sua)
calc_e0_from_mx_tbl <- function(mx_tbl, mx_col = "mx_topals") {
  df <- mx_tbl %>% dplyr::arrange(idade)
  idade <- df$idade
  mx    <- df[[mx_col]]
  
  n <- c(diff(idade), 1)
  ax <- rep(0.5, length(mx))
  if (length(ax) > 0) ax[1] <- 0.3
  
  qx <- (n * mx) / (1 + (n - ax) * mx)
  qx[length(qx)] <- 1.0
  
  lx <- numeric(length(mx))
  lx[1] <- 100000
  for (i in seq_len(length(mx) - 1)) lx[i + 1] <- lx[i] * (1 - qx[i])
  
  dx <- lx * qx
  
  Lx <- numeric(length(mx))
  if (length(mx) > 1) {
    Lx[1:(length(mx) - 1)] <- n[1:(length(mx) - 1)] * lx[2:length(mx)] + ax[1:(length(mx) - 1)] * dx[1:(length(mx) - 1)]
  }
  Lx[length(mx)] <- lx[length(mx)] / mx[length(mx)]
  
  Tx <- rev(cumsum(rev(Lx)))
  ex <- Tx / lx
  ex[1]
}

delta_stats <- e0_muni_base %>%
  dplyr::group_by(ano) %>%
  dplyr::summarise(delta_med = median(delta_e0, na.rm = TRUE), .groups = "drop") %>%
  dplyr::mutate(
    escala_ano = dplyr::case_when(
      abs(delta_med) <= 1 ~ 1,
      TRUE ~ pmax(0.25, 1 / abs(delta_med))
    )
  )

get_escala_ano <- function(ano) {
  linha <- delta_stats[delta_stats$ano == ano, ]
  if (nrow(linha) == 0) return(1)
  linha$escala_ano[[1]]
}

year_shrink_factor <- function(ano) {
  dplyr::case_when(
    ano <= 2005 ~ 1.0,
    ano == 2010 ~ 0.9,
    ano == 2015 ~ 0.7,
    ano == 2020 ~ 0.6,
    ano >= 2023 ~ 0.5,
    TRUE ~ 0.6
  )
}

compute_w <- function(e0_topals, e0_ibge, ano, delta_hi = 5, delta_cap = 10) {
  if (is.na(e0_topals) || is.na(e0_ibge)) return(0)
  delta <- e0_topals - e0_ibge
  if (delta <= 0) return(0)
  
  esc_ano <- get_escala_ano(ano)
  base_w  <- 1 - esc_ano
  
  extra <- 0
  if (delta > delta_hi) {
    extra <- (delta - delta_hi) / (delta_cap - delta_hi)
    extra <- pmin(pmax(extra, 0), 1)
    extra <- 0.3 * extra
  }
  
  w <- (base_w + extra) * year_shrink_factor(ano)
  pmin(pmax(w, 0), 0.95)
}

get_mx_tbl_from_reconstruct <- function(ano, code_muni6) {
  res <- reconstruct_mx_muni(ano, code_muni6)
  if (is.list(res) && !is.null(res$schedules)) return(res$schedules)
  stop("reconstruct_mx_muni() não retornou schedules.")
}

get_mx_col_name <- function(mx_tbl) {
  cn <- names(mx_tbl)
  candidatos <- c("mx_topals","mx","mx_fit","mx_hat","mx_total")
  hit <- candidatos[candidatos %in% cn]
  if (length(hit) == 0) stop("Não encontrei coluna de mx. Nomes: ", paste(cn, collapse = ", "))
  hit[1]
}

calc_e0_scaled <- function(logk, mx_tbl, mx_col) {
  mx_tmp <- mx_tbl
  mx_tmp$mx_scaled <- mx_tbl[[mx_col]] * exp(logk)
  calc_e0_from_mx_tbl(mx_tmp, mx_col = "mx_scaled")
}

build_life_table_from_mx_tbl <- function(mx_tbl, mx_col = "mx_nmx_final") {
  df <- mx_tbl %>% dplyr::arrange(idade)
  idade <- df$idade
  mx    <- df[[mx_col]]
  
  n <- c(diff(idade), 1L)
  ax <- rep(0.5, length(mx))
  if (length(ax) > 0) ax[1] <- 0.3
  
  qx <- (n * mx) / (1 + (n - ax) * mx)
  qx[length(qx)] <- 1
  
  lx <- numeric(length(mx)); lx[1] <- 100000
  if (length(mx) > 1) for (i in seq_len(length(mx) - 1)) lx[i + 1] <- lx[i] * (1 - qx[i])
  
  dx <- lx * qx
  
  Lx <- numeric(length(mx))
  if (length(mx) > 1) {
    Lx[1:(length(mx) - 1)] <- n[1:(length(mx) - 1)] * lx[2:length(mx)] + ax[1:(length(mx) - 1)] * dx[1:(length(mx) - 1)]
  }
  Lx[length(mx)] <- lx[length(mx)] / mx[length(mx)]
  
  Tx <- rev(cumsum(rev(Lx)))
  ex <- Tx / lx
  
  tibble::tibble(idade = idade, n = n, mx = mx, qx = qx, lx = lx, dx = dx, Lx = Lx, Tx = Tx, ex = ex)
}

# Grid (ano x muni)
munis_grid <- e0_muni_base %>% dplyr::distinct(ano, code_muni6) %>% dplyr::arrange(ano, code_muni6)

# ------------------------------------------------------------
# PATCH: indexar fitfiles por ano e carregar 1x por ano
# (mantém a metodologia; só evita reload/extract repetido)
# ------------------------------------------------------------

# Indexa anos a partir do nome do arquivo
fit_index <- tibble::tibble(fitfile = fitfiles) %>%
  dplyr::mutate(
    ano   = as.integer(stringr::str_match(basename(fitfile), "_(\\d{4})_")[,2]),
    nivel = stringr::str_match(basename(fitfile), "_(municipio|imediata|intermediaria)_")[,2]
  ) %>%
  dplyr::filter(!is.na(ano), nivel == "municipio") %>%
  dplyr::arrange(ano)

get_fitfile_year <- function(ano_i) {
  ff <- fit_index$fitfile[fit_index$ano == ano_i][1]
  if (length(ff) == 0 || is.na(ff) || !file.exists(ff)) {
    stop("Não achei fitfile para ano=", ano_i, " em TOPALS_FIT_DIR.")
  }
  ff
}

# ------------------------------------------------------------
# FIX: rstan::as.matrix NÃO é exportado em várias versões.
# Use o genérico base as.matrix(fit, pars=...)
# + fallback determinístico (lambda = std + B*alpha) se lambda não vier.
# ------------------------------------------------------------
as_matrix_stanfit_safe <- function(fit, pars) {
  # tenta o método correto (genérico base)
  out <- tryCatch(
    as.matrix(fit, pars = pars),
    error = function(e) NULL
  )
  out
}

get_lambda_draws_one_region <- function(fit, A, r, B = NULL, std_schedule = NULL) {
  # 1) tenta extrair lambda direto (metodologia original)
  pars_lam <- sprintf("lambda[%d,%d]", 1:A, r)
  m <- as_matrix_stanfit_safe(fit, pars = pars_lam)
  
  if (!is.null(m) && ncol(m) >= A) {
    # garante ordem por idade
    m <- m[, pars_lam, drop = FALSE]
    return(m)
  }
  
  # 2) fallback determinístico (não muda metodologia: é a identidade do modelo)
  if (is.null(B) || is.null(std_schedule)) {
    stop("Falha ao extrair lambda e não recebi B/std_schedule para reconstruir.")
  }
  K <- ncol(B)
  pars_a <- sprintf("alpha[%d,%d]", 1:K, r)
  a <- as_matrix_stanfit_safe(fit, pars = pars_a)
  
  if (is.null(a) || ncol(a) < K) {
    stop("Não consegui extrair alpha para reconstruir lambda.")
  }
  a <- a[, pars_a, drop = FALSE]
  
  # lambda = std_schedule + B * alpha
  # a: draws x K
  # B: A x K  => t(B): K x A
  lam <- a %*% t(B)  # draws x A
  lam <- sweep(lam, 2, as.numeric(std_schedule), "+")  # soma std_schedule por idade
  
  colnames(lam) <- pars_lam
  lam
}

mx_tbl_from_fit_current <- function(fit, regions, code_muni6, ages = 0:100, B = NULL, std_schedule = NULL) {
  r <- match(as.integer(code_muni6), as.integer(regions))
  if (is.na(r)) return(NULL)
  
  A <- length(ages)
  lam_draws <- get_lambda_draws_one_region(
    fit = fit,
    A   = A,
    r   = r,
    B   = B,
    std_schedule = std_schedule
  ) # draws x idade
  
  # consistente com teu "p50"
  mx_p50 <- apply(exp(lam_draws), 2, stats::median, na.rm = TRUE)
  
  tibble::tibble(
    idade     = ages,
    mx_topals = as.numeric(mx_p50)
  )
}

# Barra de progresso (pra não parecer que travou)
pb <- utils::txtProgressBar(min = 0, max = nrow(munis_grid), style = 3)
on.exit(close(pb), add = TRUE)

# Cache do ano corrente
ano_loaded   <- NA_integer_
env_fit      <- NULL
fit_cur      <- NULL
regions_cur  <- NULL
B_cur        <- NULL
std_cur      <- NULL

message("Iniciando shrink ex-post e construção do NMX final...")

shrink_rows <- vector("list", nrow(munis_grid))
mx_nmx_final_list <- vector("list", nrow(munis_grid))

for (idx in seq_len(nrow(munis_grid))) {
  
  ano_i       <- as.integer(munis_grid$ano[idx])
  code_muni_i <- as.integer(munis_grid$code_muni6[idx])
  
  row_muni <- e0_muni_base %>%
    dplyr::filter(ano == !!ano_i, code_muni6 == !!code_muni_i) %>%
    dplyr::slice(1)
  
  if (nrow(row_muni) == 0L) {
    shrink_rows[[idx]] <- tibble(ano = ano_i, code_muni6 = code_muni_i,
                                 e0_p50_post = NA_real_, w = 0, k_factor = 1,
                                 shrink_applied = FALSE)
    mx_nmx_final_list[[idx]] <- NULL
    utils::setTxtProgressBar(pb, idx)
    next
  }
  
  e0_topals <- row_muni$e0_p50
  e0_ibge   <- row_muni$e0_ibge
  
  w_i <- compute_w(e0_topals, e0_ibge, ano_i)
  
  # troca de ano: carrega fit 1x
  if (!identical(ano_i, ano_loaded)) {
    ano_loaded <- ano_i
    
    # limpa o env anterior
    if (!is.null(env_fit)) {
      rm(list = ls(envir = env_fit), envir = env_fit)
      env_fit <- NULL
    }
    gc()
    
    ff_year <- get_fitfile_year(ano_i)
    message("\n[05B] Carregando fit do ano ", ano_i, ": ", basename(ff_year))
    env_fit <- new.env(parent = emptyenv())
    load(ff_year, envir = env_fit)
    
    fit_cur     <- env_fit$fit
    regions_cur <- env_fit$case_meta$regions
    
    # IMPORTANTÍSSIMO: pega B e std_schedule do stanDataList do próprio fit do ano
    B_cur   <- env_fit$stanDataList$B
    std_cur <- env_fit$stanDataList$std_schedule
  }
  
  mx_tbl <- tryCatch(
    mx_tbl_from_fit_current(
      fit         = fit_cur,
      regions     = regions_cur,
      code_muni6  = code_muni_i,
      ages        = 0:100,
      B           = B_cur,
      std_schedule= std_cur
    ),
    error = function(e) NULL
  )
  
  if (is.null(mx_tbl)) {
    shrink_rows[[idx]] <- tibble(ano = ano_i, code_muni6 = code_muni_i,
                                 e0_p50_post = e0_topals, w = w_i, k_factor = 1,
                                 shrink_applied = FALSE)
    mx_nmx_final_list[[idx]] <- NULL
    utils::setTxtProgressBar(pb, idx)
    next
  }
  
  mx_col <- tryCatch(get_mx_col_name(mx_tbl), error = function(e) NA_character_)
  if (is.na(mx_col)) {
    shrink_rows[[idx]] <- tibble(ano = ano_i, code_muni6 = code_muni_i,
                                 e0_p50_post = e0_topals, w = w_i, k_factor = 1,
                                 shrink_applied = FALSE)
    mx_nmx_final_list[[idx]] <- NULL
    utils::setTxtProgressBar(pb, idx)
    next
  }
  
  e0_target <- e0_topals
  shrink_ok <- FALSE
  k_factor  <- 1
  e0_post   <- e0_topals
  
  if (!is.na(w_i) && w_i > 0 && is.finite(e0_ibge) && is.finite(e0_topals)) {
    e0_target_tmp <- e0_topals - w_i * (e0_topals - e0_ibge)
    if (is.finite(e0_target_tmp) && e0_target_tmp < e0_topals) {
      e0_target <- e0_target_tmp
      
      f <- function(logk) calc_e0_scaled(logk, mx_tbl, mx_col) - e0_target
      f0 <- tryCatch(f(0), error = function(e) NA_real_)
      
      if (is.finite(f0)) {
        hi <- log(2)
        f_hi <- tryCatch(f(hi), error = function(e) NA_real_)
        iter <- 0
        while (is.finite(f_hi) && f_hi > 0 && iter < 10) {
          hi <- hi + log(2)
          f_hi <- tryCatch(f(hi), error = function(e) NA_real_)
          iter <- iter + 1
        }
        
        if (!is.finite(f_hi) || f_hi > 0) {
          k_factor <- exp(hi)
          mx_tbl2 <- mx_tbl; mx_tbl2$mx_post <- mx_tbl2[[mx_col]] * k_factor
          e0_post <- calc_e0_from_mx_tbl(mx_tbl2, mx_col = "mx_post")
          shrink_ok <- TRUE
        } else {
          root <- tryCatch(uniroot(f, lower = 0, upper = hi), error = function(e) NULL)
          if (is.null(root)) {
            k_factor <- exp(hi)
          } else {
            k_factor <- exp(root$root)
          }
          mx_tbl2 <- mx_tbl; mx_tbl2$mx_post <- mx_tbl2[[mx_col]] * k_factor
          e0_post <- calc_e0_from_mx_tbl(mx_tbl2, mx_col = "mx_post")
          shrink_ok <- TRUE
        }
      }
    }
  }
  
  mx_tbl_final <- mx_tbl %>%
    dplyr::mutate(mx_nmx_final = .data[[mx_col]] * k_factor) %>%
    dplyr::mutate(
      uf                = row_muni$uf[1],
      uf_sigla          = row_muni$uf_sigla[1],
      nome_uf           = row_muni$nome_uf[1],
      regiao_code       = row_muni$regiao_code[1],
      regiao_nome       = row_muni$regiao_nome[1],
      rgi_imediata_code = row_muni$rgi_imediata_code[1],
      rgi_imediata_nome = row_muni$rgi_imediata_nome[1],
      code_muni6        = row_muni$code_muni6[1],
      nome_muni         = row_muni$nome_muni[1],
      ano               = ano_i,
      sexo              = SEXO_ALVO
    )
  
  shrink_rows[[idx]] <- tibble(ano = ano_i, code_muni6 = code_muni_i,
                               e0_p50_post = e0_post, w = w_i, k_factor = k_factor,
                               shrink_applied = shrink_ok)
  mx_nmx_final_list[[idx]] <- mx_tbl_final
  
  utils::setTxtProgressBar(pb, idx)
}

message("Shrink concluído. Montando base consolidada de NMX final...")

shrink_res <- dplyr::bind_rows(shrink_rows)

mx_list_ok <- purrr::compact(mx_nmx_final_list)
if (length(mx_list_ok) == 0) {
  stop("mx_nmx_final_list ficou vazia (0 municípios reconstruídos). ",
       "Isso indica falha na extração/reconstrução de lambda/alpha dentro do loop.")
}

mx_nmx_final_all <- dplyr::bind_rows(mx_list_ok) %>%
  dplyr::arrange(ano, code_muni6, idade)

nmx_final_path <- file.path(DB_DIR_05B, sprintf("nmx_final_municipios_idade_simples_%s_sex%s.parquet", UF_ALVO, SEXO_ALVO))
arrow::write_parquet(mx_nmx_final_all, nmx_final_path)
message("NMX final por idade simples salvo em: ", nmx_final_path)

e0_muni_post <- e0_muni_base %>%
  dplyr::left_join(shrink_res, by = c("ano", "code_muni6")) %>%
  dplyr::mutate(
    e0_p50_post = dplyr::if_else(is.na(e0_p50_post), e0_p50, e0_p50_post),
    delta_post  = e0_p50_post - e0_ibge,
    sexo        = SEXO_ALVO
  )

e0_post_path <- file.path(DB_DIR_05B, sprintf("e0_municipios_post_shrink_%s_sex%s.parquet", UF_ALVO, SEXO_ALVO))
arrow::write_parquet(e0_muni_post, e0_post_path)

# Tabela de vida por município usando mx_nmx_final
message("Construindo tabela de vida completa (idade simples) para todos os municípios...")

vida_list <- mx_nmx_final_all %>%
  dplyr::group_by(
    sexo, ano, uf, uf_sigla, nome_uf,
    code_muni6, nome_muni,
    regiao_code, regiao_nome,
    rgi_imediata_code, rgi_imediata_nome
  ) %>%
  dplyr::group_split()

vida_muni_all <- purrr::map_dfr(
  vida_list,
  function(df) {
    meta <- df %>%
      dplyr::slice(1) %>%
      dplyr::select(
        sexo, ano, uf, uf_sigla, nome_uf,
        code_muni6, nome_muni,
        regiao_code, regiao_nome,
        rgi_imediata_code, rgi_imediata_nome
      )
    life_tbl <- build_life_table_from_mx_tbl(df, mx_col = "mx_nmx_final")
    meta_rep <- meta[rep(1, nrow(life_tbl)), ]
    dplyr::bind_cols(meta_rep, life_tbl)
  }
)

vida_path <- file.path(DB_DIR_05B, sprintf("tabela_vida_municipios_idade_simples_%s_sex%s.parquet", UF_ALVO, SEXO_ALVO))
arrow::write_parquet(vida_muni_all, vida_path)
message("Tabela de vida completa salva em: ", vida_path)

# e60 municipal pós-shrink
e60_muni_post <- vida_muni_all %>%
  dplyr::filter(idade == 60L) %>%
  dplyr::select(
    sexo, ano, uf, uf_sigla, nome_uf,
    code_muni6, nome_muni,
    regiao_code, regiao_nome,
    rgi_imediata_code, rgi_imediata_nome,
    e60_post = ex
  )

tab_e60_ibge <- tibble(
  ano      = anos_alvo,
  e60_ibge = purrr::map_dbl(anos_alvo, ~ e60_ibge_from_tabua(.x, UF_ALVO, SEXO_ALVO))
)

###############################################################################
# 8.2) SELEÇÃO DE MUNICÍPIOS FOCO (se não houver objeto munis_foco)
###############################################################################

# Base para escolher porte populacional:
# - preferir sexo == "b" (total) se existir
# - senão, somar m+f
base_focus <- base_muni
if ("sexo" %in% names(base_focus)) {
  if (any(base_focus$sexo == "b", na.rm = TRUE)) {
    base_focus <- base_focus %>% dplyr::filter(sexo == "b")
  } else {
    # soma m+f no nível municipal/ano/idade (não precisa idade aqui; vamos agregar)
    # (mantém lógica: usar "tamanho do município" independente do sexo-alvo)
  }
}

# pega coluna de população de forma robusta
pop_col_focus <- {
  cols <- names(base_focus)
  pick_col(cols, c("pop_ambos","pop","pop_total","N","populacao","pop_final","pop_corrigida"))
}
if (is.na(pop_col_focus)) stop("Não encontrei coluna de população em base_focus (para munis_foco).")

if (!exists("munis_foco")) {
  message("Objeto 'munis_foco' não encontrado. Selecionando 4 municípios de porte distinto...")
  
  pop_info <- base_focus %>%
    dplyr::filter(uf_sigla == UF_ALVO) %>%
    dplyr::group_by(
      ano, code_muni6, nome_muni,
      rgi_imediata_code, rgi_imediata_nome
    ) %>%
    dplyr::summarise(
      pop_total = sum(.data[[pop_col_focus]], na.rm = TRUE),
      .groups   = "drop"
    ) %>%
    dplyr::group_by(
      code_muni6, nome_muni,
      rgi_imediata_code, rgi_imediata_nome
    ) %>%
    dplyr::summarise(
      pop_medio = mean(pop_total, na.rm = TRUE),
      .groups   = "drop"
    ) %>%
    dplyr::filter(!is.na(pop_medio)) %>%
    dplyr::mutate(
      porte_quartil = dplyr::ntile(pop_medio, 4)
    )
  
  if (nrow(pop_info) < 4) {
    stop("Não foi possível selecionar 4 municípios de foco (poucos municípios com população válida).")
  }
  
  munis_sel <- tibble::tibble()
  
  for (q in 1:4) {
    cand <- pop_info %>% dplyr::filter(porte_quartil == q)
    
    # tenta pegar RGIs diferentes
    if (nrow(munis_sel) > 0) {
      cand2 <- cand %>% dplyr::filter(!rgi_imediata_code %in% munis_sel$rgi_imediata_code)
      if (nrow(cand2) > 0) cand <- cand2
    }
    
    muni_q <- cand %>% dplyr::slice_sample(n = 1)
    munis_sel <- dplyr::bind_rows(munis_sel, muni_q)
  }
  
  munis_foco <- munis_sel %>%
    dplyr::transmute(
      code_muni6     = code_muni6,
      nome_muni      = nome_muni,
      nome_curto     = nome_muni,
      porte_quartil  = porte_quartil
    )
  
  message("Municípios de foco selecionados automaticamente:")
  print(munis_foco)
  
} else {
  message("Usando 'munis_foco' definido externamente (ajustando para ter coluna 'nome_curto'):")
  
  if (!"code_muni6" %in% names(munis_foco)) {
    stop("O objeto 'munis_foco' precisa ter uma coluna 'code_muni6'.")
  }
  
  if (!"nome_curto" %in% names(munis_foco)) {
    if ("nome_muni" %in% names(munis_foco)) {
      munis_foco <- munis_foco %>% dplyr::mutate(nome_curto = nome_muni)
    } else {
      munis_foco <- munis_foco %>% dplyr::mutate(nome_curto = as.character(code_muni6))
    }
  }
  
  print(munis_foco)
}

###############################################################################
# 8.3) SÉRIE DE e0 PARA MUNICÍPIOS FOCO (TOPALS vs pós-shrink vs IBGE)
###############################################################################

# garante objeto com flag (se não existir no teu script)
if (!exists("e0_muni_post_flag")) {
  e0_muni_post_flag <- e0_muni_post
}

serie_foco <- e0_muni_post_flag %>%
  dplyr::inner_join(munis_foco, by = "code_muni6")

serie_foco_long <- serie_foco %>%
  dplyr::select(ano, nome_curto, e0_p50, e0_p50_post, e0_ibge) %>%
  tidyr::pivot_longer(
    cols      = c(e0_p50, e0_p50_post),
    names_to  = "versao",
    values_to = "e0"
  ) %>%
  dplyr::mutate(
    versao = dplyr::recode(
      versao,
      e0_p50      = "TOPALS original",
      e0_p50_post = "TOPALS pós-shrink"
    )
  )

estado_line <- serie_foco %>% dplyr::distinct(ano, e0_ibge)

p_serie_foco <- ggplot2::ggplot(
  serie_foco_long,
  ggplot2::aes(x = ano, y = e0, color = nome_curto, linetype = versao)
) +
  ggplot2::geom_line() +
  ggplot2::geom_point(size = 1.5) +
  ggplot2::geom_line(
    data        = estado_line,
    ggplot2::aes(x = ano, y = e0_ibge),
    inherit.aes = FALSE,
    linetype    = "dashed",
    color       = "grey40"
  ) +
  ggplot2::labs(
    title    = paste0("e0 municipal ao longo do tempo — municípios foco (", UF_ALVO, ", sex=", SEXO_ALVO, ")"),
    subtitle = "Linhas coloridas: municípios (original vs pós-shrink) • Tracejado cinza: e0 estadual (IBGE)",
    x        = "Ano",
    y        = "e0 (anos)",
    color    = "Município",
    linetype = "Versão"
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position = "bottom",
    legend.box      = "vertical"
  )

fig_serie_foco_path <- file.path(
  FIG_DIR_05B,
  paste0("serie_e0_munis_foco_orig_vs_post_", UF_ALVO, "_sex", SEXO_ALVO, ".png")
)
ggplot2::ggsave(fig_serie_foco_path, p_serie_foco, width = 8, height = 4.5, dpi = 300)
message("Figura de série e0 (municípios foco, conjunta) salva em: ", fig_serie_foco_path)

# individuais
for (i in seq_len(nrow(munis_foco))) {
  muni_i <- munis_foco$code_muni6[i]
  nome_i <- munis_foco$nome_curto[i]
  
  serie_m <- serie_foco %>% dplyr::filter(code_muni6 == muni_i)
  if (nrow(serie_m) == 0L) next
  
  serie_m_long <- serie_m %>%
    dplyr::select(ano, e0_p50, e0_p50_post, e0_ibge) %>%
    tidyr::pivot_longer(
      cols      = c(e0_p50, e0_p50_post),
      names_to  = "versao",
      values_to = "e0"
    ) %>%
    dplyr::mutate(
      versao = dplyr::recode(
        versao,
        e0_p50      = "TOPALS original",
        e0_p50_post = "TOPALS pós-shrink"
      )
    )
  
  estado_line_m <- serie_m %>% dplyr::distinct(ano, e0_ibge)
  
  p_m <- ggplot2::ggplot(
    serie_m_long,
    ggplot2::aes(x = ano, y = e0, color = versao, linetype = versao)
  ) +
    ggplot2::geom_line() +
    ggplot2::geom_point(size = 1.5) +
    ggplot2::geom_line(
      data        = estado_line_m,
      ggplot2::aes(x = ano, y = e0_ibge),
      inherit.aes = FALSE,
      linetype    = "dashed",
      color       = "grey40"
    ) +
    ggplot2::labs(
      title    = paste0("e0 municipal ao longo do tempo — ", nome_i, " (", UF_ALVO, ", sex=", SEXO_ALVO, ")"),
      subtitle = "Linhas: TOPALS original vs pós-shrink • Tracejado cinza: e0 estadual (IBGE)",
      x        = "Ano",
      y        = "e0 (anos)",
      color    = "Versão",
      linetype = "Versão"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.background = ggplot2::element_rect(fill = "white", colour = NA),
      legend.position = "bottom",
      legend.box      = "vertical"
    )
  
  fig_m_path <- file.path(FIG_DIR_05B, paste0("serie_e0_", UF_ALVO, "_", muni_i, "_sex", SEXO_ALVO, ".png"))
  ggplot2::ggsave(fig_m_path, p_m, width = 7, height = 4.5, dpi = 300)
  message("Figura de série e0 salva para ", nome_i, ": ", fig_m_path)
}

###############################################################################
# 8.4) SÉRIE DE e60 PARA MUNICÍPIOS FOCO (pós-shrink vs IBGE)
###############################################################################

e60_muni_foco <- e60_muni_post %>%
  dplyr::inner_join(munis_foco, by = "code_muni6") %>%
  dplyr::left_join(tab_e60_ibge, by = "ano")

e60_foco_long <- e60_muni_foco %>%
  dplyr::select(ano, nome_curto, e60_post, e60_ibge) %>%
  dplyr::mutate(versao = "TOPALS pós-shrink", e60 = e60_post)

estado_e60 <- e60_muni_foco %>% dplyr::distinct(ano, e60_ibge)

p_serie_foco_e60 <- ggplot2::ggplot(
  e60_foco_long,
  ggplot2::aes(x = ano, y = e60, color = nome_curto, linetype = versao)
) +
  ggplot2::geom_line() +
  ggplot2::geom_point(size = 1.5) +
  ggplot2::geom_line(
    data        = estado_e60,
    ggplot2::aes(x = ano, y = e60_ibge),
    inherit.aes = FALSE,
    linetype    = "dashed",
    color       = "grey40"
  ) +
  ggplot2::labs(
    title    = paste0("e60 municipal ao longo do tempo — municípios foco (", UF_ALVO, ", sex=", SEXO_ALVO, ")"),
    subtitle = "Linhas coloridas: e60 pós-shrink • Tracejado cinza: e60 estadual (IBGE)",
    x        = "Ano",
    y        = "e60 (anos)",
    color    = "Município",
    linetype = "Versão"
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position = "bottom",
    legend.box      = "vertical"
  )

fig_serie_foco_e60_path <- file.path(
  FIG_DIR_05B,
  paste0("serie_e60_munis_foco_post_vs_ibge_", UF_ALVO, "_sex", SEXO_ALVO, ".png")
)
ggplot2::ggsave(fig_serie_foco_e60_path, p_serie_foco_e60, width = 8, height = 4.5, dpi = 300)
message("Figura de série e60 (municípios foco, conjunta) salva em: ", fig_serie_foco_e60_path)

for (i in seq_len(nrow(munis_foco))) {
  muni_i <- munis_foco$code_muni6[i]
  nome_i <- munis_foco$nome_curto[i]
  
  e60_m <- e60_muni_foco %>% dplyr::filter(code_muni6 == muni_i)
  if (nrow(e60_m) == 0L) next
  
  e60_m_long <- e60_m %>% dplyr::mutate(versao = "TOPALS pós-shrink", e60 = e60_post)
  estado_e60_m <- e60_m %>% dplyr::distinct(ano, e60_ibge)
  
  p_e60_m <- ggplot2::ggplot(
    e60_m_long,
    ggplot2::aes(x = ano, y = e60, color = versao, linetype = versao)
  ) +
    ggplot2::geom_line() +
    ggplot2::geom_point(size = 1.5) +
    ggplot2::geom_line(
      data        = estado_e60_m,
      ggplot2::aes(x = ano, y = e60_ibge),
      inherit.aes = FALSE,
      linetype    = "dashed",
      color       = "grey40"
    ) +
    ggplot2::labs(
      title    = paste0("e60 municipal ao longo do tempo — ", nome_i, " (", UF_ALVO, ", sex=", SEXO_ALVO, ")"),
      subtitle = "Linha: e60 pós-shrink • Tracejado cinza: e60 estadual (IBGE)",
      x        = "Ano",
      y        = "e60 (anos)",
      color    = "Versão",
      linetype = "Versão"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.background = ggplot2::element_rect(fill = "white", colour = NA),
      legend.position = "bottom",
      legend.box      = "vertical"
    )
  
  fig_e60_m_path <- file.path(FIG_DIR_05B, paste0("serie_e60_", UF_ALVO, "_", muni_i, "_sex", SEXO_ALVO, ".png"))
  ggplot2::ggsave(fig_e60_m_path, p_e60_m, width = 7, height = 4.5, dpi = 300)
  message("Figura de série e60 salva para ", nome_i, ": ", fig_e60_m_path)
}

###############################################################################
# 8.5) CURVAS DE log(mx) PARA MUNICÍPIOS FOCO (IBGE vs mediana estadual vs NMX muni)
###############################################################################

safe_log <- function(x, eps = 1e-12) log(pmax(as.numeric(x), eps))

logmx_uf_mediana <- mx_nmx_final_all %>%
  dplyr::filter(uf_sigla == UF_ALVO) %>%
  dplyr::group_by(ano, idade) %>%
  dplyr::summarise(
    mx_mediana = median(mx_nmx_final, na.rm = TRUE),
    .groups    = "drop"
  ) %>%
  dplyr::mutate(logmx_mediana_uf = safe_log(mx_mediana))

for (ano_k in intersect(ANOS_DIAG, anos_alvo)) {
  for (i in seq_len(nrow(munis_foco))) {
    muni_i <- munis_foco$code_muni6[i]
    nome_i <- munis_foco$nome_curto[i]
    
    df_muni <- mx_nmx_final_all %>%
      dplyr::filter(ano == ano_k, code_muni6 == muni_i) %>%
      dplyr::arrange(idade) %>%
      dplyr::mutate(logmx_muni = safe_log(mx_nmx_final))
    
    if (nrow(df_muni) == 0L) next
    
    ages_here <- sort(unique(df_muni$idade))
    
    logmx_ibge <- make_std_schedule_ibge(
      uf       = UF_ALVO,
      ano_std  = ano_k,
      sexo_std = SEXO_ALVO,
      ages     = ages_here
    )
    
    df_ibge <- tibble::tibble(
      idade      = ages_here,
      logmx_ibge = as.numeric(logmx_ibge)
    )
    
    df_med_uf <- logmx_uf_mediana %>%
      dplyr::filter(ano == ano_k, idade %in% ages_here)
    
    df_plot <- df_muni %>%
      dplyr::select(idade, logmx_muni) %>%
      dplyr::left_join(df_ibge, by = "idade") %>%
      dplyr::left_join(df_med_uf %>% dplyr::select(idade, logmx_mediana_uf), by = "idade") %>%
      tidyr::pivot_longer(
        cols      = c(logmx_muni, logmx_ibge, logmx_mediana_uf),
        names_to  = "serie",
        values_to = "logmx"
      ) %>%
      dplyr::mutate(
        serie = dplyr::recode(
          serie,
          logmx_muni       = paste0("Município (NMX final) — ", nome_i),
          logmx_ibge       = paste0("IBGE UF ", UF_ALVO, " (sex=", SEXO_ALVO, ")"),
          logmx_mediana_uf = paste0("Mediana estadual NMX final (", UF_ALVO, ")")
        )
      )
    
    p_logmx <- ggplot2::ggplot(
      df_plot,
      ggplot2::aes(x = idade, y = logmx, color = serie, linetype = serie)
    ) +
      ggplot2::geom_line(size = 0.9) +
      ggplot2::geom_point(size = 1.3) +
      ggplot2::labs(
        title = paste0("Curva de log(mx) — ", nome_i, " (", UF_ALVO, "), ano ", ano_k, " — sex=", SEXO_ALVO),
        x     = "Idade",
        y     = "log(mx)",
        color = "Curva",
        linetype = "Curva"
      ) +
      ggplot2::theme_minimal() +
      ggplot2::theme(
        plot.background = ggplot2::element_rect(fill = "white", colour = NA),
        legend.position = "bottom",
        legend.box      = "vertical"
      )
    
    fig_logmx_path <- file.path(
      FIG_DIR_05B,
      paste0("logmx_", UF_ALVO, "_", muni_i, "_", ano_k, "_sex", SEXO_ALVO, ".png")
    )
    ggplot2::ggsave(fig_logmx_path, p_logmx, width = 7, height = 4.5, dpi = 300)
    message("Curva de log(mx) salva para ", nome_i, " — ano ", ano_k, ": ", fig_logmx_path)
  }
}

###############################################################################
# 8.6) MAPAS CHOROPLETH: e0 e e60 (apenas ANOS_DIAG) — saídas por sexo
###############################################################################

# Malha municipal UF alvo
uf_muni_sf <- geobr::read_municipality(code_muni = UF_ALVO, year = 2020) |>
  dplyr::mutate(code_muni6 = as.integer(substr(code_muni, 1, 6)))
names(uf_muni_sf)[names(uf_muni_sf) == "geom"] <- "geometry"
uf_muni_sf <- sf::st_as_sf(uf_muni_sf)

uf_muni_df <- uf_muni_sf
class(uf_muni_df) <- setdiff(class(uf_muni_df), "sf")

pb_range <- e0_muni_post |>
  dplyr::summarise(
    min_e0 = floor(stats::quantile(e0_p50_post, probs = 0.02, na.rm = TRUE)),
    max_e0 = ceiling(stats::quantile(e0_p50_post, probs = 0.98, na.rm = TRUE))
  )
min_e0 <- pb_range$min_e0
max_e0 <- pb_range$max_e0

e60_range <- e60_muni_post |>
  dplyr::summarise(
    min_e60 = floor(stats::quantile(e60_post, 0.02, na.rm = TRUE)),
    max_e60 = ceiling(stats::quantile(e60_post, 0.98, na.rm = TRUE))
  )
min_e60 <- e60_range$min_e60
max_e60 <- e60_range$max_e60

anos_mapa <- intersect(anos_alvo, ANOS_DIAG)

for (ano_k in anos_mapa) {
  
  df_ano_e0 <- e0_muni_post |>
    dplyr::filter(ano == ano_k)
  
  map_ano_e0 <- uf_muni_df |>
    dplyr::left_join(df_ano_e0, by = "code_muni6") |>
    sf::st_as_sf(sf_column_name = "geometry")
  
  map_ano_e0 <- map_ano_e0 |>
    dplyr::mutate(
      flag_e0_low  = e0_p50_post < 69,
      flag_e0_high = e0_p50_post > 80
    )
  
  out_e0 <- map_ano_e0 |>
    dplyr::filter(flag_e0_low | flag_e0_high)
  
  p_mapa_e0 <- ggplot2::ggplot() +
    ggplot2::geom_sf(data = map_ano_e0, ggplot2::aes(fill = e0_p50_post), color = NA) +
    viridis::scale_fill_viridis(
      option    = "magma",
      direction = -1,
      name      = "e0 (anos)",
      alpha     = 0.4,
      limits    = c(min_e0, max_e0),
      breaks    = seq(min_e0, max_e0, by = 2),
      oob       = scales::squish,
      guide     = ggplot2::guide_colorbar(
        direction      = "horizontal",
        barheight      = grid::unit(2, units = "mm"),
        barwidth       = grid::unit(50, units = "mm"),
        draw.ulim      = FALSE,
        title.position = "top",
        title.hjust    = 0.5,
        label.hjust    = 0.5
      )
    ) +
    ggplot2::coord_sf() +
    ggplot2::labs(
      title    = paste0("Esperança de vida ao nascer — ", UF_ALVO, ", ", ano_k, " (pós-shrink, sexo=", SEXO_ALVO, ")"),
      subtitle = "Triângulos: municípios com e0 < 69 ou e0 > 80",
      x = "lon", y = "lat"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.background = ggplot2::element_rect(fill = "white", colour = NA),
      legend.position = "bottom",
      legend.box      = "vertical",
      legend.title    = ggplot2::element_text(hjust = 0.5)
    )
  
  if (nrow(out_e0) > 0) {
    cent   <- sf::st_centroid(out_e0$geometry)
    coords <- sf::st_coordinates(cent)
    out_e0 <- out_e0 |> dplyr::mutate(lon = coords[, 1], lat = coords[, 2])
    
    p_mapa_e0 <- p_mapa_e0 +
      ggplot2::geom_point(
        data = out_e0,
        ggplot2::aes(x = lon, y = lat),
        shape = 24, fill = "yellow", color = "black", size = 3, stroke = 0.4
      )
  }
  
  fig_mapa_e0_path <- file.path(FIG_DIR_05B, paste0("mapa_e0_", UF_ALVO, "_sex", SEXO_ALVO, "_", ano_k, ".png"))
  ggplot2::ggsave(fig_mapa_e0_path, p_mapa_e0, width = 6, height = 6, dpi = 300)
  
  df_ano_e60 <- e60_muni_post |>
    dplyr::filter(ano == ano_k)
  
  map_ano_e60 <- uf_muni_df |>
    dplyr::left_join(df_ano_e60, by = "code_muni6") |>
    sf::st_as_sf(sf_column_name = "geometry")
  
  p_mapa_e60 <- ggplot2::ggplot() +
    ggplot2::geom_sf(data = map_ano_e60, ggplot2::aes(fill = e60_post), color = NA) +
    viridis::scale_fill_viridis(
      option    = "plasma",
      direction = -1,
      name      = "e60 (anos)",
      alpha     = 0.4,
      limits    = c(min_e60, max_e60),
      breaks    = seq(min_e60, max_e60, by = 2),
      oob       = scales::squish,
      guide     = ggplot2::guide_colorbar(
        direction      = "horizontal",
        barheight      = grid::unit(2, units = "mm"),
        barwidth       = grid::unit(50, units = "mm"),
        draw.ulim      = FALSE,
        title.position = "top",
        title.hjust    = 0.5,
        label.hjust    = 0.5
      )
    ) +
    ggplot2::coord_sf() +
    ggplot2::labs(
      title = paste0("Esperança de vida aos 60 anos — ", UF_ALVO, ", ", ano_k, " (pós-shrink, sexo=", SEXO_ALVO, ")"),
      x = "lon", y = "lat"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.background = ggplot2::element_rect(fill = "white", colour = NA),
      legend.position = "bottom",
      legend.box      = "vertical",
      legend.title    = ggplot2::element_text(hjust = 0.5)
    )
  
  fig_mapa_e60_path <- file.path(FIG_DIR_05B, paste0("mapa_e60_", UF_ALVO, "_sex", SEXO_ALVO, "_", ano_k, ".png"))
  ggplot2::ggsave(fig_mapa_e60_path, p_mapa_e60, width = 6, height = 6, dpi = 300)
}

###############################################################################
# 99) FINALIZAÇÃO: marca sucesso + limpa fits/RData (só se deu tudo certo)
###############################################################################

# marque como OK só aqui no final mesmo
PIPELINE_OK <- TRUE

# configura limpeza
DELETE_FITS <- FALSE  # <--- mude pra TRUE se quiser apagar de verdade (irreversível)
MOVE_DIR_FITS <- file.path(TOPALS_FIT_DIR, "_trash_fits_ok")

cleanup_fits_if_success <- function() {
  
  # condição “absoluto êxito”: outputs-chave existem
  ok_files <- c(
    nmx_final_path,
    vida_path,
    e0_post_path
  )
  ok_files <- ok_files[!is.na(ok_files)]
  
  if (!PIPELINE_OK) {
    message("[CLEANUP] PIPELINE_OK=FALSE. Não vou limpar nada.")
    return(invisible(FALSE))
  }
  if (any(!file.exists(ok_files))) {
    message("[CLEANUP] Saídas-chave faltando. Não vou limpar nada.\nFaltando:\n - ",
            paste(ok_files[!file.exists(ok_files)], collapse = "\n - "))
    return(invisible(FALSE))
  }
  
  # lista fits
  ff <- list.files(TOPALS_FIT_DIR, pattern="^topals_pi_ibge_.*\\.RData$", full.names=TRUE)
  if (length(ff) == 0) {
    message("[CLEANUP] Nenhum fit para limpar em: ", TOPALS_FIT_DIR)
    return(invisible(TRUE))
  }
  
  if (!DELETE_FITS) {
    dir.create(MOVE_DIR_FITS, recursive = TRUE, showWarnings = FALSE)
    ok_move <- file.rename(ff, file.path(MOVE_DIR_FITS, basename(ff)))
    if (any(!ok_move)) warning("[CLEANUP] Falha ao mover alguns fits.")
    message("[CLEANUP] Fits movidos para: ", MOVE_DIR_FITS)
  } else {
    ok_del <- file.remove(ff)
    if (any(!ok_del)) warning("[CLEANUP] Falha ao apagar alguns fits.")
    message("[CLEANUP] Fits APAGADOS (DELETE_FITS=TRUE).")
  }
  
  # opcional: limpar resumos antigos duplicados no fitdir (mantém o mais recente parquet+rds)
  sums <- list.files(TOPALS_FIT_DIR, pattern="^resumo_fits_.*\\.(parquet|rds)$", full.names=TRUE)
  if (length(sums) > 2) {
    o <- sums[order(file.info(sums)$mtime, decreasing = TRUE)]
    trash <- o[-c(1,2)]
    td <- file.path(TOPALS_FIT_DIR, "_trash_summaries_ok")
    dir.create(td, recursive = TRUE, showWarnings = FALSE)
    file.rename(trash, file.path(td, basename(trash)))
    message("[CLEANUP] Resumos antigos movidos para: ", td, " (", length(trash), ")")
  }
  
  invisible(TRUE)
}

cleanup_fits_if_success()

cat("\n✅ PIPELINE COMPLETO (00b + 01 + 02 + 03 + 05B) CONCLUÍDO ✅\n")
cat("UF = ", UF_ALVO, " | sexo = ", SEXO_ALVO, "\n")
cat(" - NMX final: ", nmx_final_path, "\n")
cat(" - Tabela de vida: ", vida_path, "\n")
