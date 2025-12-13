###############################################################################
# TOPALS + pi + IBGE – PIPELINE ÚNICO (00b + 01 + 02 + 03 + 05B)
#
# 1) CONFIG: defina BASE_DIR, UF_ALVO, anos e níveis e rode o script inteiro.
# 2) Pré-requisito: já ter rodado o 00_prep_topals.R para gerar:
#       00_prep_topals_output/bases_topals_preparadas.RData
#    com base_muni, base_muni_raw, pesos_80mais.
#
# Este script:
#   - Ajusta TOPALS + pi ancorado em e0 (IBGE)
#   - Faz shrink ex-post em e0
#   - Gera NMX final por idade simples (0–100) por município
#   - Gera tabela de vida completa (idade simples) por município
#   - Salva TODOS os dataframes em .parquet, organizados em:
#       BASE_DIR/resultados/UF_ALVO/{figuras,bancos_de_dados,indicadores_avancados}
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
  library(arrow)     # <- para leitura/escrita em .parquet
})

###############################################################################
# 0) CONFIGURAÇÃO GERAL (SÓ MEXE AQUI)
###############################################################################

BASE_DIR <- "C:/Users/uriel/Documents/UFPB Estatística/Demografia 1/Estudos Demográficos - PB/TOPALS"

UF_ALVO   <- "PE"                  # UF que você quer rodar
ANOS_FIT  <- 2000:2023             # anos para rodar o TOPALS
NIVEIS_FIT <- "municipio"          # por enquanto só município
ANOS_DIAG <- c(2000L, 2005L, 2010L, 2015L, 2020L, 2023L) # anos para diagnósticos/log(mx) etc.

# Caminhos padrão
OUTPUT_00_DIR <- file.path(BASE_DIR, "00_prep_topals_output")
dir.create(OUTPUT_00_DIR, showWarnings = FALSE, recursive = TRUE)

RDATA_PATH <- file.path(OUTPUT_00_DIR, "bases_topals_preparadas.RData")

if (!file.exists(RDATA_PATH)) {
  stop("Não encontrei ", RDATA_PATH,
       ". Rode antes o 00_prep_topals.R para gerar base_muni/base_muni_raw/pesos_80mais.")
}

# Carrega base_muni, base_muni_raw, pesos_80mais se ainda não existirem
load(RDATA_PATH)  # deve trazer base_muni_raw, base_muni, pesos_80mais (se já existiam), talvez tabua_ibge_uf

# Diretórios de RESULTADOS padronizados (por UF)
RESULTS_DIR <- file.path(BASE_DIR, "resultados", UF_ALVO)
RES_FIG_DIR <- file.path(RESULTS_DIR, "figuras")
RES_DB_DIR  <- file.path(RESULTS_DIR, "bancos_de_dados")
RES_IND_DIR <- file.path(RESULTS_DIR, "indicadores_avancados")

dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(RES_FIG_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(RES_DB_DIR,  showWarnings = FALSE, recursive = TRUE)
dir.create(RES_IND_DIR, showWarnings = FALSE, recursive = TRUE)

###############################################################################
# 1) 00b_build_tabua_ibge_uf.R  – Construir tabua_ibge_uf e salvar no .RData
#    Só roda se tabua_ibge_uf AINDA não existir (primeira vez).
###############################################################################

if (!exists("tabua_ibge_uf")) {
  
  message("\n[00b] 'tabua_ibge_uf' não encontrado na memória. Vou construir a partir do Excel do IBGE...")
  
  # 1.1 Tabela de UFs
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
  
  FILE_REF <- file.path(
    BASE_DIR,
    "projecoes_2024_tab5_tabuas_mortalidade.xlsx"
  )
  
  if (!file.exists(FILE_REF)) {
    stop("Arquivo de referência do IBGE não encontrado em:\n  ", FILE_REF)
  }
  
  message("\n===== [00b] Construindo tabua_ibge_uf a partir de: ", FILE_REF, " =====")
  
  # Detectar aba da tábua
  sheets <- readxl::excel_sheets(FILE_REF)
  sheet_candidates <- sheets[grepl("5", sheets) & grepl("T[ÁA]BUAS", sheets, ignore.case = TRUE)]
  if (length(sheet_candidates) == 0L) {
    sheet_use <- sheets[1]
    message("Não encontrei aba com '5' e 'TÁBUAS' no nome. Usando a primeira aba: ", sheet_use)
  } else {
    sheet_use <- sheet_candidates[1]
    message("Usando aba da tábua IBGE: ", sheet_use)
  }
  
  # Detectar linha de cabeçalho
  tmp0 <- readxl::read_excel(
    FILE_REF,
    sheet     = sheet_use,
    col_names = FALSE
  )
  
  header_row_candidates <- which(
    !is.na(tmp0[[1]]) &
      stringr::str_detect(tmp0[[1]], regex("^IDADE$", ignore_case = TRUE))
  )
  
  if (length(header_row_candidates) == 0L) {
    stop("Não encontrei uma linha com 'IDADE' na primeira coluna para usar como cabeçalho.")
  }
  
  header_row <- header_row_candidates[1]
  message("Linha de cabeçalho detectada: ", header_row)
  
  # Releitura com cabeçalho correto
  tab_raw <- readxl::read_excel(
    FILE_REF,
    sheet     = sheet_use,
    skip      = header_row - 1L,
    col_names = TRUE
  )
  tab_raw <- janitor::clean_names(tab_raw)
  
  nm <- names(tab_raw)
  message("Colunas detectadas na tábua IBGE (após detecção de cabeçalho):")
  print(nm)
  
  # Função auxiliar pra achar colunas
  pick_col <- function(patterns) {
    hits <- Reduce(
      `|`,
      lapply(patterns, function(p) grepl(p, nm, ignore.case = TRUE))
    )
    pos <- which(hits)
    if (length(pos) == 0L) return(NA_character_)
    nm[pos[1]]
  }
  
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
  
  ufs <- uf_table()
  
  # Construir tabua_ibge_uf
  tabua_ibge_uf <- tab_raw %>%
    dplyr::filter(
      stringr::str_detect(.data[[col_sexo]], regex("ambos", ignore_case = TRUE))
    ) %>%
    dplyr::transmute(
      uf_sigla = as.character(.data[[col_uf]]),
      ano      = as.integer(.data[[col_ano]]),
      idade    = as.integer(.data[[col_idade]]),
      mx_ibge  = as.numeric(.data[[col_mx]])
    ) %>%
    dplyr::inner_join(ufs, by = "uf_sigla") %>%
    dplyr::select(uf_sigla, ano, idade, mx_ibge) %>%
    dplyr::filter(
      !is.na(ano),
      !is.na(idade),
      !is.na(mx_ibge)
    ) %>%
    dplyr::filter(ano >= 2000, ano <= 2070) %>%
    dplyr::arrange(uf_sigla, ano, idade)
  
  message("\nResumo rápido de tabua_ibge_uf (UF == 'PB'):")
  print(
    tabua_ibge_uf %>%
      dplyr::filter(uf_sigla == "PB") %>%
      dplyr::count(ano, name = "n_idades") %>%
      head()
  )
  
  # ATENÇÃO: aqui NÃO damos load de novo.
  # A essa altura base_muni_raw, base_muni e pesos_80mais já estão na memória
  # por causa daquele load(RDATA_PATH) lá em cima do script.
  
  save(
    base_muni_raw,
    base_muni,
    pesos_80mais,
    tabua_ibge_uf,
    file = RDATA_PATH
  )
  
  message("[00b] Atualizei ", basename(RDATA_PATH),
          " para incluir o objeto 'tabua_ibge_uf'.")
  
} else {
  
  message("\n[00b] 'tabua_ibge_uf' já existe na memória (e veio de ", RDATA_PATH, ").")
  message("[00b] Pulando reconstrução da tábua IBGE para evitar briga entre sessões.")
}

###############################################################################
# 2) Funções auxiliares globais (TOPALS + IBGE)
###############################################################################

# Reload para garantir base_muni, pesos_80mais, tabua_ibge_uf na memória
load(RDATA_PATH)

# Base de splines em idade
make_B_matrix <- function(ages = 0:100) {
  splines::bs(ages, knots = c(0, 1, 10, 20, 40, 70), degree = 1)
}

# e0 a partir de log(mx) – integração simples (TOPALS/Stan)
e0_from_logmx <- function(logmx) {
  mx <- exp(logmx)
  px <- exp(-mx)
  lx <- c(1, cumprod(px))
  sum(head(lx, -1) + tail(lx, -1)) / 2
}

# ex(a) a partir de log(mx) – vetor de esperança de vida a cada idade
ex_from_logmx <- function(logmx) {
  A  <- length(logmx)
  mx <- exp(logmx)
  px <- exp(-mx)
  
  lx <- numeric(A + 1L)
  lx[1] <- 1
  for (a in 1:A) {
    lx[a + 1] <- lx[a] * px[a]
  }
  
  Lx <- 0.5 * (lx[1:A] + lx[2:(A + 1L)])
  Tx <- rev(cumsum(rev(Lx)))
  ex <- Tx / lx[1:A]
  ex
}

# std_schedule (log mx IBGE UF-ano, suavizado 0:100)
make_std_schedule_ibge <- function(uf,
                                   ano_std,
                                   ages = 0:100) {
  
  df_uf <- tabua_ibge_uf %>%
    dplyr::filter(
      uf_sigla == uf,
      ano == ano_std
    ) %>%
    dplyr::arrange(idade)
  
  if (nrow(df_uf) == 0L) {
    stop("Nenhos dados em tabua_ibge_uf para uf=", uf, ", ano=", ano_std)
  }
  
  ages_src   <- df_uf$idade
  log_mx_src <- log(df_uf$mx_ibge)
  
  log_mx_interp <- approx(
    x    = ages_src,
    y    = log_mx_src,
    xout = ages,
    rule = 2
  )$y
  
  B_std <- make_B_matrix(ages = ages)
  Proj  <- B_std %*% solve(crossprod(B_std)) %*% t(B_std)
  
  as.vector(Proj %*% log_mx_interp)
}

# e0 a partir da tábua IBGE (UF_ALVO)
ages_vec <- 0:100
e0_ibge_from_tabua <- function(ano_target, uf = UF_ALVO) {
  df <- tabua_ibge_uf %>%
    dplyr::filter(uf_sigla == uf, ano == ano_target) %>%
    dplyr::arrange(idade)
  
  if (nrow(df) == 0L) {
    stop("Sem tábua IBGE para uf=", uf, ", ano=", ano_target)
  }
  
  ages_src   <- df$idade
  log_mx_src <- log(df$mx_ibge)
  
  log_mx_interp <- approx(
    x    = ages_src,
    y    = log_mx_src,
    xout = ages_vec,
    rule = 2
  )$y
  
  e0_from_logmx(log_mx_interp)
}

# e60 a partir da tábua IBGE (UF_ALVO)
e60_ibge_from_tabua <- function(ano_target, uf = UF_ALVO, idade_ref = 60L) {
  df <- tabua_ibge_uf %>%
    dplyr::filter(uf_sigla == uf, ano == ano_target) %>%
    dplyr::arrange(idade)
  
  if (nrow(df) == 0L) {
    stop("Sem tábua IBGE para uf=", uf, ", ano=", ano_target)
  }
  
  ages_src   <- df$idade
  log_mx_src <- log(df$mx_ibge)
  
  log_mx_interp <- approx(
    x    = ages_src,
    y    = log_mx_src,
    xout = ages_vec,
    rule = 2
  )$y
  
  ex_vec <- ex_from_logmx(log_mx_interp)
  idx    <- which.min(abs(ages_vec - idade_ref))
  ex_vec[idx]
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

message("\nCompilando modelo Stan (TOPALS + pi + IBGE + âncora e0)...")
topals_pi_model <- rstan::stan_model(model_code = stanModelText_pi)

rstan::rstan_options(auto_write = TRUE)
options(mc.cores = max(1L, parallel::detectCores() - 1L))

N_CHAINS <- 4L
N_ITER   <- 1200L
N_WARMUP <- 200L

###############################################################################
# 4) Funções auxiliares para um caso (ano x nível x UF)
###############################################################################

build_stan_data_case <- function(base_muni,
                                 ano_target,
                                 nivel = c("municipio", "imediata", "intermediaria"),
                                 uf,
                                 ages = 0:100) {
  
  nivel <- match.arg(nivel)
  
  df <- base_muni %>%
    dplyr::filter(
      ano == ano_target,
      uf_sigla == uf
    )
  
  if (nrow(df) == 0L) {
    stop("Nenhum dado para ano=", ano_target, " e UF=", uf, " em base_muni.")
  }
  
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
      D = sum(obitos,    na.rm = TRUE),
      N = sum(pop_ambos, na.rm = TRUE),
      .groups = "drop"
    )
  
  if (nrow(df_agg) == 0L) {
    stop("Nenhum dado agregado para ano=", ano_target,
         ", nível=", nivel, ", UF=", uf, " após agrupamento.")
  }
  
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
    uf      = uf,
    ano_std = ano_target,
    ages    = ages
  )
  
  if (length(std_logmx) != A) {
    stop("Comprimento de std_schedule diferente de A.")
  }
  
  e0_target <- e0_from_logmx(std_logmx)
  
  cov_prior <- df %>%
    dplyr::distinct(cobertura_sim) %>%
    dplyr::pull()
  
  if (length(cov_prior) == 0L || all(is.na(cov_prior))) {
    warning("cobertura_sim ausente/NA para ano=", ano_target,
            ", UF=", uf, ". Usando prior pi ≈ 0.9 com logit sd 0.7.")
    cov_prior <- 0.9
  } else {
    cov_prior <- mean(cov_prior, na.rm = TRUE)
  }
  
  cov_prior <- cov_prior / ifelse(cov_prior > 1.5, 100, 1)
  cov_prior <- min(max(cov_prior, 0.01), 0.99)
  
  pi_prior_mean   <- rep(cov_prior, R)
  sigma_logit_pi  <- 0.7
  sigma_e0_target <- 1.0
  
  stanDataList <- list(
    R = R,
    S = 1L,
    A = A,
    K = ncol(B_age),
    B = B_age,
    std_schedule    = std_logmx,
    N               = pmax(N_mat, 0.01),
    D               = D_mat,
    pi_prior_mean   = pi_prior_mean,
    sigma_logit_pi  = sigma_logit_pi,
    e0_target       = e0_target,
    sigma_e0_target = sigma_e0_target
  )
  
  list(
    stanData = stanDataList,
    regions  = regions,
    ages     = ages
  )
}

fit_one_case <- function(this_ano,
                         this_nivel,
                         uf        = UF_ALVO,
                         sexos     = "b",
                         out_dir,
                         nchains   = N_CHAINS,
                         niter     = N_ITER,
                         warmup    = N_WARMUP) {
  
  if (sexos != "b") {
    stop("Nesta versão a base está agregada em sexos; use sexos = 'b'.")
  }
  
  message(
    "Rodando TOPALS+pi+IBGE | ano=", this_ano,
    " | nível=", this_nivel,
    " | UF=", uf
  )
  
  ages_vec <- 0:100
  
  stan_case <- build_stan_data_case(
    base_muni  = base_muni,
    ano_target = this_ano,
    nivel      = this_nivel,
    uf         = uf,
    ages       = ages_vec
  )
  
  stanDataList <- stan_case$stanData
  R <- stanDataList$R
  
  if (R == 0L) {
    warning("Nenhuma região para ano=", this_ano,
            ", nível=", this_nivel, ".")
    return(NULL)
  }
  
  fit <- rstan::sampling(
    object  = topals_pi_model,
    data    = stanDataList,
    seed    = 6447100 + this_ano,
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
  
  save(fit, stanDataList, case_meta, file = fitfile)
  
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

# 1) Caminho da pasta com os fits
TOPALS_FIT_DIR <- file.path(
  BASE_DIR,
  "01_topals_fits_pi_ibge",
  sprintf("%s_municipio_imediata_pi_ibge", UF_ALVO)
)

# 1) Caminho da pasta com os fits
TOPALS_FIT_DIR <- file.path(
  BASE_DIR,
  "01_topals_fits_pi_ibge",
  sprintf("%s_municipio_imediata_pi_ibge", UF_ALVO)
)

# 👉 GARANTIR QUE ESSA PASTA EXISTE
dir.create(TOPALS_FIT_DIR, showWarnings = FALSE, recursive = TRUE)

# 2) Listar todos os arquivos de fit (.RData)
fitfiles <- list.files(
  TOPALS_FIT_DIR,
  pattern = "^topals_pi_ibge_.*\\.RData$",
  full.names = TRUE
)

# 2) Listar todos os arquivos de fit (.RData)
fitfiles <- list.files(
  TOPALS_FIT_DIR,
  pattern = "^topals_pi_ibge_.*\\.RData$",
  full.names = TRUE
)

length(fitfiles)

# 3) Função segura pra reconstruir UM fit
rebuild_one_fit_safe <- function(ff) {
  tryCatch({
    # carrega em ambiente isolado pra não sujar o global
    env <- new.env()
    load(ff, envir = env)
    
    if (!exists("case_meta", envir = env)) {
      stop("Objeto 'case_meta' não encontrado no arquivo.")
    }
    
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
    message("⚠️ Pulando arquivo com erro: ", ff,
            " -> ", conditionMessage(e))
    NULL
  })
}

# 4) Reconstruir o 'results' varrendo todos os arquivos, pulando os quebrados
if (length(fitfiles) == 0L) {
  message(
    "\n[01] Nenhum fit pré-existente encontrado em:\n  ", TOPALS_FIT_DIR,
    "\n[01] Vou iniciar 'results' vazio e preencher conforme forem sendo estimados."
  )
  
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
    message(
      "\n[01] Há arquivos de fit, mas todos deram erro em 'rebuild_one_fit_safe()'.",
      "\n[01] Vou seguir com 'results' vazio."
    )
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

# conferir rapidamente
print(results %>% count(ano, nivel))

# 5) Salvar em parquet (e opcionalmente em RDS, agora DIREITO)
res_pb_parquet_path <- file.path(
  TOPALS_FIT_DIR,
  sprintf("resumo_fits_%s_municipio_imediata_pi_ibge.parquet", UF_ALVO)
)

res_pb_rds_path <- file.path(
  TOPALS_FIT_DIR,
  sprintf("resumo_fits_%s_municipio_imediata_pi_ibge.rds", UF_ALVO)
)

arrow::write_parquet(results, res_pb_parquet_path)
saveRDS(results, res_pb_rds_path)

message("Resumo reconstruído salvo em:")
message(" - ", res_pb_parquet_path)
message(" - ", res_pb_rds_path)

dir.create(TOPALS_FIT_DIR, showWarnings = FALSE, recursive = TRUE)

anos_disponiveis <- sort(unique(base_muni$ano))

cases <- tidyr::expand_grid(
  ano   = ANOS_FIT,
  nivel = NIVEIS_FIT
) %>%
  dplyr::arrange(ano, nivel)

message("\n===== [01] Rodando ", nrow(cases),
        " casos TOPALS+pi+IBGE para UF = ", UF_ALVO, " =====")

results <- tibble::tibble(
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
  
  res_i <- try(
    fit_one_case(
      this_ano   = row$ano,
      this_nivel = row$nivel,
      uf         = UF_ALVO,
      sexos      = "b",
      out_dir    = TOPALS_FIT_DIR
    ),
    silent = TRUE
  )
  
  if (inherits(res_i, "try-error") || is.null(res_i)) {
    warning("Falha no caso ano=", row$ano, ", nivel=", row$nivel,
            " -> pulando.")
    next
  }
  
  results <- dplyr::bind_rows(results, res_i)
}

res_pb_rds_path <- file.path(
  TOPALS_FIT_DIR,
  sprintf("resumo_fits_%s_municipio_imediata_pi_ibge.rds", UF_ALVO)
)
saveRDS(results, res_pb_rds_path)

# Versão em parquet do resumo de fits (para uso posterior)
res_pb_parquet_path <- file.path(
  RES_DB_DIR,
  sprintf("resumo_fits_%s_municipio_imediata_pi_ibge.parquet", UF_ALVO)
)
arrow::write_parquet(results, res_pb_parquet_path)

message("Resumo dos fits salvo em (RDS): ", res_pb_rds_path)
message("Resumo dos fits salvo em (PARQUET): ", res_pb_parquet_path)

###############################################################################
# 6) 02_e0_pi_from_topals_ibge – Extrair e0/pi para UF_ALVO
###############################################################################

message("\n===== [02] Extraindo e0/pi dos fits TOPALS+pi+IBGE =====")

res_pb_parquet_path <- file.path(
  TOPALS_FIT_DIR,
  sprintf("resumo_fits_%s_municipio_imediata_pi_ibge.parquet", UF_ALVO)
)

if (!file.exists(res_pb_parquet_path)) {
  stop("Não encontrei o resumo dos fits em parquet: ", res_pb_parquet_path)
}

res_pb <- arrow::read_parquet(res_pb_parquet_path) |>
  dplyr::mutate(
    ano   = as.integer(ano),
    nivel = as.character(nivel),
    uf    = as.character(uf),
    sexo  = as.character(sexo)
  )

valid_cases <- res_pb %>%
  dplyr::filter(
    sexo == "b",
    uf == UF_ALVO,
    nivel %in% c("municipio", "imediata"),
    !is.na(fitfile),
    file.exists(fitfile)
  ) %>%
  dplyr::arrange(ano, nivel)

if (nrow(valid_cases) == 0L) {
  stop("Nenhum caso válido encontrado em res_pb para UF = ", UF_ALVO,
       ", sexo='b'.")
}

compute_e0_pi_case <- function(row_case) {
  row_case <- as.list(row_case)
  
  fitfile    <- row_case$fitfile
  this_ano   <- row_case$ano
  this_nivel <- row_case$nivel
  this_sexo  <- row_case$sexo
  this_uf    <- row_case$uf
  
  message(
    "Calculando e0 & pi | ano=", this_ano,
    " | nivel=", this_nivel,
    " | sexo=", this_sexo,
    " | UF=", this_uf,
    "\n  Arquivo: ", fitfile
  )
  
  load(fitfile)  # fit, stanDataList, case_meta
  
  post_alpha <- rstan::extract(fit, pars = "alpha")$alpha
  dims <- dim(post_alpha)
  S <- dims[1]
  K <- dims[2]
  R <- dims[3]
  
  post_pi <- rstan::extract(fit, pars = "pi")$pi
  
  B            <- stanDataList$B
  std_schedule <- stanDataList$std_schedule
  ages         <- case_meta$ages
  regions      <- case_meta$regions
  
  df_label <- base_muni %>%
    dplyr::filter(
      ano == this_ano,
      uf_sigla == this_uf
    ) %>%
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
    pi_qs <- as.numeric(quantile(pi_s,  probs = c(0.10, 0.50, 0.90), na.rm = TRUE))
    
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
      ano        = this_ano,
      nivel      = this_nivel,
      sexo       = this_sexo,
      uf         = this_uf,
      uf_sigla   = lab_j$uf_sigla[1],
      nome_uf    = lab_j$nome_uf[1],
      regiao_code = lab_j$regiao_code[1],
      regiao_nome = lab_j$regiao_nome[1],
      code_muni6        = lab_j$code_muni6[1],
      nome_muni         = lab_j$nome_muni[1],
      rgi_imediata_code = lab_j$rgi_imediata_code[1],
      rgi_imediata_nome = lab_j$rgi_imediata_nome[1],
      region_code  = as.character(code_j),
      region_name  = dplyr::case_when(
        this_nivel == "municipio"  ~ lab_j$nome_muni[1],
        this_nivel == "imediata"   ~ lab_j$rgi_imediata_nome[1],
        TRUE                       ~ NA_character_
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

e0_pi_all <- purrr::map_dfr(
  seq_len(nrow(valid_cases)),
  ~ compute_e0_pi_case(valid_cases[.x, ])
)

cat("\nSem calibração ex-post: mantendo e0_p* direto do Stan.\n")
print(
  e0_pi_all %>%
    dplyr::filter(nivel == "municipio") %>%
    dplyr::group_by(ano) %>%
    dplyr::summarise(
      e0_mean = mean(e0_p50, na.rm = TRUE),
      e0_med  = median(e0_p50, na.rm = TRUE),
      .groups = "drop"
    )
)

# Caminhos de saída (tudo em PARQUET, na pasta resultados)
e0_pi_all_path  <- file.path(
  RES_DB_DIR,
  sprintf("e0_pi_regioes_municipio_imediata_sexb_%s_2000_2023_ibge.parquet", UF_ALVO)
)
e0_pi_muni_path <- file.path(
  RES_DB_DIR,
  sprintf("e0_pi_municipios_sexb_%s_2000_2023_ibge.parquet", UF_ALVO)
)
e0_pi_imed_path <- file.path(
  RES_DB_DIR,
  sprintf("e0_pi_rgi_imediata_sexb_%s_2000_2023_ibge.parquet", UF_ALVO)
)

arrow::write_parquet(e0_pi_all,  e0_pi_all_path)
arrow::write_parquet(
  e0_pi_all %>% dplyr::filter(nivel == "municipio"),
  e0_pi_muni_path
)
arrow::write_parquet(
  e0_pi_all %>% dplyr::filter(nivel == "imediata"),
  e0_pi_imed_path
)

message("Arquivos e0/pi salvos em PARQUET:")
message(" - ", e0_pi_all_path)
message(" - ", e0_pi_muni_path)
message(" - ", e0_pi_imed_path)

###############################################################################
# 7) 03_diag_mx_topals_ibge – Define reconstruct_mx_muni (usado no shrink)
###############################################################################

message("\n===== [03] Definindo funções de diagnóstico (reconstruct_mx_muni) =====")

res_pb <- readRDS(res_pb_rds_path)

reconstruct_mx_muni <- function(ano_target, code_muni6_target) {
  row_case <- res_pb %>%
    dplyr::filter(
      ano   == ano_target,
      nivel == "municipio",
      uf    == UF_ALVO
    ) %>%
    dplyr::slice(1)
  
  if (nrow(row_case) == 0L) {
    stop("Não achei fit para ano=", ano_target,
         " nivel=municipio, UF=", UF_ALVO, " em res_pb.")
  }
  
  fitfile <- row_case$fitfile
  if (!file.exists(fitfile)) {
    stop("fitfile não existe: ", fitfile)
  }
  
  message("\n[reconstruct_mx_muni] ano=", ano_target,
          " muni=", code_muni6_target,
          " | carregando fit:\n  ", fitfile)
  
  load(fitfile)  # fit, stanDataList, case_meta
  
  post_alpha <- rstan::extract(fit, pars = "alpha")$alpha  # S x K x R
  B          <- stanDataList$B                             # A x K
  std_sched  <- stanDataList$std_schedule                  # A
  N_mat      <- stanDataList$N                             # A x R
  D_mat      <- stanDataList$D                             # A x R
  ages       <- case_meta$ages                             # 0:100
  regions    <- case_meta$regions                          # códigos
  
  j <- which(regions == code_muni6_target)
  if (length(j) == 0L) {
    stop("Municipio code_muni6=", code_muni6_target,
         " não encontrado em case_meta$regions desse fit.")
  }
  if (length(j) > 1L) {
    warning("Mais de um índice para esse code_muni6; usando o primeiro.")
    j <- j[1]
  }
  
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
    ano         = ano_target,
    code_muni6  = code_muni6_target,
    idade       = ages,
    mx_ibge     = mx_ibge,
    mx_raw      = mx_raw,
    mx_topals   = mx_topals_med
  )
  
  e0_summary <- tibble(
    ano        = ano_target,
    code_muni6 = code_muni6_target,
    e0_ibge    = e0_ibge,
    e0_raw     = e0_raw,
    e0_topals  = e0_topals
  )
  
  list(
    schedules = schedules,
    e0_summary = e0_summary
  )
}

###############################################################################
# 8) 05B_mx_post_e0 – Shrink ex-post + NMX final + mapas + tabela de vida
###############################################################################

message("\n===== [05B] Shrink ex-post do e0 + NMX final + mapas e0/e60 =====")

# Leitura de e0/pi municipal (parquet)
e0_muni_path <- file.path(
  RES_DB_DIR,
  sprintf("e0_pi_municipios_sexb_%s_2000_2023_ibge.parquet", UF_ALVO)
)

# Diretórios específicos da etapa 05B dentro da estrutura padronizada
FIG_DIR_05B <- file.path(RES_FIG_DIR, "05B_mx_post_e0")
DB_DIR_05B  <- file.path(RES_DB_DIR,  "05B_mx_post_e0")
IND_DIR_05B <- file.path(RES_IND_DIR, "05B_mx_post_e0")

dir.create(FIG_DIR_05B, showWarnings = FALSE, recursive = TRUE)
dir.create(DB_DIR_05B,  showWarnings = FALSE, recursive = TRUE)
dir.create(IND_DIR_05B, showWarnings = FALSE, recursive = TRUE)

e0_muni <- arrow::read_parquet(e0_muni_path)
anos_alvo <- sort(unique(e0_muni$ano))

# e0 alvo do IBGE (UF_ALVO)
tab_e0_ibge <- tibble(
  ano     = anos_alvo,
  e0_ibge = purrr::map_dbl(anos_alvo, ~ e0_ibge_from_tabua(.x, UF_ALVO))
)

# Base para shrink (antes do ajuste)
e0_muni_base <- e0_muni %>%
  dplyr::left_join(tab_e0_ibge, by = "ano") %>%
  dplyr::mutate(
    delta_e0 = e0_p50 - e0_ibge
  )

# Função de tábua simples usada no ajuste (para e0)
calc_e0_from_mx_tbl <- function(mx_tbl, mx_col = "mx_topals") {
  df <- mx_tbl %>%
    dplyr::arrange(idade)
  
  idade <- df$idade
  mx    <- df[[mx_col]]
  
  n <- c(diff(idade), 1)
  
  ax <- rep(0.5, length(mx))
  if (length(ax) > 0) ax[1] <- 0.3
  
  qx <- (n * mx) / (1 + (n - ax) * mx)
  qx[length(qx)] <- 1.0
  
  lx <- numeric(length(mx))
  lx[1] <- 100000
  
  for (i in seq_len(length(mx) - 1)) {
    lx[i + 1] <- lx[i] * (1 - qx[i])
  }
  
  dx <- lx * qx
  
  Lx <- numeric(length(mx))
  if (length(mx) > 1) {
    Lx[1:(length(mx) - 1)] <- n[1:(length(mx) - 1)] * lx[2:length(mx)] +
      ax[1:(length(mx) - 1)] * dx[1:(length(mx) - 1)]
  }
  Lx[length(mx)] <- lx[length(mx)] / mx[length(mx)]
  
  Tx <- rev(cumsum(rev(Lx)))
  ex <- Tx / lx
  
  ex[1]
}

# Estatísticas de delta por ano e escala de shrink
delta_stats <- e0_muni_base %>%
  dplyr::group_by(ano) %>%
  dplyr::summarise(
    delta_med = median(delta_e0, na.rm = TRUE),
    .groups   = "drop"
  ) %>%
  dplyr::mutate(
    escala_ano = dplyr::case_when(
      abs(delta_med) <= 1 ~ 1,
      TRUE              ~ pmax(0.25, 1 / abs(delta_med))
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
    TRUE        ~ 0.6
  )
}

compute_w <- function(e0_topals,
                      e0_ibge,
                      ano,
                      delta_hi  = 5,
                      delta_cap = 10) {
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
  w <- pmin(pmax(w, 0), 0.95)
  w
}

# Pegador de tabela de mx a partir de reconstruct_mx_muni
get_mx_tbl_from_reconstruct <- function(ano, code_muni6) {
  res <- reconstruct_mx_muni(ano, code_muni6)
  
  if (inherits(res, c("data.frame", "tbl_df", "tbl"))) {
    mx_tbl <- res
  } else if (is.list(res)) {
    if (!is.null(res$mx_tbl) && inherits(res$mx_tbl, c("data.frame", "tbl_df", "tbl"))) {
      mx_tbl <- res$mx_tbl
    } else if (!is.null(res$mx) && inherits(res$mx, c("data.frame", "tbl_df", "tbl"))) {
      mx_tbl <- res$mx
    } else if (!is.null(res$schedules) && inherits(res$schedules, c("data.frame", "tbl_df", "tbl"))) {
      mx_tbl <- res$schedules
    } else {
      idx_df <- which(vapply(res, inherits, logical(1),
                             what = c("data.frame", "tbl_df", "tbl")))
      if (length(idx_df) > 0) {
        mx_tbl <- res[[idx_df[1]]]
      } else {
        stop("reconstruct_mx_muni() não retornou data.frame/tibble identificável.")
      }
    }
  } else {
    stop("Objeto retornado por reconstruct_mx_muni() não é lista nem data.frame.")
  }
  
  if (!"idade" %in% names(mx_tbl)) {
    stop("Tabela de mx não tem coluna 'idade'. Nomes: ",
         paste(names(mx_tbl), collapse = ", "))
  }
  
  mx_tbl
}

get_mx_col_name <- function(mx_tbl) {
  cn <- names(mx_tbl)
  candidatos <- c("mx_topals", "mx", "mx_fit", "mx_hat", "mx_total")
  cand_encontrados <- candidatos[candidatos %in% cn]
  if (length(cand_encontrados) == 0) {
    stop("Não encontrei coluna de mortalidade em mx_tbl. Nomes disponíveis: ",
         paste(cn, collapse = ", "))
  }
  cand_encontrados[1]
}

calc_e0_scaled <- function(logk, mx_tbl, mx_col) {
  mx_tmp <- mx_tbl
  mx_tmp$mx_scaled <- mx_tbl[[mx_col]] * exp(logk)
  calc_e0_from_mx_tbl(mx_tmp, mx_col = "mx_scaled")
}

# Função geral para montar tabela de vida completa a partir de mx
build_life_table_from_mx_tbl <- function(mx_tbl, mx_col = "mx_nmx_final") {
  df <- mx_tbl %>%
    dplyr::arrange(idade)
  
  idade <- df$idade
  mx    <- df[[mx_col]]
  
  n <- c(diff(idade), 1L)
  
  ax <- rep(0.5, length(mx))
  if (length(ax) > 0) ax[1] <- 0.3
  
  qx <- (n * mx) / (1 + (n - ax) * mx)
  qx[length(qx)] <- 1
  
  lx <- numeric(length(mx))
  lx[1] <- 100000
  
  if (length(mx) > 1) {
    for (i in seq_len(length(mx) - 1)) {
      lx[i + 1] <- lx[i] * (1 - qx[i])
    }
  }
  
  dx <- lx * qx
  
  Lx <- numeric(length(mx))
  if (length(mx) > 1) {
    Lx[1:(length(mx) - 1)] <- n[1:(length(mx) - 1)] * lx[2:length(mx)] +
      ax[1:(length(mx) - 1)] * dx[1:(length(mx) - 1)]
  }
  Lx[length(mx)] <- lx[length(mx)] / mx[length(mx)]
  
  Tx <- rev(cumsum(rev(Lx)))
  ex <- Tx / lx
  
  tibble::tibble(
    idade = idade,
    n     = n,
    mx    = mx,
    qx    = qx,
    lx    = lx,
    dx    = dx,
    Lx    = Lx,
    Tx    = Tx,
    ex    = ex
  )
}

# Grid de municípios (ano x muni)
munis_grid <- e0_muni_base %>%
  dplyr::distinct(ano, code_muni6) %>%
  dplyr::arrange(ano, code_muni6)

message("Iniciando shrink ex-post de mx para municípios e construção do NMX final...")

# Listas para:
#  - shrink_res: resultados de e0 pós-shrink e fatores k
#  - mx_nmx_final_list: NMX final por idade para cada ano x município
shrink_rows        <- vector("list", nrow(munis_grid))
mx_nmx_final_list  <- vector("list", nrow(munis_grid))

for (idx in seq_len(nrow(munis_grid))) {
  ano_i        <- as.integer(munis_grid$ano[idx])
  code_muni_i  <- as.integer(munis_grid$code_muni6[idx])
  
  row_muni <- e0_muni_base %>%
    dplyr::filter(ano == !!ano_i, code_muni6 == !!code_muni_i) %>%
    dplyr::slice(1)
  
  if (nrow(row_muni) == 0L) {
    shrink_rows[[idx]] <- tibble(
      ano            = ano_i,
      code_muni6     = code_muni_i,
      e0_p50_post    = NA_real_,
      w              = 0,
      k_factor       = 1,
      shrink_applied = FALSE
    )
    mx_nmx_final_list[[idx]] <- NULL
    next
  }
  
  e0_topals <- row_muni$e0_p50
  e0_ibge   <- row_muni$e0_ibge
  
  # Peso de shrink baseado no desvio em relação ao IBGE
  w_i <- compute_w(e0_topals, e0_ibge, ano_i)
  
  # Tentativa de reconstruir curva de mx (TOPALS + pi) para esse município
  mx_tbl <- tryCatch(
    get_mx_tbl_from_reconstruct(ano_i, code_muni_i),
    error = function(e) {
      message("Falha get_mx_tbl_from_reconstruct para ano=", ano_i,
              ", muni=", code_muni_i, " -> ", conditionMessage(e),
              ". Mantendo e0 original (sem NMX explícito).")
      NULL
    }
  )
  
  if (is.null(mx_tbl)) {
    shrink_rows[[idx]] <- tibble(
      ano            = ano_i,
      code_muni6     = code_muni_i,
      e0_p50_post    = e0_topals,
      w              = w_i,
      k_factor       = 1,
      shrink_applied = FALSE
    )
    mx_nmx_final_list[[idx]] <- NULL
    next
  }
  
  mx_col <- tryCatch(
    get_mx_col_name(mx_tbl),
    error = function(e) {
      message("Não consegui identificar coluna mx para ano=", ano_i,
              ", muni=", code_muni_i, " -> ", conditionMessage(e),
              ". Mantendo e0 original.")
      NA_character_
    }
  )
  
  if (is.na(mx_col)) {
    shrink_rows[[idx]] <- tibble(
      ano            = ano_i,
      code_muni6     = code_muni_i,
      e0_p50_post    = e0_topals,
      w              = w_i,
      k_factor       = 1,
      shrink_applied = FALSE
    )
    mx_nmx_final_list[[idx]] <- NULL
    next
  }
  
  # Cálculo do alvo de e0 pós-shrink
  e0_target <- e0_topals
  shrink_ok <- FALSE
  k_factor  <- 1
  e0_post   <- e0_topals
  
  if (!is.na(w_i) && w_i > 0 && is.finite(e0_ibge) && is.finite(e0_topals)) {
    e0_target_tmp <- e0_topals - w_i * (e0_topals - e0_ibge)
    
    if (is.finite(e0_target_tmp) && e0_target_tmp < e0_topals) {
      e0_target <- e0_target_tmp
      
      f <- function(logk) {
        calc_e0_scaled(logk, mx_tbl, mx_col) - e0_target
      }
      
      f0 <- tryCatch(f(0), error = function(e) NA_real_)
      if (is.finite(f0)) {
        hi   <- log(2)
        f_hi <- tryCatch(f(hi), error = function(e) NA_real_)
        iter <- 0
        while (is.finite(f_hi) && f_hi > 0 && iter < 10) {
          hi   <- hi + log(2)
          f_hi <- tryCatch(f(hi), error = function(e) NA_real_)
          iter <- iter + 1
        }
        
        if (!is.finite(f_hi) || f_hi > 0) {
          k_factor <- exp(hi)
          mx_tbl2  <- mx_tbl
          mx_tbl2$mx_post <- mx_tbl2[[mx_col]] * k_factor
          e0_post  <- calc_e0_from_mx_tbl(mx_tbl2, mx_col = "mx_post")
          shrink_ok <- TRUE
        } else {
          root <- tryCatch(
            uniroot(f, lower = 0, upper = hi),
            error = function(e) NULL
          )
          
          if (is.null(root)) {
            k_factor <- exp(hi)
            mx_tbl2  <- mx_tbl
            mx_tbl2$mx_post <- mx_tbl2[[mx_col]] * k_factor
            e0_post  <- calc_e0_from_mx_tbl(mx_tbl2, mx_col = "mx_post")
            shrink_ok <- TRUE
          } else {
            logk     <- root$root
            k_factor <- exp(logk)
            mx_tbl2  <- mx_tbl
            mx_tbl2$mx_post <- mx_tbl2[[mx_col]] * k_factor
            e0_post  <- calc_e0_from_mx_tbl(mx_tbl2, mx_col = "mx_post")
            shrink_ok <- TRUE
          }
        }
      }
    }
  }
  
  # NMX final por idade simples = mx_topals (ou equivalente) * k_factor
  mx_tbl_final <- mx_tbl %>%
    dplyr::mutate(
      mx_nmx_final = .data[[mx_col]] * k_factor
    )
  
  # Acrescentar rótulos do município/UF a partir de e0_muni_base
  mx_tbl_final <- mx_tbl_final %>%
    dplyr::mutate(
      uf              = row_muni$uf[1],
      uf_sigla        = row_muni$uf_sigla[1],
      nome_uf         = row_muni$nome_uf[1],
      regiao_code     = row_muni$regiao_code[1],
      regiao_nome     = row_muni$regiao_nome[1],
      rgi_imediata_code = row_muni$rgi_imediata_code[1],
      rgi_imediata_nome = row_muni$rgi_imediata_nome[1],
      code_muni6      = row_muni$code_muni6[1],
      nome_muni       = row_muni$nome_muni[1]
    )
  
  shrink_rows[[idx]] <- tibble(
    ano            = ano_i,
    code_muni6     = code_muni_i,
    e0_p50_post    = e0_post,
    w              = w_i,
    k_factor       = k_factor,
    shrink_applied = shrink_ok
  )
  
  mx_nmx_final_list[[idx]] <- mx_tbl_final
}

message("Shrink concluído. Montando base consolidada de NMX final...")

shrink_res <- dplyr::bind_rows(shrink_rows)

# Base NMX final para todas as idades e municípios
mx_nmx_final_all <- mx_nmx_final_list %>%
  purrr::compact() %>%
  dplyr::bind_rows() %>%
  dplyr::arrange(ano, code_muni6, idade)

# Salvar NMX final (por idade simples) em parquet – SAÍDA CHAVE
nmx_final_path <- file.path(
  DB_DIR_05B,
  sprintf("nmx_final_municipios_idade_simples_%s.parquet", UF_ALVO)
)
arrow::write_parquet(mx_nmx_final_all, nmx_final_path)
message("NMX final por idade simples salvo em: ", nmx_final_path)

# Integrar e0 pós-shrink à base municipal
e0_muni_post <- e0_muni_base %>%
  dplyr::left_join(shrink_res, by = c("ano", "code_muni6")) %>%
  dplyr::mutate(
    e0_p50_post = dplyr::if_else(is.na(e0_p50_post), e0_p50, e0_p50_post),
    delta_post  = e0_p50_post - e0_ibge
  )

# Salvar e0 municipal pós-shrink em parquet
e0_post_path <- file.path(DB_DIR_05B, "e0_municipios_post_shrink.parquet")
arrow::write_parquet(e0_muni_post, e0_post_path)

# Distribuição de e0 antes/depois do shrink
tab_dist_e0 <- e0_muni_post %>%
  dplyr::group_by(ano) %>%
  dplyr::summarise(
    e0_min_orig = min(e0_p50, na.rm = TRUE),
    e0_p10_orig = quantile(e0_p50, 0.10, na.rm = TRUE),
    e0_med_orig = median(e0_p50, na.rm = TRUE),
    e0_p90_orig = quantile(e0_p50, 0.90, na.rm = TRUE),
    e0_max_orig = max(e0_p50, na.rm = TRUE),
    
    e0_min_post = min(e0_p50_post, na.rm = TRUE),
    e0_p10_post = quantile(e0_p50_post, 0.10, na.rm = TRUE),
    e0_med_post = median(e0_p50_post, na.rm = TRUE),
    e0_p90_post = quantile(e0_p50_post, 0.90, na.rm = TRUE),
    e0_max_post = max(e0_p50_post, na.rm = TRUE),
    .groups = "drop"
  )

print(tab_dist_e0)

tab_dist_e0_path <- file.path(IND_DIR_05B, "dist_e0_orig_vs_post.parquet")
arrow::write_parquet(tab_dist_e0, tab_dist_e0_path)

# Distribuição dos desvios delta (e0_topals - e0_ibge) vs delta pós-shrink
tab_delta <- e0_muni_post %>%
  dplyr::group_by(ano) %>%
  dplyr::summarise(
    delta_min_orig = min(delta_e0, na.rm = TRUE),
    delta_p10_orig = quantile(delta_e0, 0.10, na.rm = TRUE),
    delta_med_orig = median(delta_e0, na.rm = TRUE),
    delta_p90_orig = quantile(delta_e0, 0.90, na.rm = TRUE),
    delta_max_orig = max(delta_e0, na.rm = TRUE),
    
    delta_min_post = min(delta_post, na.rm = TRUE),
    delta_p10_post = quantile(delta_post, 0.10, na.rm = TRUE),
    delta_med_post = median(delta_post, na.rm = TRUE),
    delta_p90_post = quantile(delta_post, 0.90, na.rm = TRUE),
    delta_max_post = max(delta_post, na.rm = TRUE),
    .groups = "drop"
  )

print(tab_delta)

tab_delta_path <- file.path(IND_DIR_05B, "delta_e0_orig_vs_post.parquet")
arrow::write_parquet(tab_delta, tab_delta_path)

# Flags de outliers
e0_muni_post_flag <- e0_muni_post %>%
  dplyr::mutate(
    flag_e0_low   = e0_p50_post < 69,
    flag_e0_high  = e0_p50_post > 80,
    flag_delta_hi = delta_post >  2,
    flag_delta_lo = delta_post < -2
  )

outliers_report <- e0_muni_post_flag %>%
  dplyr::filter(flag_e0_low | flag_e0_high | flag_delta_hi | flag_delta_lo) %>%
  dplyr::arrange(ano, dplyr::desc(e0_p50_post)) %>%
  dplyr::select(
    ano, code_muni6, nome_muni,
    e0_p50, e0_p50_post, e0_ibge,
    delta_e0, delta_post,
    flag_e0_low, flag_e0_high,
    flag_delta_hi, flag_delta_lo
  )

outliers_path <- file.path(IND_DIR_05B, "outliers_e0_post.parquet")
arrow::write_parquet(outliers_report, outliers_path)

###############################################################################
# 8.1) TABELA DE VIDA COMPLETA (idade simples) POR MUNICÍPIO – USANDO NMX FINAL
###############################################################################

message("Construindo tabela de vida completa (idade simples) para todos os municípios...")

vida_list <- mx_nmx_final_all %>%
  dplyr::group_by(
    ano, uf, uf_sigla, nome_uf,
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
        ano, uf, uf_sigla, nome_uf,
        code_muni6, nome_muni,
        regiao_code, regiao_nome,
        rgi_imediata_code, rgi_imediata_nome
      )
    
    life_tbl <- build_life_table_from_mx_tbl(df, mx_col = "mx_nmx_final")
    
    meta_rep <- meta[rep(1, nrow(life_tbl)), ]
    dplyr::bind_cols(meta_rep, life_tbl)
  }
)

vida_path <- file.path(
  DB_DIR_05B,
  sprintf("tabela_vida_municipios_idade_simples_%s.parquet", UF_ALVO)
)
arrow::write_parquet(vida_muni_all, vida_path)
message("Tabela de vida completa (idade simples) salva em: ", vida_path)

# e60 municipal pós-shrink (a partir da tábua final)
e60_muni_post <- vida_muni_all %>%
  dplyr::filter(idade == 60L) %>%
  dplyr::select(
    ano, uf, uf_sigla, nome_uf,
    code_muni6, nome_muni,
    regiao_code, regiao_nome,
    rgi_imediata_code, rgi_imediata_nome,
    e60_post = ex
  )

# e60 alvo IBGE UF
tab_e60_ibge <- tibble(
  ano      = anos_alvo,
  e60_ibge = purrr::map_dbl(anos_alvo, ~ e60_ibge_from_tabua(.x, UF_ALVO))
)

###############################################################################
# 8.2) SELEÇÃO DE MUNICÍPIOS FOCO (se não houver objeto munis_foco)
###############################################################################

if (!exists("munis_foco")) {
  message("Objeto 'munis_foco' não encontrado. Selecionando 4 municípios de porte distinto...")
  
  # Perfil de tamanho populacional médio por município (na UF alvo)
  pop_info <- base_muni %>%
    dplyr::filter(uf_sigla == UF_ALVO) %>%
    dplyr::group_by(
      ano, code_muni6, nome_muni,
      rgi_imediata_code, rgi_imediata_nome
    ) %>%
    dplyr::summarise(
      pop_total = sum(pop_ambos, na.rm = TRUE),
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
    cand <- pop_info %>%
      dplyr::filter(porte_quartil == q)
    
    # tenta pegar RGIs diferentes
    if (nrow(munis_sel) > 0) {
      cand <- cand %>%
        dplyr::filter(!rgi_imediata_code %in% munis_sel$rgi_imediata_code)
      if (nrow(cand) == 0) {
        cand <- pop_info %>%
          dplyr::filter(porte_quartil == q)
      }
    }
    
    muni_q <- cand %>%
      dplyr::slice_sample(n = 1)
    
    munis_sel <- dplyr::bind_rows(munis_sel, muni_q)
  }
  
  # Garante que sempre exista 'nome_curto'
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
  
  # Checagem mínima
  if (!"code_muni6" %in% names(munis_foco)) {
    stop("O objeto 'munis_foco' precisa ter uma coluna 'code_muni6'.")
  }
  
  # Se não tiver 'nome_curto', cria a partir de 'nome_muni' (se existir),
  # senão usa o próprio código como rótulo
  if (!"nome_curto" %in% names(munis_foco)) {
    if ("nome_muni" %in% names(munis_foco)) {
      munis_foco <- munis_foco %>%
        dplyr::mutate(nome_curto = nome_muni)
    } else {
      munis_foco <- munis_foco %>%
        dplyr::mutate(nome_curto = as.character(code_muni6))
    }
  }
  
  print(munis_foco)
}

###############################################################################
# 8.3) SÉRIE DE e0 PARA MUNICÍPIOS FOCO (TOPALS vs pós-shrink vs IBGE)
###############################################################################

serie_foco <- e0_muni_post_flag %>%
  dplyr::inner_join(munis_foco, by = "code_muni6")

# ---------------------------------------------------------------------------
# 8.3.1) Figura comparando TODOS os municípios foco
# ---------------------------------------------------------------------------

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

estado_line <- serie_foco %>%
  dplyr::distinct(ano, e0_ibge)

p_serie_foco <- ggplot2::ggplot(
  serie_foco_long,
  ggplot2::aes(
    x        = ano,
    y        = e0,
    color    = nome_curto,
    linetype = versao
  )
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
    title    = paste0("e0 municipal ao longo do tempo — municípios foco (", UF_ALVO, ")"),
    subtitle = "Linhas coloridas: municípios (original vs pós-shrink) • Linha tracejada cinza: e0 estadual (IBGE)",
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
  paste0("serie_e0_munis_foco_orig_vs_post_", UF_ALVO, ".png")
)

ggplot2::ggsave(fig_serie_foco_path, p_serie_foco,
                width = 8, height = 4.5, dpi = 300)

message("Figura de série e0 (municípios foco, comparação conjunta) salva em: ", fig_serie_foco_path)

# ---------------------------------------------------------------------------
# 8.3.2) Figuras INDIVIDUAIS de e0 para cada município foco
# ---------------------------------------------------------------------------

for (i in seq_len(nrow(munis_foco))) {
  muni_i <- munis_foco$code_muni6[i]
  nome_i <- munis_foco$nome_curto[i]
  
  serie_m <- serie_foco %>%
    dplyr::filter(code_muni6 == muni_i)
  
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
  
  estado_line_m <- serie_m %>%
    dplyr::distinct(ano, e0_ibge)
  
  p_m <- ggplot2::ggplot(
    serie_m_long,
    ggplot2::aes(
      x        = ano,
      y        = e0,
      color    = versao,
      linetype = versao
    )
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
      title    = paste0("e0 municipal ao longo do tempo — ", nome_i, " (", UF_ALVO, ")"),
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
  
  fig_m_path <- file.path(
    FIG_DIR_05B,
    paste0("serie_e0_", UF_ALVO, "_", muni_i, ".png")
  )
  
  ggplot2::ggsave(fig_m_path, p_m,
                  width = 7, height = 4.5, dpi = 300)
  message("Figura de série e0 salva para ", nome_i, ": ", fig_m_path)
}

###############################################################################
# 8.4) SÉRIE DE e60 PARA MUNICÍPIOS FOCO (pós-shrink vs IBGE)
###############################################################################

# e60 municipal pós-shrink + e60 IBGE
e60_muni_foco <- e60_muni_post %>%
  dplyr::inner_join(munis_foco, by = "code_muni6") %>%
  dplyr::left_join(tab_e60_ibge, by = "ano")

# ---------------------------------------------------------------------------
# 8.4.1) Figura comparando TODOS os municípios foco
# ---------------------------------------------------------------------------

e60_foco_long <- e60_muni_foco %>%
  dplyr::select(ano, nome_curto, e60_post, e60_ibge) %>%
  tidyr::pivot_longer(
    cols      = c(e60_post),
    names_to  = "versao",
    values_to = "e60"
  ) %>%
  dplyr::mutate(
    versao = dplyr::recode(
      versao,
      e60_post = "TOPALS pós-shrink"
    )
  )

estado_e60 <- e60_muni_foco %>%
  dplyr::distinct(ano, e60_ibge)

p_serie_foco_e60 <- ggplot2::ggplot(
  e60_foco_long,
  ggplot2::aes(
    x        = ano,
    y        = e60,
    color    = nome_curto,
    linetype = versao
  )
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
    title    = paste0("e60 municipal ao longo do tempo — municípios foco (", UF_ALVO, ")"),
    subtitle = "Linhas coloridas: e60 pós-shrink • Linha tracejada cinza: e60 estadual (IBGE)",
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
  paste0("serie_e60_munis_foco_post_vs_ibge_", UF_ALVO, ".png")
)

ggplot2::ggsave(fig_serie_foco_e60_path, p_serie_foco_e60,
                width = 8, height = 4.5, dpi = 300)

message("Figura de série e60 (municípios foco, comparação conjunta) salva em: ", fig_serie_foco_e60_path)

# ---------------------------------------------------------------------------
# 8.4.2) Figuras INDIVIDUAIS de e60 para cada município foco
# ---------------------------------------------------------------------------

for (i in seq_len(nrow(munis_foco))) {
  muni_i <- munis_foco$code_muni6[i]
  nome_i <- munis_foco$nome_curto[i]
  
  e60_m <- e60_muni_foco %>%
    dplyr::filter(code_muni6 == muni_i)
  
  if (nrow(e60_m) == 0L) next
  
  e60_m_long <- e60_m %>%
    dplyr::select(ano, e60_post, e60_ibge) %>%
    tidyr::pivot_longer(
      cols      = c(e60_post),
      names_to  = "versao",
      values_to = "e60"
    ) %>%
    dplyr::mutate(
      versao = dplyr::recode(
        versao,
        e60_post = "TOPALS pós-shrink"
      )
    )
  
  estado_e60_m <- e60_m %>%
    dplyr::distinct(ano, e60_ibge)
  
  p_e60_m <- ggplot2::ggplot(
    e60_m_long,
    ggplot2::aes(
      x        = ano,
      y        = e60,
      color    = versao,
      linetype = versao
    )
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
      title    = paste0("e60 municipal ao longo do tempo — ", nome_i, " (", UF_ALVO, ")"),
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
  
  fig_e60_m_path <- file.path(
    FIG_DIR_05B,
    paste0("serie_e60_", UF_ALVO, "_", muni_i, ".png")
  )
  
  ggplot2::ggsave(fig_e60_m_path, p_e60_m,
                  width = 7, height = 4.5, dpi = 300)
  message("Figura de série e60 salva para ", nome_i, ": ", fig_e60_m_path)
}

###############################################################################
# 8.5) CURVAS DE log(mx) PARA MUNICÍPIOS FOCO (IBGE vs mediana estadual vs NMX muni)
###############################################################################

# Mediana estadual pós-shrink do NMX final (por ano, idade)
logmx_uf_mediana <- mx_nmx_final_all %>%
  dplyr::filter(uf_sigla == UF_ALVO) %>%
  dplyr::group_by(ano, idade) %>%
  dplyr::summarise(
    mx_mediana = median(mx_nmx_final, na.rm = TRUE),
    .groups    = "drop"
  ) %>%
  dplyr::mutate(logmx_mediana_uf = log(mx_mediana))

for (ano_k in intersect(ANOS_DIAG, anos_alvo)) {
  for (i in seq_len(nrow(munis_foco))) {
    muni_i <- munis_foco$code_muni6[i]
    nome_i <- munis_foco$nome_curto[i]
    
    # Curva NMX final do município
    df_muni <- mx_nmx_final_all %>%
      dplyr::filter(ano == ano_k, code_muni6 == muni_i) %>%
      dplyr::arrange(idade) %>%
      dplyr::mutate(logmx_muni = log(mx_nmx_final))
    
    if (nrow(df_muni) == 0L) next
    
    # Curva IBGE (std schedule suavizada)
    logmx_ibge <- make_std_schedule_ibge(
      UF_ALVO, ano_k,
      ages = unique(df_muni$idade)
    )
    df_ibge <- tibble::tibble(
      idade      = unique(df_muni$idade),
      logmx_ibge = as.numeric(logmx_ibge)
    )
    
    # Mediana estadual pós-shrink
    df_med_uf <- logmx_uf_mediana %>%
      dplyr::filter(ano == ano_k, idade %in% df_muni$idade)
    
    df_plot <- df_muni %>%
      dplyr::select(idade, logmx_muni) %>%
      dplyr::left_join(df_ibge, by = "idade") %>%
      dplyr::left_join(
        df_med_uf %>% dplyr::select(idade, logmx_mediana_uf),
        by = "idade"
      ) %>%
      tidyr::pivot_longer(
        cols      = c(logmx_muni, logmx_ibge, logmx_mediana_uf),
        names_to  = "serie",
        values_to = "logmx"
      ) %>%
      dplyr::mutate(
        serie = dplyr::recode(
          serie,
          logmx_muni       = paste0("Município (NMX final) — ", nome_i),
          logmx_ibge       = paste0("IBGE UF ", UF_ALVO),
          logmx_mediana_uf = paste0("Mediana estadual NMX final (", UF_ALVO, ")")
        )
      )
    
    p_logmx <- ggplot2::ggplot(
      df_plot,
      ggplot2::aes(
        x        = idade,
        y        = logmx,
        color    = serie,
        linetype = serie
      )
    ) +
      ggplot2::geom_line(size = 0.9) +
      ggplot2::geom_point(size = 1.3) +
      ggplot2::labs(
        title = paste0("Curva de log(mx) — ", nome_i, " (", UF_ALVO, "), ano ", ano_k),
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
      paste0("logmx_", UF_ALVO, "_", muni_i, "_", ano_k, ".png")
    )
    ggplot2::ggsave(fig_logmx_path, p_logmx, width = 7, height = 4.5, dpi = 300)
    message("Curva de log(mx) salva para ", nome_i, " — ano ", ano_k, ": ", fig_logmx_path)
  }
}

###############################################################################
# 8.6) MAPAS CHOROPLETH: e0 e e60 (apenas ANOS_DIAG)
###############################################################################

# Malha municipal da UF alvo
uf_muni_sf <- geobr::read_municipality(code_muni = UF_ALVO, year = 2020) |>
  dplyr::mutate(code_muni6 = as.integer(substr(code_muni, 1, 6)))
names(uf_muni_sf)[names(uf_muni_sf) == "geom"] <- "geometry"
uf_muni_sf <- sf::st_as_sf(uf_muni_sf)

uf_muni_df <- uf_muni_sf
class(uf_muni_df) <- setdiff(class(uf_muni_df), "sf")

# Intervalo de e0 para escala de cor (pós-shrink)
pb_range <- e0_muni_post_flag |>
  dplyr::filter(uf == UF_ALVO, sexo == "b", nivel == "municipio") |>
  dplyr::summarise(
    min_e0 = floor(stats::quantile(e0_p50_post, probs = 0.02, na.rm = TRUE)),
    max_e0 = ceiling(stats::quantile(e0_p50_post, probs = 0.98, na.rm = TRUE))
  )

min_e0 <- pb_range$min_e0
max_e0 <- pb_range$max_e0

# e60: intervalo para mapas
e60_range <- e60_muni_post |>
  dplyr::filter(uf_sigla == UF_ALVO) |>
  dplyr::summarise(
    min_e60 = floor(stats::quantile(e60_post, 0.02, na.rm = TRUE)),
    max_e60 = ceiling(stats::quantile(e60_post, 0.98, na.rm = TRUE))
  )

min_e60 <- e60_range$min_e60
max_e60 <- e60_range$max_e60

anos_mapa <- intersect(anos_alvo, ANOS_DIAG)

for (ano_k in anos_mapa) {
  # ----- MAPA e0 -----
  df_ano_e0 <- e0_muni_post_flag |>
    dplyr::filter(
      uf == UF_ALVO,
      sexo == "b",
      nivel == "municipio",
      ano == ano_k
    )
  
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
    ggplot2::geom_sf(
      data  = map_ano_e0,
      ggplot2::aes(fill = e0_p50_post),
      color = NA
    )
  
  if (nrow(out_e0) > 0) {
    cent   <- sf::st_centroid(out_e0$geometry)
    coords <- sf::st_coordinates(cent)
    out_e0 <- out_e0 |>
      dplyr::mutate(
        lon = coords[, 1],
        lat = coords[, 2]
      )
    
    p_mapa_e0 <- p_mapa_e0 +
      ggplot2::geom_point(
        data  = out_e0,
        ggplot2::aes(x = lon, y = lat),
        shape  = 24,
        fill   = "yellow",
        color  = "black",
        size   = 3,
        stroke = 0.4
      )
  }
  
  p_mapa_e0 <- p_mapa_e0 +
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
      title    = paste0("Esperança de vida ao nascer — ", UF_ALVO, ", ", ano_k, " (pós-shrink)"),
      subtitle = "Triângulos amarelos: municípios com e0 < 69 ou e0 > 80 anos (após ajuste)",
      x        = "lon",
      y        = "lat"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
      legend.position   = "bottom",
      legend.box        = "vertical",
      legend.title      = ggplot2::element_text(hjust = 0.5),
      legend.background = ggplot2::element_rect(fill = "white", colour = NA)
    )
  
  fig_mapa_e0_path <- file.path(
    FIG_DIR_05B,
    paste0("mapa_e0_", UF_ALVO, "_post_shrink_tri_outliers_", ano_k, ".png")
  )
  ggplot2::ggsave(fig_mapa_e0_path, p_mapa_e0, width = 6, height = 6, dpi = 300)
  message("Mapa e0 salvo em: ", fig_mapa_e0_path)
  
  # ----- MAPA e60 -----
  df_ano_e60 <- e60_muni_post |>
    dplyr::filter(
      uf_sigla == UF_ALVO,
      ano == ano_k
    )
  
  map_ano_e60 <- uf_muni_df |>
    dplyr::left_join(df_ano_e60, by = "code_muni6") |>
    sf::st_as_sf(sf_column_name = "geometry")
  
  p_mapa_e60 <- ggplot2::ggplot() +
    ggplot2::geom_sf(
      data  = map_ano_e60,
      ggplot2::aes(fill = e60_post),
      color = NA
    ) +
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
      title    = paste0("Esperança de vida aos 60 anos — ", UF_ALVO, ", ", ano_k, " (pós-shrink)"),
      x        = "lon",
      y        = "lat"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
      legend.position   = "bottom",
      legend.box        = "vertical",
      legend.title      = ggplot2::element_text(hjust = 0.5),
      legend.background = ggplot2::element_rect(fill = "white", colour = NA)
    )
  
  fig_mapa_e60_path <- file.path(
    FIG_DIR_05B,
    paste0("mapa_e60_", UF_ALVO, "_post_shrink_", ano_k, ".png")
  )
  ggplot2::ggsave(fig_mapa_e60_path, p_mapa_e60, width = 6, height = 6, dpi = 300)
  message("Mapa e60 salvo em: ", fig_mapa_e60_path)
}

cat("\n✅ PIPELINE COMPLETO (00b + 01 + 02 + 03 + 05B) CONCLUÍDO PARA UF = ", UF_ALVO, " ✅\n")
cat("   - NMX final (idade simples) por município em: ", nmx_final_path, "\n")
cat("   - Tabela de vida completa (idade simples) por município em: ", vida_path, "\n")
