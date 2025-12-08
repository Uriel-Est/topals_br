###############################################################################
# TOPALS + pi + IBGE – PIPELINE ÚNICO (00b + 01 + 02 + 03 + 05B)
#
# 1) CONFIG: defina BASE_DIR, UF_ALVO, anos e níveis e rode o script inteiro.
# 2) Pré-requisito: já ter rodado o 00_prep_topals.R para gerar:
#       00_prep_topals_output/bases_topals_preparadas.RData
#    com base_muni, base_muni_raw, pesos_80mais.
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
})

###############################################################################
# 0) CONFIGURAÇÃO GERAL (SÓ MEXE AQUI)
###############################################################################

BASE_DIR <- "C:/Users/uriel/Documents/UFPB Estatística/Demografia 1/Estudos Demográficos - PB/TOPALS"

UF_ALVO   <- "BA"                  # UF que você quer rodar
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

###############################################################################
# 1) 00b_build_tabua_ibge_uf.R  – Construir tabua_ibge_uf e salvar no .RData
###############################################################################

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

# Atualizar .RData com tabua_ibge_uf
if (file.exists(RDATA_PATH)) {
  message("\nCarregando ", RDATA_PATH, " para atualizar com tabua_ibge_uf...")
  load(RDATA_PATH)  # base_muni_raw, base_muni, pesos_80mais (se existirem)
  
  save(
    base_muni_raw,
    base_muni,
    pesos_80mais,
    tabua_ibge_uf,
    file = RDATA_PATH
  )
  
  message("Atualizei ", basename(RDATA_PATH),
          " para incluir o objeto 'tabua_ibge_uf'.")
} else {
  message("\nATENÇÃO: ", basename(RDATA_PATH), " ainda não existe.")
  message("Vou criar um novo .RData apenas com tabua_ibge_uf.")
  save(tabua_ibge_uf, file = RDATA_PATH)
}

tab_ibge_rdata <- file.path(OUTPUT_00_DIR, "tabua_ibge_uf_only.RData")
save(tabua_ibge_uf, file = tab_ibge_rdata)

cat("\n✅ Construção de 'tabua_ibge_uf' concluída com sucesso!\n")
cat("   - Linhas: ", nrow(tabua_ibge_uf), " (UF x ano x idade)\n", sep = "")
cat("   - Intervalo de anos em tabua_ibge_uf:",
    min(tabua_ibge_uf$ano), "a", max(tabua_ibge_uf$ano), "\n\n")

###############################################################################
# 2) Funções auxiliares globais (TOPALS + IBGE)
###############################################################################

# Reload para garantir base_muni, pesos_80mais, tabua_ibge_uf na memória
load(RDATA_PATH)

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

TOPALS_FIT_DIR <- file.path(
  BASE_DIR,
  "01_topals_fits_pi_ibge",
  sprintf("%s_municipio_imediata_pi_ibge", UF_ALVO)
)
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

res_pb_path <- file.path(
  TOPALS_FIT_DIR,
  sprintf("resumo_fits_%s_municipio_imediata_pi_ibge.rds", UF_ALVO)
)
saveRDS(results, res_pb_path)
message("Resumo dos fits salvo em: ", res_pb_path)

###############################################################################
# 6) 02_e0_pi_from_topals_ibge – Extrair e0/pi para UF_ALVO
###############################################################################

message("\n===== [02] Extraindo e0/pi dos fits TOPALS+pi+IBGE =====")

res_pb <- readRDS(res_pb_path)

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

# Caminhos de saída
out_path_all  <- file.path(
  BASE_DIR,
  sprintf("e0_pi_regioes_municipio_imediata_sexb_%s_2000_2023_ibge.csv", UF_ALVO)
)
out_path_muni <- file.path(
  BASE_DIR,
  sprintf("e0_pi_municipios_sexb_%s_2000_2023_ibge.csv", UF_ALVO)
)
out_path_imed <- file.path(
  BASE_DIR,
  sprintf("e0_pi_rgi_imediata_sexb_%s_2000_2023_ibge.csv", UF_ALVO)
)

readr::write_csv(e0_pi_all, out_path_all)
readr::write_csv(
  e0_pi_all %>% dplyr::filter(nivel == "municipio"),
  out_path_muni
)
readr::write_csv(
  e0_pi_all %>% dplyr::filter(nivel == "imediata"),
  out_path_imed
)

message("Arquivos salvos em:")
message(" - ", out_path_all)
message(" - ", out_path_muni)
message(" - ", out_path_imed)

###############################################################################
# 7) 03_diag_mx_topals_ibge – Define reconstruct_mx_muni (usado no shrink)
###############################################################################

message("\n===== [03] Definindo funções de diagnóstico (reconstruct_mx_muni) =====")

TOPALS_FIT_DIR <- file.path(
  BASE_DIR,
  "01_topals_fits_pi_ibge",
  sprintf("%s_municipio_imediata_pi_ibge", UF_ALVO)
)
res_pb_path <- file.path(
  TOPALS_FIT_DIR,
  sprintf("resumo_fits_%s_municipio_imediata_pi_ibge.rds", UF_ALVO)
)
res_pb <- readRDS(res_pb_path)

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
# 8) 05B_mx_post_e0 – Shrink ex-post + mapas de outliers
###############################################################################

message("\n===== [05B] Shrink ex-post do e0 + mapas de outliers =====")

e0_muni_path <- file.path(
  BASE_DIR,
  sprintf("e0_pi_municipios_sexb_%s_2000_2023_ibge.csv", UF_ALVO)
)

OUT_DIR <- file.path(BASE_DIR, sprintf("05B_mx_post_e0_%s", UF_ALVO))
FIG_DIR <- file.path(OUT_DIR, "figuras")
TAB_DIR <- file.path(OUT_DIR, "tabelas")

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(FIG_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(TAB_DIR, showWarnings = FALSE, recursive = TRUE)

e0_muni <- readr::read_csv(e0_muni_path, show_col_types = FALSE)
anos_alvo <- sort(unique(e0_muni$ano))

tab_e0_ibge <- tibble(
  ano     = anos_alvo,
  e0_ibge = purrr::map_dbl(anos_alvo, ~ e0_ibge_from_tabua(.x, UF_ALVO))
)

e0_muni_base <- e0_muni %>%
  dplyr::left_join(tab_e0_ibge, by = "ano") %>%
  dplyr::mutate(
    delta_e0 = e0_p50 - e0_ibge
  )

# Função de tábua simples
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

# Estatísticas de delta por ano e escala
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

munis_grid <- e0_muni_base %>%
  dplyr::distinct(ano, code_muni6)

message("Iniciando shrink ex-post de mx para municípios com delta_e0 > 0 ...")

shrink_res <- purrr::pmap_dfr(
  munis_grid,
  function(ano, code_muni6) {
    ano        <- as.integer(ano)
    code_muni6 <- as.integer(code_muni6)
    
    row_muni <- e0_muni_base %>%
      dplyr::filter(ano == !!ano, code_muni6 == !!code_muni6) %>%
      dplyr::slice(1)
    
    if (nrow(row_muni) == 0L) {
      return(tibble(
        ano            = ano,
        code_muni6     = code_muni6,
        e0_p50_post    = NA_real_,
        w              = 0,
        k_factor       = 1,
        shrink_applied = FALSE
      ))
    }
    
    e0_topals <- row_muni$e0_p50
    e0_ibge   <- row_muni$e0_ibge
    
    w <- compute_w(e0_topals, e0_ibge, ano)
    
    if (is.na(w) || w <= 0) {
      return(tibble(
        ano            = ano,
        code_muni6     = code_muni6,
        e0_p50_post    = e0_topals,
        w              = 0,
        k_factor       = 1,
        shrink_applied = FALSE
      ))
    }
    
    e0_target <- e0_topals - w * (e0_topals - e0_ibge)
    
    if (!is.finite(e0_target) || e0_target >= e0_topals) {
      return(tibble(
        ano            = ano,
        code_muni6     = code_muni6,
        e0_p50_post    = e0_topals,
        w              = w,
        k_factor       = 1,
        shrink_applied = FALSE
      ))
    }
    
    mx_tbl <- tryCatch(
      get_mx_tbl_from_reconstruct(ano, code_muni6),
      error = function(e) {
        message("Falha get_mx_tbl_from_reconstruct para ano=", ano,
                ", muni=", code_muni6, " -> ", conditionMessage(e),
                ". Mantendo e0 original.")
        NULL
      }
    )
    
    if (is.null(mx_tbl)) {
      return(tibble(
        ano            = ano,
        code_muni6     = code_muni6,
        e0_p50_post    = e0_topals,
        w              = w,
        k_factor       = 1,
        shrink_applied = FALSE
      ))
    }
    
    mx_col <- tryCatch(
      get_mx_col_name(mx_tbl),
      error = function(e) {
        message("Não consegui identificar coluna mx para ano=", ano,
                ", muni=", code_muni6, " -> ", conditionMessage(e),
                ". Mantendo e0 original.")
        NA_character_
      }
    )
    
    if (is.na(mx_col)) {
      return(tibble(
        ano            = ano,
        code_muni6     = code_muni6,
        e0_p50_post    = e0_topals,
        w              = w,
        k_factor       = 1,
        shrink_applied = FALSE
      ))
    }
    
    f <- function(logk) {
      calc_e0_scaled(logk, mx_tbl, mx_col) - e0_target
    }
    
    f0 <- tryCatch(f(0), error = function(e) NA_real_)
    if (!is.finite(f0)) {
      message("Problema ao calcular e0 em k=1 para ano=", ano,
              ", muni=", code_muni6, ". Mantendo e0 original.")
      return(tibble(
        ano            = ano,
        code_muni6     = code_muni6,
        e0_p50_post    = e0_topals,
        w              = w,
        k_factor       = 1,
        shrink_applied = FALSE
      ))
    }
    
    hi   <- log(2)
    f_hi <- tryCatch(f(hi), error = function(e) NA_real_)
    iter <- 0
    while (is.finite(f_hi) && f_hi > 0 && iter < 10) {
      hi   <- hi + log(2)
      f_hi <- tryCatch(f(hi), error = function(e) NA_real_)
      iter <- iter + 1
    }
    
    k_factor <- 1
    e0_post  <- e0_topals
    shrink_ok <- FALSE
    
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
    
    tibble(
      ano            = ano,
      code_muni6     = code_muni6,
      e0_p50_post    = e0_post,
      w              = w,
      k_factor       = k_factor,
      shrink_applied = shrink_ok
    )
  }
)

message("Shrink concluído.")

e0_muni_post <- e0_muni_base %>%
  dplyr::left_join(shrink_res, by = c("ano", "code_muni6")) %>%
  dplyr::mutate(
    e0_p50_post = dplyr::if_else(is.na(e0_p50_post), e0_p50, e0_p50_post),
    delta_post  = e0_p50_post - e0_ibge
  )

readr::write_csv(e0_muni_post, file.path(TAB_DIR, "e0_municipios_post_shrink.csv"))

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
readr::write_csv(tab_dist_e0, file.path(TAB_DIR, "dist_e0_orig_vs_post.csv"))

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
readr::write_csv(tab_delta, file.path(TAB_DIR, "delta_e0_orig_vs_post.csv"))

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

readr::write_csv(outliers_report, file.path(TAB_DIR, "outliers_e0_post.csv"))

# Série foco (só faz sentido pra PB, deixo condicional)
if (UF_ALVO == "PB") {
  munis_foco <- tibble(
    code_muni6 = c(250400L, 250750L, 251130L, 251230L),
    nome_curto = c("Campina Grande", "João Pessoa", "Piancó", "Princesa Isabel")
  )
  
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
      versao = dplyr::recode(versao,
                             e0_p50      = "TOPALS original",
                             e0_p50_post = "TOPALS pós-shrink")
    )
  
  estado_line <- serie_foco %>%
    dplyr::distinct(ano, e0_ibge)
  
  p_serie_foco <- ggplot(serie_foco_long,
                         aes(x = ano, y = e0,
                             color = nome_curto, linetype = versao)) +
    geom_line() +
    geom_point(size = 1.5) +
    geom_line(
      data = estado_line,
      aes(x = ano, y = e0_ibge),
      inherit.aes = FALSE,
      linetype = "dashed",
      color = "grey40"
    ) +
    labs(
      title    = "e0 municipal ao longo do tempo — municípios foco",
      subtitle = "Linhas coloridas: municípios (original vs pós-shrink) • Linha tracejada cinza: e0 estadual (IBGE)",
      x        = "Ano",
      y        = "e0 (anos)",
      color    = "Município",
      linetype = "Versão"
    ) +
    theme_minimal() +
    theme(
      plot.background = element_rect(fill = "white", colour = NA)
    )
  
  fig_serie_foco_path <- file.path(FIG_DIR, "serie_e0_munis_foco_orig_vs_post.png")
  ggsave(fig_serie_foco_path, p_serie_foco, width = 8, height = 4.5, dpi = 300)
}

# Mapas choropleth com triângulos para outliers
pb_muni_sf <- geobr::read_municipality(code_muni = UF_ALVO, year = 2020) |>
  dplyr::mutate(code_muni6 = as.integer(substr(code_muni, 1, 6)))
names(pb_muni_sf)[names(pb_muni_sf) == "geom"] <- "geometry"
pb_muni_sf <- sf::st_as_sf(pb_muni_sf)

pb_muni_df <- pb_muni_sf
class(pb_muni_df) <- setdiff(class(pb_muni_df), "sf")

pb_range <- e0_muni_post_flag |>
  dplyr::filter(uf == UF_ALVO, sexo == "b", nivel == "municipio") |>
  dplyr::summarise(
    min_e0 = floor(stats::quantile(e0_p50_post, probs = 0.02, na.rm = TRUE)),
    max_e0 = ceiling(stats::quantile(e0_p50_post, probs = 0.98, na.rm = TRUE))
  )

min_e0 <- pb_range$min_e0
max_e0 <- pb_range$max_e0

anos_mapa <- anos_alvo

for (ano_k in anos_mapa) {
  # dados do ano k para UF alvo, sexo ambos, nível município
  df_ano <- e0_muni_post_flag |>
    dplyr::filter(
      uf == UF_ALVO,
      sexo == "b",
      nivel == "municipio",
      ano == ano_k
    )
  
  # junta malha com dados (usando o data.frame sem classe sf)
  map_ano <- pb_muni_df |>
    dplyr::left_join(df_ano, by = "code_muni6") |>
    sf::st_as_sf(sf_column_name = "geometry")
  
  # flags de outlier (pós-shrink)
  map_ano <- map_ano |>
    dplyr::mutate(
      flag_e0_low  = e0_p50_post < 69,
      flag_e0_high = e0_p50_post > 80
    )
  
  out_ano <- map_ano |>
    dplyr::filter(flag_e0_low | flag_e0_high)
  
  # mapa base
  p_mapa <- ggplot2::ggplot() +
    ggplot2::geom_sf(
      data  = map_ano,
      ggplot2::aes(fill = e0_p50_post),
      color = NA
    )
  
  # só adiciona triângulo se tiver outlier
  if (nrow(out_ano) > 0) {
    cent   <- sf::st_centroid(out_ano$geometry)
    coords <- sf::st_coordinates(cent)
    out_ano <- out_ano |>
      dplyr::mutate(
        lon = coords[, 1],
        lat = coords[, 2]
      )
    
    p_mapa <- p_mapa +
      ggplot2::geom_point(
        data  = out_ano,
        ggplot2::aes(x = lon, y = lat),
        shape  = 24,
        fill   = "yellow",
        color  = "black",
        size   = 3,
        stroke = 0.4
      )
  }
  
  # resto igual
  p_mapa <- p_mapa +
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
  
  fig_mapa_path <- file.path(
    FIG_DIR,
    paste0("mapa_e0_", UF_ALVO, "_post_shrink_tri_outliers_", ano_k, ".png")
  )
  
  ggplot2::ggsave(fig_mapa_path, p_mapa, width = 6, height = 6, dpi = 300)
  message("Mapa salvo em: ", fig_mapa_path)
}

cat("\n✅ PIPELINE COMPLETO CONCLUÍDO PARA UF = ", UF_ALVO, " ✅\n")
