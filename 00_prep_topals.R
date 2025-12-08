###############################################################################
# 00_prep_topals.R
#
# Objetivo:
#   Preparar a base municipal de população e óbitos por idade simples para
#   aplicação de modelos TOPALS em nível de município (Brasil, 2000–2023/2024).
#
# Estrutura geral:
#   1. Carregamento de pacotes e definição de caminhos de entrada
#   2. Funções auxiliares:
#      - padronização de códigos (município, UF, região)
#      - leitura e tratamento da Cobertura SIM (XLS)
#      - leitura e tratamento da DTB 2024 Municípios (XLS)
#      - construção da base municipal-ano-idade (SIM + POP + Cobertura + DTB)
#      - construção de pesos decrescentes para redistribuição 80+
#      - redistribuição da população 80+ em idades 80–100
#   3. Execução:
#      - construção de base bruta (0–100, com 80+ concentrado em 80/100+)
#      - redistribuição de 80+ em 80–100, com queda monotônica
#
# Observação importante:
#   - Idades acima de max_idade (default = 100) são colapsadas em max_idade,
#     tanto na POPSVS quanto no SIM (100+).
###############################################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(readxl)
  library(arrow)
  library(tidyr)
  library(stringr)
  library(janitor)
})

# ----------------- 1. Parâmetros de diretório/arquivos ----------------------

BASE_DIR <- "C:/Users/uriel/Documents/UFPB Estatística/Demografia 1/Estudos Demográficos - PB/TOPALS"

FILE_SIM <- file.path(BASE_DIR, "sim_municipio_idade_simples_2000_2023.parquet")
FILE_POP <- file.path(BASE_DIR, "popsvs_muni_idade_simples_2000_2024.parquet")
FILE_COV <- file.path(BASE_DIR, "cobertura-informacao-obitos.xls")
FILE_DTB <- file.path(BASE_DIR, "RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls")
FILE_REF <- file.path(BASE_DIR, "projecoes_2024_tab5_tabuas_mortalidade.xlsx") # apenas referência

# Saídas organizadas - criando estrutura automática
OUTPUT_DIR <- file.path(BASE_DIR, "00_prep_topals_output")
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

OUT_BASE_RAW    <- file.path(OUTPUT_DIR, "base_municipal_raw_2000_2023.parquet")
OUT_BASE_FINAL  <- file.path(OUTPUT_DIR, "base_municipal_final_2000_2023.parquet")
OUT_PESOS       <- file.path(OUTPUT_DIR, "pesos_redistribuicao_80plus.csv")
OUT_METADATA    <- file.path(OUTPUT_DIR, "metadata_preparacao.txt")

# ----------------- 2. Funções auxiliares gerais -----------------------------

pad_muni7 <- function(x) {
  x <- as.character(x)
  x <- gsub("[^0-9]", "", x)
  stringr::str_pad(x, 7, pad = "0")
}

uf_to_regiao <- function(uf_sigla) {
  u <- stringr::str_trim(as.character(uf_sigla))
  dplyr::case_when(
    u %in% c("RO","AC","AM","RR","PA","AP","TO") ~ "Norte",
    u %in% c("MA","PI","CE","RN","PB","PE","AL","SE","BA") ~ "Nordeste",
    u %in% c("MG","ES","RJ","SP") ~ "Sudeste",
    u %in% c("PR","SC","RS") ~ "Sul",
    u %in% c("MS","MT","GO","DF") ~ "Centro-Oeste",
    TRUE ~ NA_character_
  )
}

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
  ) %>%
    dplyr::mutate(
      regiao_nome = uf_to_regiao(uf_sigla),
      regiao_code = dplyr::case_when(
        regiao_nome == "Norte"        ~ 1L,
        regiao_nome == "Nordeste"     ~ 2L,
        regiao_nome == "Sudeste"      ~ 3L,
        regiao_nome == "Sul"          ~ 4L,
        regiao_nome == "Centro-Oeste" ~ 5L,
        TRUE ~ NA_integer_
      )
    )
}

# ----------------- 3. Leitura e tratamento Cobertura SIM --------------------

read_cov_sim <- function(path) {
  raw <- readxl::read_excel(
    path,
    sheet     = "Cobertura SIM",
    skip      = 2,
    col_names = FALSE
  )
  
  header <- as.character(raw[1, ])
  header[1] <- "local"
  names(raw) <- header
  
  raw <- raw[-1, ]
  raw <- raw %>% dplyr::select(where(~ !all(is.na(.x))))
  
  cov_long <- raw %>%
    tidyr::pivot_longer(
      cols      = -local,
      names_to  = "ano",
      values_to = "cobertura_sim"
    ) %>%
    dplyr::mutate(
      ano = suppressWarnings(as.integer(ano))
    ) %>%
    dplyr::filter(!is.na(ano), !is.na(cobertura_sim))
  
  cov_long
}

# ----------------- 4. Leitura e tratamento DTB Municípios -------------------

read_dtb_muni <- function(path) {
  df <- readxl::read_excel(
    path,
    sheet = "DTB_Municípios",
    skip  = 5
  )
  
  nm <- names(df)
  pick_first <- function(idx) {
    idx <- which(idx)
    if (length(idx) == 0) return(NA_character_)
    nm[idx[1]]
  }
  
  col_uf_code <- pick_first(grepl("^UF$", nm, ignore.case = TRUE))
  col_uf_nome <- pick_first(grepl("Nome.*UF", nm, ignore.case = TRUE))
  
  col_rgint_code <- pick_first(
    grepl("Regi[aã]o Geogr[aá]fica Intermedi[aá]ria", nm, ignore.case = TRUE) &
      !grepl("Nome", nm, ignore.case = TRUE)
  )
  col_rgint_nome <- pick_first(
    grepl("Nome.*Intermedi[aá]ria", nm, ignore.case = TRUE) |
      grepl("Nome.*Regi[aã]o Geogr[aá]fica Intermedi[aá]ria", nm, ignore.case = TRUE)
  )
  
  col_rgi_code <- pick_first(
    grepl("Regi[aã]o Geogr[aá]fica Imediata", nm, ignore.case = TRUE) &
      !grepl("Nome", nm, ignore.case = TRUE)
  )
  col_rgi_nome <- pick_first(
    grepl("Nome.*Imediata", nm, ignore.case = TRUE) |
      grepl("Nome.*Regi[aã]o Geogr[aá]fica Imediata", nm, ignore.case = TRUE)
  )
  
  col_muni_code <- pick_first(
    grepl("C[oó]d.*Munic[ií]pio.*Completo", nm, ignore.case = TRUE) |
      grepl("C[oó]d.*Munic[ií]pio", nm, ignore.case = TRUE)
  )
  col_muni_nome <- pick_first(grepl("Nome.*Munic[ií]pio$", nm, ignore.case = TRUE))
  
  if (any(is.na(c(col_uf_code, col_uf_nome, col_muni_code, col_muni_nome)))) {
    stop("Não consegui localizar algumas colunas-chave na DTB.")
  }
  
  df <- df %>% dplyr::filter(!is.na(.data[[col_muni_code]]))
  ufs <- uf_table()
  
  out <- df %>%
    dplyr::transmute(
      uf_code            = stringr::str_pad(.data[[col_uf_code]], 2, pad = "0"),
      nome_uf            = .data[[col_uf_nome]],
      rgi_intermed_code  = if (!is.na(col_rgint_code)) as.character(.data[[col_rgint_code]]) else NA_character_,
      rgi_intermed_nome  = if (!is.na(col_rgint_nome)) as.character(.data[[col_rgint_nome]]) else NA_character_,
      rgi_imediata_code  = if (!is.na(col_rgi_code))  as.character(.data[[col_rgi_code]])  else NA_character_,
      rgi_imediata_nome  = if (!is.na(col_rgi_nome))  as.character(.data[[col_rgi_nome]])  else NA_character_,
      code_muni7         = pad_muni7(.data[[col_muni_code]]),
      code_muni6         = as.integer(substr(code_muni7, 1, 6)),
      nome_muni          = .data[[col_muni_nome]]
    ) %>%
    dplyr::left_join(ufs, by = "uf_code") %>%
    dplyr::relocate(
      code_muni6, code_muni7, nome_muni,
      uf_code, uf_sigla, nome_uf,
      regiao_code, regiao_nome,
      rgi_intermed_code, rgi_intermed_nome,
      rgi_imediata_code, rgi_imediata_nome
    ) %>%
    dplyr::distinct()
  
  out
}

# ----------------- 5. Construção da base muni-ano-idade ---------------------

build_base_muni_idade <- function(
    file_sim  = FILE_SIM,
    file_pop  = FILE_POP,
    file_cov  = FILE_COV,
    file_dtb  = FILE_DTB,
    ano_min   = 2000L,
    ano_max   = 2023L,
    max_idade = 100L
) {
  
  # --- POPSVS: agrega para sexo == "ambos" ---
  pop <- arrow::open_dataset(file_pop) %>%
    dplyr::filter(ano >= ano_min, ano <= ano_max) %>%
    dplyr::collect() %>%
    dplyr::mutate(
      code_muni7 = pad_muni7(code_muni),
      code_muni6 = as.integer(substr(code_muni7, 1, 6))
    ) %>%
    dplyr::group_by(code_muni6, code_muni7, ano, idade) %>%
    dplyr::summarise(
      pop_ambos = sum(pop, na.rm = TRUE),
      .groups   = "drop"
    )
  
  # --- SIM: óbitos por município-ano-idade (0..100+) ---
  sim <- arrow::open_dataset(file_sim) %>%
    dplyr::filter(ano >= ano_min, ano <= ano_max) %>%
    dplyr::select(code_muni6, ano,
                  idade = idade_simples,
                  obitos) %>%
    dplyr::collect()
  
  # --- DTB: geografia municipal (códigos + nomes) ---
  dtb_muni <- read_dtb_muni(file_dtb)
  
  # --- Cobertura SIM (nível UF x ano) ---
  cov_bruto <- read_cov_sim(file_cov)
  
  dtb_uf <- dtb_muni %>%
    dplyr::distinct(uf_code, uf_sigla, nome_uf,
                    regiao_code, regiao_nome)
  
  cov_uf <- cov_bruto %>%
    dplyr::inner_join(dtb_uf, by = c("local" = "nome_uf")) %>%
    dplyr::select(uf_code, ano, cobertura_sim)
  
  # --- Backbone município-ano (união POP + SIM) -----------------------------
  
  muni_ano <- dplyr::bind_rows(
    pop %>%
      dplyr::distinct(code_muni6, code_muni7, ano),
    sim %>%
      dplyr::distinct(code_muni6, ano) %>%
      dplyr::left_join(
        dtb_muni %>% dplyr::select(code_muni6, code_muni7),
        by = "code_muni6"
      )
  ) %>%
    dplyr::distinct() %>%
    # cola geografia completa + cobertura SIM
    dplyr::left_join(dtb_muni,
                     by = c("code_muni6", "code_muni7")) %>%
    dplyr::left_join(cov_uf,
                     by = c("uf_code", "ano"))
  
  # --- Grid município-ano-idade 0:max_idade ---------------------------------
  
  grid <- muni_ano %>%
    dplyr::distinct(
      code_muni6, code_muni7, nome_muni,
      uf_code, uf_sigla, nome_uf,
      regiao_code, regiao_nome,
      rgi_intermed_code, rgi_intermed_nome,
      rgi_imediata_code, rgi_imediata_nome,
      ano, cobertura_sim
    ) %>%
    tidyr::expand_grid(idade = 0:max_idade)
  
  # --- Cola POP e SIM nesse grid -------------------------------------------
  
  base_full <- grid %>%
    dplyr::left_join(
      pop,
      by = c("code_muni6", "code_muni7", "ano", "idade")
    ) %>%
    dplyr::left_join(
      sim,
      by = c("code_muni6", "ano", "idade")
    ) %>%
    dplyr::arrange(code_muni6, ano, idade) %>%
    dplyr::mutate(
      # NAs em óbitos => 0 (sem registro no SIM)
      obitos = dplyr::coalesce(obitos, 0L)
    )
  
  base_full
}

# ----------------- 6. Redistribuição 80+ guiada por óbitos -------------------
# Ideia:
#   - POPSVS traz apenas 80+ agregado; SIM traz óbitos 80,81,...,100+.
#   - Para cada município-ano:
#       * mantemos o total de população 80+ (N_tot)
#       * usamos os óbitos por idade (D_x) e um padrão Gompertz-alvo m_target(x)
#         para definir novos pesos e redistribuir N_tot entre 80..100
#   - Resultado: as taxas m_x = D_x / N_x ficam proporcionais a m_target(x),
#     ou seja, crescentes com a idade (sem aquele tombo depois dos 70).

# padrão Gompertz alvo para 80–100 (só importa a forma, não o nível)
make_target_mx_80plus <- function(idade_min = 80L,
                                  idade_max = 100L,
                                  g = 0.09) {
  idades <- idade_min:idade_max
  # m_target(x) ∝ exp(g * (x - idade_min)); g ≈ 0.09 ~ Gompertz plausível
  m_target <- exp(g * (idades - idade_min))
  tibble::tibble(idade = idades, m_target = m_target)
}

# ----------------- 7. Redistribuição da população 80+ (80–100) --------------
# OBS: o argumento pesos_80mais é mantido só para compatibilidade com o script
# anterior; aqui ele é ignorado. A redistribuição passa a ser guiada por D_x.

redistribute_80plus <- function(base_muni,
                                pesos_80mais = NULL,  # ignorado
                                idade_min = 80L,
                                idade_max = 100L,
                                g = 0.09) {
  
  target <- make_target_mx_80plus(idade_min = idade_min,
                                  idade_max = idade_max,
                                  g = g)
  
  ages_80 <- target$idade
  m_target <- target$m_target
  
  base_muni %>%
    dplyr::group_by(code_muni6, code_muni7, ano) %>%
    dplyr::group_modify(function(df, key) {
      
      # total 80+ (do POPSVS): pode estar todo em idade == 80 no dado bruto
      idx_80plus <- df$idade >= idade_min & df$idade <= idade_max
      N_tot <- sum(df$pop_ambos[idx_80plus], na.rm = TRUE)
      
      # óbitos 80..100 (SIM) no mesmo município-ano
      D_vec <- df %>%
        dplyr::filter(idade %in% ages_80) %>%
        dplyr::arrange(idade) %>%
        dplyr::pull(obitos)
      
      if (length(D_vec) != length(ages_80)) {
        D_vec <- rep(0L, length(ages_80))
      }
      D_vec[is.na(D_vec)] <- 0L
      
      # Casos degenerados: sem pop 80+ ou sem nenhum óbito 80+
      if (N_tot <= 0 | sum(D_vec) == 0) {
        # Se não tiver nada, simplesmente zera 80..100 e segue a vida
        df$pop_ambos[idx_80plus] <- 0
        return(df)
      }
      
      # Para evitar pesos zero, adiciona um "meio óbito" em cada idade
      D_use <- D_vec + 0.5
      
      # pesos brutos ∝ D_x / m_target(x)
      w_raw <- D_use / m_target
      
      if (!is.finite(sum(w_raw)) || sum(w_raw) <= 0) {
        # fallback extremo (não deveria acontecer): pesos decrescentes simples
        w_raw <- rev(seq_along(ages_80))
      }
      
      w <- as.numeric(w_raw / sum(w_raw))
      
      # novas exposições por idade 80..100, somando exatamente N_tot
      N_vec <- N_tot * w
      
      # grava de volta em df$pop_ambos
      for (k in seq_along(ages_80)) {
        idade_k <- ages_80[k]
        df$pop_ambos[df$idade == idade_k] <- N_vec[k]
      }
      
      df
    }) %>%
    dplyr::ungroup()
}

# ----------------- 8. Execução ----------------------------------------------

base_muni_raw <- build_base_muni_idade(
  file_sim = FILE_SIM,
  file_pop = FILE_POP,
  file_cov = FILE_COV,
  file_dtb = FILE_DTB,
  ano_min  = 2000L,
  ano_max  = 2023L,
  max_idade = 100L
)

#pesos_80mais <- make_pesos_80plus(lambda = 0.15, idade_min = 80L, idade_max = 100L)

base_muni <- redistribute_80plus(
  base_muni = base_muni_raw,
  pesos_80mais = pesos_80mais,  # ignorado
  idade_min = 80L,
  idade_max = 100L,
  g = 0.09   # se quiser curva mais/quase íngreme, mexe aqui
)

# ----------------- 9. Salvar -------------------------------------------------

arrow::write_parquet(base_muni_raw, OUT_BASE_RAW)
arrow::write_parquet(base_muni, OUT_BASE_FINAL)
readr::write_csv(pesos_80mais, OUT_PESOS)

save(base_muni_raw, base_muni, pesos_80mais,
     file = file.path(OUTPUT_DIR, "bases_topals_preparadas.RData"))

metadata_content <- paste(
  "METADADOS - PREPARAÇÃO BASE MUNICIPAL TOPALS",
  "==============================================",
  paste("Data de geração:", Sys.time()),
  "Script: 00_prep_topals.R",
  "",
  "DESCRIÇÃO DOS ARQUIVOS:",
  "- base_municipal_raw_2000_2023.parquet: Base bruta com 80+/100+ concentrado em idade 80/100.",
  "- base_municipal_final_2000_2023.parquet: Base com população 80+ redistribuída (80-100).",
  "- pesos_redistribuicao_80plus.csv: Pesos utilizados para redistribuição 80+.",
  "- bases_topals_preparadas.RData: Objetos em formato R.",
  "",
  "PARÂMETROS DA REDISTRIBUIÇÃO 80+:",
  paste("- Lambda:", 0.15),
  "- Idades redistribuídas: 80-100",
  "",
  "DIMENSÕES DAS BASES:",
  paste("- base_muni_raw:", format(nrow(base_muni_raw), big.mark = "."), "linhas"),
  paste("- base_muni:",     format(nrow(base_muni),     big.mark = "."), "linhas"),
  "- Período: 2000-2023",
  "- Idades: 0-100 anos",
  sep = "\n"
)

writeLines(metadata_content, OUT_METADATA)

cat("\n🎉 PREPARAÇÃO CONCLUÍDA COM SUCESSO!\n")
cat("=====================================\n")
cat("📁 Diretório de saída:", OUTPUT_DIR, "\n\n")
cat("📊 Arquivos gerados:\n")
cat("   •", basename(OUT_BASE_RAW),    "\n")
cat("   •", basename(OUT_BASE_FINAL),  "\n")
cat("   •", basename(OUT_PESOS),       "\n")
cat("   •", basename(OUT_METADATA),    "\n")
cat("   • bases_topals_preparadas.RData\n\n")
cat("📋 Estatísticas:\n")
cat("   -", format(nrow(base_muni_raw), big.mark = "."), "observações (raw)\n")
cat("   -", format(nrow(base_muni),     big.mark = "."), "observações (final)\n")
cat("   -", length(unique(base_muni$code_muni6)), "municípios\n")
cat("   - Período:", min(base_muni$ano), "a", max(base_muni$ano), "\n")
cat("   - Idades: 0 a", max(base_muni$idade), "anos\n\n")
cat("➡️  Próximo passo: Rodar modelagem TOPALS\n")
###############################################################################
