###############################################################################
# 06_analises_avancadas_e0.R
#
# Gera produtos analíticos a partir dos resultados já estimados:
#  - Mapa de ganho absoluto em e0 (ANO_INI_GAIN–ANO_FIM_GAIN)
#  - Mapas com suavização espacial (lag/EB) de e0
#  - Tendência de log(mx) por faixa etária (UF + municípios foco)
#  - Decomposição das mudanças em e0 (Horiuchi) (UF + municípios foco)
#  - Curvas de sobrevivência lx comparativas (UF + municípios foco)
#  - Curvas de log(mx) por idade (UF + municípios foco)
#  - Clusters de mortalidade (LISA) em e0 municipal
#  - Transição de categorias de e0 (baixa/média/alta)
#  - APVP (anos potenciais de vida perdidos) e AVP (déficit de e0)
#  - Desigualdade na longevidade (Gini da morte) – UF
#  - Pirâmides etárias de mortalidade (dx) – UF
#  - Superfícies idade x ano x log(mx) – UF (2D + opcional 3D plotly)
#  - “Quantos anos faltam?” (déficit/superávit de e0 vs UF)
#
# Tudo parametrizado por UF_ALVO e SEXO_ALVO. Nas análises NÃO-mapas,
# comparamos UF com um conjunto de municípios foco (bolsões) definidos
# automaticamente (quantis, LISA) + listas manuais (municípios/RGIs).
###############################################################################

# ------------------------------- Configuração -------------------------------

library(dplyr)
library(tidyr)
library(purrr)
library(readr)
library(ggplot2)
library(sf)
library(geobr)
library(viridis)
library(grid)
library(rlang)

# Pacotes opcionais (blocos vão rodar só se existirem)
has_spdep       <- requireNamespace("spdep",      quietly = TRUE)
has_DemoDecomp  <- requireNamespace("DemoDecomp", quietly = TRUE)
has_plotly      <- requireNamespace("plotly",     quietly = TRUE)

# Caminho base do projeto TOPALS
BASE_DIR <- "C:/Users/uriel/Documents/UFPB Estatística/Demografia 1/Estudos Demográficos - PB/TOPALS"

UF_ALVO   <- "BA"   # ex.: "PB", "SP", "MG"...
SEXO_ALVO <- "b"    # "b" (ambos) ou conforme e0_muni_post/base_muni

# Saídas do script 05B (gerado como 05B_mx_post_e0_<UF>)
OUT_05B_DIR <- file.path(BASE_DIR, paste0("05B_mx_post_e0_", UF_ALVO))
TAB_05B_DIR <- file.path(OUT_05B_DIR, "tabelas")
FIG_05B_DIR <- file.path(OUT_05B_DIR, "figuras")

# Nova pasta para análises avançadas
OUT_DIR <- file.path(BASE_DIR, "06_analises_avancadas", UF_ALVO)
FIG_DIR <- file.path(OUT_DIR, "figuras")
TAB_DIR <- file.path(OUT_DIR, "tabelas")

dir.create(FIG_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(TAB_DIR, showWarnings = FALSE, recursive = TRUE)

# Intervalo de anos de interesse
ANOS_ANALISE      <- 2000:2023
ANO_INI_GAIN      <- 2000L
ANO_FIM_GAIN      <- 2023L
IDADE_LIMITE_APVP <- 75L   # idade limite p/ APVP
# Anos-resumo para curvas (lx, logmx por idade)
ANOS_CURVAS_LOGMX <- unique(c(ANO_INI_GAIN,
                              floor((ANO_INI_GAIN + ANO_FIM_GAIN) / 2),
                              ANO_FIM_GAIN))
ANOS_CURVAS_LOGMX <- ANOS_CURVAS_LOGMX[ANOS_CURVAS_LOGMX %in% ANOS_ANALISE]
if (length(ANOS_CURVAS_LOGMX) == 0) {
  ANOS_CURVAS_LOGMX <- sort(unique(c(ANO_INI_GAIN, ANO_FIM_GAIN)))
}

# ------------------------ Configuração de "foco" ---------------------------

# 1) Lista manual de municípios foco (códigos 6 dígitos: code_muni6)
#    Ex.: MUNIS_FIXOS <- c(250750L, 250400L)  # João Pessoa, Campina Grande
MUNIS_FIXOS <- integer(0)

# 2) Lista manual de RGIs foco (regiões geográficas imediatas), se existirem
#    na base_muni. Ajuste os nomes das colunas conforme sua base.
RGI_FIXOS    <- integer(0)     # ex.: c(2501L, 2502L)
COL_RGI_CODE <- "code_imed"    # coluna com o código da RGI (se existir)
COL_RGI_NAME <- "nome_imed"    # opcional, só p/ uso em rótulos se quiser

# 3) Uso dos clusters LISA para montar municípios foco
USAR_LISA_PARA_FOCO <- TRUE
LISA_CATS_FOCO      <- c("Alto-Alto", "Baixo-Baixo")  # clusters usados
LISA_ANO_REF        <- ANO_FIM_GAIN                   # ano referencial LISA

# ----------------------- Carrega bases já existentes ------------------------

# e0 municipal pós-shrink gerado no 05B
e0_muni_post <- readr::read_csv(
  file.path(TAB_05B_DIR, "e0_municipios_post_shrink.csv"),
  show_col_types = FALSE
)

# Base de mx por idade/município/ano (gerada no pipeline 00)
if (!exists("RDATA_PATH")) {
  OUTPUT_00_DIR <- file.path(BASE_DIR, "00_mx_reconstrucao")
  RDATA_PATH    <- file.path(OUTPUT_00_DIR, "bases_topals_preparadas.RData")
}
load(RDATA_PATH)  # espera carregar 'base_muni' pelo menos

# ---------------------------- Funções auxiliares ---------------------------
# --- Função auxiliar para mx com correção de continuidade ---
ajusta_mx <- function(D, N, half_event = 0.5) {
  mx_bruto <- dplyr::if_else(N > 0, D / N, NA_real_)
  
  # se D == 0 mas tem exposição, usa 0.5/N pra evitar mx = 0 (e log = -Inf)
  mx_corrigido <- dplyr::case_when(
    is.na(mx_bruto)                 ~ NA_real_,
    mx_bruto == 0 & N > 0           ~ half_event / N,
    TRUE                            ~ mx_bruto
  )
  
  mx_corrigido
}

# Tabela de vida a partir de mx (1 ano em 1 ano)
build_lifetable_from_mx <- function(mx_tbl,
                                    idade_col = "idade",
                                    mx_col    = "mx",
                                    radix     = 100000) {
  df <- mx_tbl %>%
    dplyr::arrange(.data[[idade_col]]) %>%
    dplyr::select(!!idade_col, !!mx_col)
  
  idade <- df[[idade_col]]
  mx    <- df[[mx_col]]
  
  n  <- c(diff(idade), 1)        # intervalo ~1 ano
  ax <- ifelse(idade == 0, 0.3, 0.5 * n)
  
  qx <- (n * mx) / (1 + (n - ax) * mx)
  qx[qx > 1] <- 1
  
  nA  <- length(idade)
  lx  <- dx <- Lx <- Tx <- ex <- numeric(nA)
  
  lx[1] <- radix
  dx[1] <- lx[1] * qx[1]
  Lx[1] <- n[1] * (lx[1] - ax[1] * dx[1])
  
  if (nA > 1) {
    for (i in 2:nA) {
      lx[i] <- lx[i - 1] - dx[i - 1]
      dx[i] <- lx[i] * qx[i]
      Lx[i] <- n[i] * (lx[i] - ax[i] * dx[i])
    }
  }
  
  # Grupo aberto: Lx ≈ lx/mx (se mx não for NA)
  if (!is.na(mx[nA]) && mx[nA] > 0) {
    Lx[nA] <- lx[nA] / mx[nA]
  }
  
  for (i in nA:1) {
    if (i == nA) {
      Tx[i] <- Lx[i]
    } else {
      Tx[i] <- Lx[i] + Tx[i + 1]
    }
    ex[i] <- ifelse(lx[i] > 0, Tx[i] / lx[i], NA_real_)
  }
  
  tibble::tibble(
    idade = idade,
    n     = n,
    mx    = mx,
    ax    = ax,
    qx    = qx,
    lx    = lx,
    dx    = dx,
    Lx    = Lx,
    Tx    = Tx,
    ex    = ex
  )
}

# e0 a partir de tabela de vida
calc_e0_from_lt <- function(lt) {
  lt %>%
    dplyr::filter(idade == min(idade, na.rm = TRUE)) %>%
    dplyr::pull(ex) %>%
    as.numeric()
}

# Gini da idade ao óbito (Gini da morte) a partir de (idade, dx)
gini_morte <- function(idade, dx) {
  ok <- is.finite(idade) & is.finite(dx) & dx > 0
  idade <- idade[ok]
  dx    <- dx[ok]
  if (length(idade) <= 1L) return(0)
  
  ord   <- order(idade)
  idade <- idade[ord]
  dx    <- dx[ord]
  
  mu <- sum(dx * idade) / sum(dx)
  if (mu == 0) return(NA_real_)
  
  diff_mat <- abs(outer(idade, idade, "-"))
  w_mat    <- outer(dx, dx)
  G <- sum(w_mat * diff_mat) / (2 * mu * (sum(dx)^2))
  G
}

# mx estadual por idade/ano
uf_mx_por_idade <- function(ano_k, uf_sigla_alvo = UF_ALVO) {
  base_muni %>%
    dplyr::filter(
      uf_sigla == uf_sigla_alvo,
      ano      == ano_k
    ) %>%
    dplyr::group_by(idade) %>%
    dplyr::summarise(
      D = sum(obitos,    na.rm = TRUE),
      N = sum(pop_ambos, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      mx = ajusta_mx(D, N)
    ) %>%
    dplyr::select(idade, mx)
}

# mx municipal por idade/ano
mx_muni_por_idade <- function(code_muni6_sel,
                              ano_sel,
                              uf_sel = UF_ALVO) {
  base_muni %>%
    dplyr::filter(
      uf_sigla   == uf_sel,
      ano        == ano_sel,
      code_muni6 == code_muni6_sel
    ) %>%
    dplyr::group_by(idade) %>%
    dplyr::summarise(
      D = sum(obitos,    na.rm = TRUE),
      N = sum(pop_ambos, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      mx = ajusta_mx(D, N)
    ) %>%
    dplyr::select(idade, mx)
}

# ------------------------ Malha municipal do estado -------------------------

uf_muni_sf <- geobr::read_municipality(code_muni = UF_ALVO, year = 2020) |>
  dplyr::mutate(code_muni6 = as.integer(substr(code_muni, 1, 6)))

names(uf_muni_sf)[names(uf_muni_sf) == "geom"] <- "geometry"
uf_muni_sf <- sf::st_as_sf(uf_muni_sf)
attr(uf_muni_sf, "sf_column") <- "geometry"

uf_muni_df <- uf_muni_sf
class(uf_muni_df) <- setdiff(class(uf_muni_df), "sf")

# ------------------- 1) Tabela de ganho em e0 (municípios) ------------------

e0_gain <- e0_muni_post %>%
  dplyr::filter(
    uf    == UF_ALVO,
    sexo  == SEXO_ALVO,
    nivel == "municipio",
    ano   %in% c(ANO_INI_GAIN, ANO_FIM_GAIN)
  ) %>%
  dplyr::select(ano, code_muni6, nome_muni, e0_p50_post) %>%
  tidyr::pivot_wider(
    names_from   = ano,
    values_from  = e0_p50_post,
    names_prefix = "e0_"
  )

nm_ini <- paste0("e0_", ANO_INI_GAIN)
nm_fim <- paste0("e0_", ANO_FIM_GAIN)

e0_gain <- e0_gain %>%
  dplyr::mutate(
    ganho_abs = .data[[nm_fim]] - .data[[nm_ini]]
  )

# e0 de referência da UF (IBGE)
ref_e0 <- e0_muni_post %>%
  dplyr::filter(
    uf    == UF_ALVO,
    sexo  == SEXO_ALVO,
    nivel == "municipio",
    ano   %in% c(ANO_INI_GAIN, ANO_FIM_GAIN)
  ) %>%
  dplyr::distinct(ano, e0_ibge)

e0_uf_ini <- ref_e0$e0_ibge[ref_e0$ano == ANO_INI_GAIN][1]
e0_uf_fim <- ref_e0$e0_ibge[ref_e0$ano == ANO_FIM_GAIN][1]

e0_gain <- e0_gain %>%
  dplyr::mutate(
    e0_uf_ini = e0_uf_ini,
    e0_uf_fim = e0_uf_fim,
    def_ini   = .data[[nm_ini]] - e0_uf_ini,
    def_fim   = .data[[nm_fim]] - e0_uf_fim
  )

if (nrow(e0_gain) == 0) {
  stop("Nenhum município encontrado em e0_muni_post para UF_ALVO/SEXO_ALVO/nivel=municipio.")
}

# ----------------- 2) Clusters LISA (para seleção + mapa) -------------------

lisa_clusters_df <- tibble::tibble()

if (has_spdep) {
  message("Calculando clusters LISA de e0 pós-shrink para seleção e mapa...")
  library(spdep)
  
  ano_lisa <- LISA_ANO_REF
  
  df_lisa <- e0_muni_post %>%
    dplyr::filter(
      uf    == UF_ALVO,
      sexo  == SEXO_ALVO,
      nivel == "municipio",
      ano   == ano_lisa
    ) %>%
    dplyr::select(code_muni6, e0_p50_post)
  
  map_lisa <- uf_muni_sf %>%
    dplyr::left_join(df_lisa, by = "code_muni6")
  
  map_lisa_nona <- map_lisa[!is.na(map_lisa$e0_p50_post), ]
  
  nb <- spdep::poly2nb(map_lisa_nona)
  lw <- spdep::nb2listw(nb, style = "W", zero.policy = TRUE)
  
  x  <- map_lisa_nona$e0_p50_post
  lm <- spdep::localmoran(
    x,
    lw,
    zero.policy = TRUE,
    na.action   = na.exclude
  )
  
  # Coluna de p-valor pode mudar de nome; procuramos "Pr(...)"
  p_col_name <- grep("^Pr\\(", colnames(lm), value = TRUE)[1]
  if (is.na(p_col_name)) {
    stop("Não encontrei coluna de p-valor no output de localmoran().")
  }
  
  map_lisa_nona$lisa_I <- lm[, "Ii"]
  map_lisa_nona$lisa_p <- lm[, p_col_name]
  
  mean_x <- mean(x, na.rm = TRUE)
  Z      <- x - mean_x
  
  map_lisa_nona$quad <- NA_character_
  map_lisa_nona$quad[Z > 0 & lm[, "Ii"] > 0] <- "Alto-Alto"
  map_lisa_nona$quad[Z < 0 & lm[, "Ii"] > 0] <- "Baixo-Baixo"
  map_lisa_nona$quad[Z > 0 & lm[, "Ii"] < 0] <- "Alto-Baixo"
  map_lisa_nona$quad[Z < 0 & lm[, "Ii"] < 0] <- "Baixo-Alto"
  
  map_lisa_nona$quad[
    map_lisa_nona$lisa_p > 0.05 | is.na(map_lisa_nona$lisa_p)
  ] <- "Não significativo"
  
  map_lisa_nona$quad <- factor(
    map_lisa_nona$quad,
    levels = c("Alto-Alto", "Baixo-Baixo", "Alto-Baixo",
               "Baixo-Alto", "Não significativo")
  )
  
  lisa_clusters_df <- map_lisa_nona %>%
    sf::st_drop_geometry() %>%
    dplyr::select(code_muni6, lisa_I, lisa_p, quad)
} else {
  message("Pacote 'spdep' não disponível — LISA não será calculado.")
}

# ------------------------ 3) Define municípios foco -------------------------

q_ini  <- stats::quantile(e0_gain[[nm_ini]],  probs = c(0.10, 0.90), na.rm = TRUE)
q_fim  <- stats::quantile(e0_gain[[nm_fim]],  probs = c(0.10, 0.90), na.rm = TRUE)
q_gain <- stats::quantile(e0_gain$ganho_abs,  probs = c(0.10, 0.90), na.rm = TRUE)

n_top_por_tipo <- 3L

pega_top_por_tipo <- function(df, tipo, var, n, desc = TRUE) {
  if (!nrow(df)) return(dplyr::slice_head(df, n = 0))
  if (desc) {
    df %>%
      dplyr::arrange(dplyr::desc(.data[[var]])) %>%
      dplyr::mutate(tipo_raw = tipo) %>%
      dplyr::slice_head(n = min(n, nrow(df)))
  } else {
    df %>%
      dplyr::arrange(.data[[var]]) %>%
      dplyr::mutate(tipo_raw = tipo) %>%
      dplyr::slice_head(n = min(n, nrow(df)))
  }
}

df_baixa_ini   <- e0_gain %>% dplyr::filter(.data[[nm_ini]] <= q_ini[1])
df_alta_ini    <- e0_gain %>% dplyr::filter(.data[[nm_ini]] >= q_ini[2])
df_baixa_fim   <- e0_gain %>% dplyr::filter(.data[[nm_fim]] <= q_fim[1])
df_alta_fim    <- e0_gain %>% dplyr::filter(.data[[nm_fim]] >= q_fim[2])
df_baixo_ganho <- e0_gain %>% dplyr::filter(ganho_abs <= q_gain[1])
df_alto_ganho  <- e0_gain %>% dplyr::filter(ganho_abs >= q_gain[2])

munis_foco_raw <- dplyr::bind_rows(
  pega_top_por_tipo(df_baixa_ini,   "e0 baixa no início",  nm_ini,      n_top_por_tipo, desc = FALSE),
  pega_top_por_tipo(df_alta_ini,    "e0 alta no início",   nm_ini,      n_top_por_tipo, desc = TRUE),
  pega_top_por_tipo(df_baixa_fim,   "e0 baixa no final",   nm_fim,      n_top_por_tipo, desc = FALSE),
  pega_top_por_tipo(df_alta_fim,    "e0 alta no final",    nm_fim,      n_top_por_tipo, desc = TRUE),
  pega_top_por_tipo(df_baixo_ganho, "baixo ganho em e0",   "ganho_abs", n_top_por_tipo, desc = FALSE),
  pega_top_por_tipo(df_alto_ganho,  "alto ganho em e0",    "ganho_abs", n_top_por_tipo, desc = TRUE)
)

# 3a) Acrescenta municípios fixos (lista manual)
if (length(MUNIS_FIXOS) > 0) {
  munis_fixos_extra <- e0_gain %>%
    dplyr::filter(code_muni6 %in% MUNIS_FIXOS) %>%
    dplyr::mutate(tipo_raw = "selecionado_manual")
  munis_foco_raw <- dplyr::bind_rows(munis_foco_raw, munis_fixos_extra)
}

# 3b) Acrescenta municípios vindos de RGIs fixos (se colunas existirem)
if (length(RGI_FIXOS) > 0 && COL_RGI_CODE %in% names(base_muni)) {
  munis_from_rgi_codes <- base_muni %>%
    dplyr::filter(
      uf_sigla == UF_ALVO,
      ano %in% c(ANO_INI_GAIN, ANO_FIM_GAIN),
      !!rlang::sym(COL_RGI_CODE) %in% RGI_FIXOS
    ) %>%
    dplyr::distinct(code_muni6)
  
  munis_from_rgi <- e0_gain %>%
    dplyr::filter(code_muni6 %in% munis_from_rgi_codes$code_muni6) %>%
    dplyr::mutate(tipo_raw = "selecionado_RGI")
  
  if (nrow(munis_from_rgi) > 0) {
    munis_foco_raw <- dplyr::bind_rows(munis_foco_raw, munis_from_rgi)
  }
}

# 3c) Acrescenta municípios selecionados por LISA (clusters de interesse)
if (USAR_LISA_PARA_FOCO && nrow(lisa_clusters_df) > 0) {
  munis_lisa_extra <- e0_gain %>%
    dplyr::left_join(lisa_clusters_df, by = "code_muni6") %>%
    dplyr::filter(quad %in% LISA_CATS_FOCO) %>%
    dplyr::mutate(tipo_raw = paste0("cluster LISA: ", quad))
  
  if (nrow(munis_lisa_extra) > 0) {
    munis_foco_raw <- dplyr::bind_rows(munis_foco_raw, munis_lisa_extra)
  }
}

# Consolida por município (podendo ter mais de um rótulo)
munis_foco <- munis_foco_raw %>%
  dplyr::select(code_muni6, nome_muni, tipo_raw) %>%
  dplyr::distinct() %>%
  dplyr::group_by(code_muni6, nome_muni) %>%
  dplyr::summarise(
    tipo = paste(unique(tipo_raw), collapse = " / "),
    .groups = "drop"
  ) %>%
  dplyr::arrange(code_muni6)

if (nrow(munis_foco) == 0) {
  warning("munis_foco ficou vazio – as análises municipais serão puladas.")
}

readr::write_csv(
  munis_foco,
  file.path(TAB_DIR, paste0("munis_foco_", UF_ALVO, "_", SEXO_ALVO, ".csv"))
)

# ----------------- 4) Mapa ganho absoluto em e0 (UF_ALVO) -------------------

e0_gain_map <- e0_gain %>%
  dplyr::select(code_muni6, nome_muni,
                dplyr::all_of(c(nm_ini, nm_fim, "ganho_abs"))) %>%
  dplyr::select(-dplyr::any_of(c("geom", "geometry")))

map_gain <- uf_muni_df %>%
  dplyr::left_join(e0_gain_map, by = "code_muni6") %>%
  sf::st_as_sf(sf_column_name = "geometry")

p_gain <- ggplot2::ggplot(map_gain) +
  ggplot2::geom_sf(ggplot2::aes(fill = ganho_abs), color = NA) +
  viridis::scale_fill_viridis(
    option    = "magma",
    direction = -1,
    name      = paste0("Ganho em e0 (", ANO_INI_GAIN, "–", ANO_FIM_GAIN, ")"),
    alpha     = 0.8
  ) +
  ggplot2::labs(
    title    = paste0("Ganho absoluto na esperança de vida ao nascer — ", UF_ALVO),
    subtitle = paste0(ANO_INI_GAIN, "–", ANO_FIM_GAIN),
    x        = NULL,
    y        = NULL
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position   = "bottom",
    legend.background = ggplot2::element_rect(fill = "white", colour = NA),
    panel.grid        = ggplot2::element_blank(),
    axis.text         = ggplot2::element_blank(),
    axis.ticks        = ggplot2::element_blank()
  )

ggplot2::ggsave(
  file.path(FIG_DIR, paste0("mapa_ganho_abs_e0_", UF_ALVO, "_", ANO_INI_GAIN, "_", ANO_FIM_GAIN, ".png")),
  p_gain, width = 6, height = 6, dpi = 300
)

# ---------------- 5) Mapas de suavização espacial (lag / EB) ----------------

if (has_spdep) {
  message("Gerando mapas com suavização espacial (spdep)...")
  library(spdep)
  
  nb <- spdep::poly2nb(uf_muni_sf)
  lw <- spdep::nb2listw(nb, style = "W", zero.policy = TRUE)
  
  ano_smooth <- ANO_FIM_GAIN
  
  df_smooth <- e0_muni_post %>%
    dplyr::filter(
      uf    == UF_ALVO,
      sexo  == SEXO_ALVO,
      nivel == "municipio",
      ano   == ano_smooth
    ) %>%
    dplyr::select(code_muni6, e0_p50_post)
  
  map_smooth <- uf_muni_sf %>%
    dplyr::left_join(df_smooth, by = "code_muni6")
  
  e0_vec    <- map_smooth$e0_p50_post
  e0_smooth <- spdep::lag.listw(lw, e0_vec, zero.policy = TRUE)
  map_smooth$e0_smooth <- as.numeric(e0_smooth)
  
  p_smooth <- ggplot2::ggplot(map_smooth) +
    ggplot2::geom_sf(ggplot2::aes(fill = e0_smooth), color = NA) +
    viridis::scale_fill_viridis(
      option    = "magma",
      direction = -1,
      name      = paste0("e0 suavizado (", ano_smooth, ")"),
      alpha     = 0.8
    ) +
    ggplot2::labs(
      title    = paste0("e0 pós-shrink com suavização espacial — ", UF_ALVO),
      subtitle = paste0("Ano: ", ano_smooth, " • Suavização via lag espacial (spdep)"),
      x        = NULL,
      y        = NULL
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
      legend.position   = "bottom",
      legend.background = ggplot2::element_rect(fill = "white", colour = NA),
      panel.grid        = ggplot2::element_blank(),
      axis.text         = ggplot2::element_blank(),
      axis.ticks        = ggplot2::element_blank()
    )
  
  ggplot2::ggsave(
    file.path(FIG_DIR, paste0("mapa_e0_suavizado_", UF_ALVO, "_", ano_smooth, ".png")),
    p_smooth, width = 6, height = 6, dpi = 300
  )
} else {
  message("Pacote 'spdep' não disponível — pulando suavização espacial.")
}

# --------------- 6) Tendência de log(mx) por faixa (UF + foco) --------------

# 6a) UF
trend_mx_uf <- purrr::map_dfr(
  ANOS_ANALISE,
  ~ {
    ano_k <- .x
    mx_k  <- uf_mx_por_idade(ano_k)
    mx_k$ano <- ano_k
    mx_k
  }
) %>%
  dplyr::mutate(
    faixa = dplyr::case_when(
      idade <= 4                  ~ "0–4",
      idade >= 5  & idade <= 14   ~ "5–14",
      idade >= 15 & idade <= 39   ~ "15–39",
      idade >= 40 & idade <= 59   ~ "40–59",
      idade >= 60                 ~ "60+",
      TRUE                        ~ NA_character_
    )
  ) %>%
  dplyr::filter(!is.na(faixa))

trend_faixa_uf <- trend_mx_uf %>%
  dplyr::group_by(ano, faixa) %>%
  dplyr::summarise(
    mx    = mean(mx, na.rm = TRUE),
    logmx = dplyr::if_else(mx > 0, log(mx), NA_real_),
    .groups = "drop"
  )

p_trend_uf <- ggplot2::ggplot(trend_faixa_uf,
                              ggplot2::aes(x = ano, y = logmx, color = faixa)) +
  ggplot2::geom_line() +
  ggplot2::geom_point(size = 1) +
  ggplot2::labs(
    title    = paste0("Tendência de log(mx) por faixa etária — UF ", UF_ALVO),
    subtitle = paste0("Sexo: ", SEXO_ALVO, " (mx estadual agregado por faixa etária)"),
    x        = "Ano",
    y        = "log(mx)",
    color    = "Faixa etária"
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position   = "bottom",
    legend.background = ggplot2::element_rect(fill = "white", colour = NA)
  )

ggplot2::ggsave(
  file.path(FIG_DIR, paste0("tendencia_logmx_por_faixa_UF_", UF_ALVO, "_", SEXO_ALVO, ".png")),
  p_trend_uf, width = 8, height = 4.5, dpi = 300
)

# 6b) UF + municípios foco
if (nrow(munis_foco) > 0) {
  for (i in seq_len(nrow(munis_foco))) {
    code_i <- munis_foco$code_muni6[i]
    nome_i <- munis_foco$nome_muni[i]
    tipo_i <- munis_foco$tipo[i]
    
    message("Tendência log(mx) por faixa — município foco: ", nome_i,
            " (", code_i, ")")
    
    trend_mx_muni <- purrr::map_dfr(
      ANOS_ANALISE,
      ~ {
        ano_k <- .x
        mx_k  <- mx_muni_por_idade(code_muni6_sel = code_i,
                                   ano_sel        = ano_k)
        if (nrow(mx_k) == 0) return(NULL)
        mx_k$ano <- ano_k
        mx_k
      }
    )
    
    if (nrow(trend_mx_muni) == 0) next
    
    trend_mx_muni <- trend_mx_muni %>%
      dplyr::mutate(
        faixa = dplyr::case_when(
          idade <= 4                  ~ "0–4",
          idade >= 5  & idade <= 14   ~ "5–14",
          idade >= 15 & idade <= 39   ~ "15–39",
          idade >= 40 & idade <= 59   ~ "40–59",
          idade >= 60                 ~ "60+",
          TRUE                        ~ NA_character_
        )
      ) %>%
      dplyr::filter(!is.na(faixa))
    
    trend_faixa_muni <- trend_mx_muni %>%
      dplyr::group_by(ano, faixa) %>%
      dplyr::summarise(
        mx    = mean(mx, na.rm = TRUE),
        logmx = dplyr::if_else(mx > 0, log(mx), NA_real_),
        .groups = "drop"
      )
    
    trend_merged <- dplyr::bind_rows(
      trend_faixa_uf %>%
        dplyr::mutate(local = paste0("UF ", UF_ALVO)),
      trend_faixa_muni %>%
        dplyr::mutate(local = nome_i)
    )
    
    p_trend_muni <- ggplot2::ggplot(
      trend_merged,
      ggplot2::aes(x = ano, y = logmx, color = local, linetype = local)
    ) +
      ggplot2::geom_line() +
      ggplot2::geom_point(size = 1) +
      ggplot2::facet_wrap(~ faixa) +
      ggplot2::labs(
        title    = paste0("Tendência de log(mx) por faixa — ", nome_i),
        subtitle = paste0(
          "Comparação UF ", UF_ALVO, " vs município • Sexo: ", SEXO_ALVO,
          " • Tipo de município: ", tipo_i
        ),
        x        = "Ano",
        y        = "log(mx)",
        color    = "Local",
        linetype = "Local"
      ) +
      ggplot2::theme_minimal() +
      ggplot2::theme(
        plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
        legend.position   = "bottom",
        legend.background = ggplot2::element_rect(fill = "white", colour = NA)
      )
    
    ggplot2::ggsave(
      file.path(
        FIG_DIR,
        paste0(
          "tendencia_logmx_por_faixa_muni_",
          UF_ALVO, "_", SEXO_ALVO, "_", code_i, ".png"
        )
      ),
      p_trend_muni, width = 9, height = 5, dpi = 300
    )
  }
}

# ------------- 7) Decomposição ∆e0 por idade (Horiuchi / DemoDecomp) --------

if (has_DemoDecomp) {
  message("Rodando decomposição de e0 com DemoDecomp (Horiuchi)...")
  library(DemoDecomp)
  
  anos_decomp <- c(ANO_INI_GAIN, ANO_FIM_GAIN)
  
  # 7a) Decomposição para a UF
  mx_1_uf  <- uf_mx_por_idade(anos_decomp[1])$mx
  mx_2_uf  <- uf_mx_por_idade(anos_decomp[2])$mx
  idade_uf <- uf_mx_por_idade(anos_decomp[1])$idade
  
  f_e0_uf <- function(mx_vec) {
    lt <- build_lifetable_from_mx(
      mx_tbl    = tibble::tibble(idade = idade_uf, mx = mx_vec),
      idade_col = "idade",
      mx_col    = "mx"
    )
    calc_e0_from_lt(lt)
  }
  
  contrib_uf <- DemoDecomp::horiuchi(
    func  = f_e0_uf,
    pars1 = mx_1_uf,
    pars2 = mx_2_uf,
    N     = 50
  )
  
  decomp_uf <- tibble::tibble(
    idade   = idade_uf,
    contrib = contrib_uf
  )
  
  readr::write_csv(
    decomp_uf,
    file.path(TAB_DIR, paste0("decomp_e0_horiuchi_UF_", UF_ALVO, "_", anos_decomp[1], "_", anos_decomp[2], ".csv"))
  )
  
  p_decomp_uf <- ggplot2::ggplot(decomp_uf,
                                 ggplot2::aes(x = idade, y = contrib)) +
    ggplot2::geom_col() +
    ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
    ggplot2::labs(
      title    = paste0("Decomposição da variação de e0 por idade — UF ", UF_ALVO),
      subtitle = paste0(anos_decomp[1], "→", anos_decomp[2], " (Horiuchi)"),
      x        = "Idade",
      y        = "Contribuição para ∆e0 (anos)"
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
      legend.position   = "bottom"
    )
  
  ggplot2::ggsave(
    file.path(FIG_DIR, paste0("decomp_e0_horiuchi_UF_", UF_ALVO, "_", anos_decomp[1], "_", anos_decomp[2], ".png")),
    p_decomp_uf, width = 8, height = 4.5, dpi = 300
  )
  
  # 7b) Decomposição para cada município foco
  if (nrow(munis_foco) > 0) {
    for (i in seq_len(nrow(munis_foco))) {
      code_i <- munis_foco$code_muni6[i]
      nome_i <- munis_foco$nome_muni[i]
      tipo_i <- munis_foco$tipo[i]
      
      message("Decomposição ∆e0 (Horiuchi) — município foco: ", nome_i,
              " (", code_i, ")")
      
      mx_ini_m <- mx_muni_por_idade(code_muni6_sel = code_i,
                                    ano_sel        = ANO_INI_GAIN)
      mx_fim_m <- mx_muni_por_idade(code_muni6_sel = code_i,
                                    ano_sel        = ANO_FIM_GAIN)
      
      if (nrow(mx_ini_m) == 0 || nrow(mx_fim_m) == 0) next
      
      idade_m <- mx_ini_m$idade
      mx_1_m  <- mx_ini_m$mx
      mx_2_m  <- mx_fim_m$mx
      
      # garante mesmo conjunto de idades e tira grupo aberto
      idx_m   <- idade_m < max(idade_m, na.rm = TRUE)
      idade_m <- idade_m[idx_m]
      mx_1_m  <- mx_1_m[idx_m]
      mx_2_m  <- mx_2_m[idx_m]
      
      f_e0_m <- function(mx_vec) {
        lt <- build_lifetable_from_mx(
          mx_tbl    = tibble::tibble(idade = idade_m, mx = mx_vec),
          idade_col = "idade",
          mx_col    = "mx"
        )
        calc_e0_from_lt(lt)
      }
      
      contrib_m <- DemoDecomp::horiuchi(
        func  = f_e0_m,
        pars1 = mx_1_m,
        pars2 = mx_2_m,
        N     = 50
      )
      
      decomp_m <- tibble::tibble(
        idade   = idade_m,
        contrib = contrib_m
      )
      
      e0_ini_m <- f_e0_m(mx_1_m)
      e0_fim_m <- f_e0_m(mx_2_m)
      
      readr::write_csv(
        decomp_m,
        file.path(
          TAB_DIR,
          paste0(
            "decomp_e0_horiuchi_muni_",
            UF_ALVO, "_", SEXO_ALVO, "_", code_i, "_",
            ANO_INI_GAIN, "_", ANO_FIM_GAIN, ".csv"
          )
        )
      )
      
      p_decomp_m <- ggplot2::ggplot(
        decomp_m,
        ggplot2::aes(x = idade, y = contrib)
      ) +
        ggplot2::geom_col() +
        ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
        ggplot2::labs(
          title    = paste0("Decomposição da variação de e0 por idade — ", nome_i),
          subtitle = paste0(
            ANO_INI_GAIN, "→", ANO_FIM_GAIN,
            " • e0: ", round(e0_ini_m, 1), " → ", round(e0_fim_m, 1),
            " (∆ = ", round(e0_fim_m - e0_ini_m, 1), " anos)\n",
            "Tipo de município: ", tipo_i
          ),
          x        = "Idade",
          y        = "Contribuição para ∆e0 (anos)"
        ) +
        ggplot2::theme_minimal() +
        ggplot2::theme(
          plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
          legend.position   = "bottom"
        )
      
      ggplot2::ggsave(
        file.path(
          FIG_DIR,
          paste0(
            "decomp_e0_horiuchi_muni_",
            UF_ALVO, "_", SEXO_ALVO, "_", code_i, "_",
            ANO_INI_GAIN, "_", ANO_FIM_GAIN, ".png"
          )
        ),
        p_decomp_m, width = 8, height = 4.5, dpi = 300
      )
    }
  }
} else {
  message("Pacote 'DemoDecomp' não disponível — pulando decomposição de e0.")
}

# ----------------- 8) Curvas de sobrevivência lx comparativas ---------------

# 8a) UF (anos selecionados)
anos_lx <- ANOS_CURVAS_LOGMX

lt_list_uf <- purrr::map(
  anos_lx,
  ~ {
    ano_k <- .x
    mx_k  <- uf_mx_por_idade(ano_k)
    lt_k  <- build_lifetable_from_mx(mx_k, idade_col = "idade", mx_col = "mx")
    lt_k$ano <- ano_k
    lt_k
  }
)

lt_all_uf <- dplyr::bind_rows(lt_list_uf) %>%
  dplyr::group_by(ano) %>%
  dplyr::mutate(Sx = lx / dplyr::first(lx[idade == min(idade)])) %>%
  dplyr::ungroup()

p_lx_uf <- ggplot2::ggplot(lt_all_uf,
                           ggplot2::aes(x = idade, y = Sx, color = factor(ano))) +
  ggplot2::geom_line() +
  ggplot2::labs(
    title    = paste0("Curvas de sobrevivência lx/l0 — UF ", UF_ALVO),
    subtitle = paste0("Comparação entre anos selecionados (", paste(anos_lx, collapse = ", "), ")"),
    x        = "Idade",
    y        = "lx / l0",
    color    = "Ano"
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position   = "bottom",
    legend.background = ggplot2::element_rect(fill = "white", colour = NA)
  )

ggplot2::ggsave(
  file.path(FIG_DIR, paste0("curvas_lx_l0_UF_", UF_ALVO, ".png")),
  p_lx_uf, width = 8, height = 4.5, dpi = 300
)

# 8b) Curvas lx/l0 UF vs municípios foco (anos inicial e final)
if (nrow(munis_foco) > 0) {
  anos_lx_comp <- c(ANO_INI_GAIN, ANO_FIM_GAIN)
  
  for (i in seq_len(nrow(munis_foco))) {
    code_i <- munis_foco$code_muni6[i]
    nome_i <- munis_foco$nome_muni[i]
    tipo_i <- munis_foco$tipo[i]
    
    message("Curvas lx/l0 — município foco: ", nome_i,
            " (", code_i, ")")
    
    lx_plot_df <- purrr::map_dfr(
      anos_lx_comp,
      function(ano_k) {
        # UF
        mx_uf_k <- uf_mx_por_idade(ano_k)
        if (nrow(mx_uf_k) == 0) return(NULL)
        
        lt_uf <- build_lifetable_from_mx(
          mx_tbl    = mx_uf_k,
          idade_col = "idade",
          mx_col    = "mx"
        ) %>%
          dplyr::select(idade, lx) %>%
          dplyr::mutate(
            ano   = ano_k,
            local = paste0("UF ", UF_ALVO)
          )
        
        # Município
        mx_muni_k <- mx_muni_por_idade(code_muni6_sel = code_i,
                                       ano_sel        = ano_k)
        if (nrow(mx_muni_k) == 0) return(lt_uf)
        
        lt_muni <- build_lifetable_from_mx(
          mx_tbl    = mx_muni_k,
          idade_col = "idade",
          mx_col    = "mx"
        ) %>%
          dplyr::select(idade, lx) %>%
          dplyr::mutate(
            ano   = ano_k,
            local = paste0(nome_i, " (município)")
          )
        
        dplyr::bind_rows(lt_uf, lt_muni)
      }
    )
    
    if (nrow(lx_plot_df) == 0) next
    
    lx_plot_df <- lx_plot_df %>%
      dplyr::group_by(ano, local) %>%
      dplyr::mutate(lx_norm = lx / dplyr::first(lx)) %>%
      dplyr::ungroup()
    
    p_lx_muni <- ggplot2::ggplot(
      lx_plot_df,
      ggplot2::aes(x = idade, y = lx_norm, color = local)
    ) +
      ggplot2::geom_line(size = 0.9) +
      ggplot2::facet_wrap(~ ano, nrow = 1) +
      ggplot2::labs(
        title    = paste0("Curvas de sobrevivência (lx/l0) — ", nome_i),
        subtitle = paste0(
          "Comparação com UF ", UF_ALVO,
          " • Sexo: ", SEXO_ALVO,
          " • Tipo de município: ", tipo_i
        ),
        x        = "Idade",
        y        = "lx / l0",
        color    = "Local"
      ) +
      ggplot2::theme_minimal() +
      ggplot2::theme(
        plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
        legend.position   = "bottom",
        legend.background = ggplot2::element_rect(fill = "white", colour = NA)
      )
    
    ggplot2::ggsave(
      file.path(
        FIG_DIR,
        paste0(
          "curvas_lx_l0_muni_vs_uf_",
          UF_ALVO, "_", SEXO_ALVO, "_", code_i, "_",
          ANO_INI_GAIN, "_", ANO_FIM_GAIN, ".png"
        )
      ),
      p_lx_muni, width = 8, height = 4.5, dpi = 300
    )
  }
}

# -------------- 9) APVP (anos potenciais perdidos) + AVP --------------------

# 9a) APVP (Anos Potenciais de Vida Perdidos) até IDADE_LIMITE_APVP
apvp_df <- base_muni %>%
  dplyr::filter(
    uf_sigla == UF_ALVO,
    ano %in% ANOS_ANALISE,
    idade < IDADE_LIMITE_APVP
  ) %>%
  dplyr::group_by(ano, code_muni6) %>%
  dplyr::summarise(
    apvp_total    = sum((IDADE_LIMITE_APVP - idade) * obitos, na.rm = TRUE),
    pop_total     = sum(pop_ambos, na.rm = TRUE),
    apvp_por_100k = dplyr::if_else(pop_total > 0, apvp_total / pop_total * 1e5, NA_real_),
    .groups       = "drop"
  ) %>%
  dplyr::left_join(
    e0_muni_post %>%
      dplyr::filter(
        uf    == UF_ALVO,
        sexo  == SEXO_ALVO,
        nivel == "municipio"
      ) %>%
      dplyr::distinct(ano, code_muni6, nome_muni),
    by = c("ano", "code_muni6")
  )

readr::write_csv(
  apvp_df,
  file.path(TAB_DIR, paste0("apvp_municipios_", UF_ALVO, "_", SEXO_ALVO, ".csv"))
)

apvp_2023 <- apvp_df %>%
  dplyr::filter(ano == ANO_FIM_GAIN) %>%
  dplyr::select(code_muni6, apvp_por_100k)

map_apvp <- uf_muni_sf %>%
  dplyr::left_join(apvp_2023, by = "code_muni6")

p_apvp <- ggplot2::ggplot(map_apvp) +
  ggplot2::geom_sf(ggplot2::aes(fill = apvp_por_100k), color = NA) +
  viridis::scale_fill_viridis(
    option    = "magma",
    direction = -1,
    name      = paste0("APVP por 100 mil\n(<", IDADE_LIMITE_APVP, " anos)"),
    alpha     = 0.8
  ) +
  ggplot2::labs(
    title    = paste0("Anos Potenciais de Vida Perdidos (APVP) — ", UF_ALVO),
    subtitle = paste0("Ano: ", ANO_FIM_GAIN, " • Sexo: ", SEXO_ALVO),
    x        = NULL,
    y        = NULL
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position   = "bottom",
    legend.background = ggplot2::element_rect(fill = "white", colour = NA),
    panel.grid        = ggplot2::element_blank(),
    axis.text         = ggplot2::element_blank(),
    axis.ticks        = ggplot2::element_blank()
  )

ggplot2::ggsave(
  file.path(FIG_DIR, paste0("mapa_apvp_", UF_ALVO, "_", SEXO_ALVO, "_", ANO_FIM_GAIN, ".png")),
  p_apvp, width = 6, height = 6, dpi = 300
)

# 9b) AVP (déficit de e0 municipal vs e0 da UF)
avp_df <- e0_muni_post %>%
  dplyr::filter(
    uf    == UF_ALVO,
    sexo  == SEXO_ALVO,
    nivel == "municipio",
    ano %in% ANOS_ANALISE
  ) %>%
  dplyr::mutate(
    avp_uf = pmax(0, e0_ibge - e0_p50_post)
  )

readr::write_csv(
  avp_df,
  file.path(TAB_DIR, paste0("avp_municipios_vs_uf_", UF_ALVO, "_", SEXO_ALVO, ".csv"))
)

avp_2023 <- avp_df %>%
  dplyr::filter(ano == ANO_FIM_GAIN) %>%
  dplyr::select(code_muni6, avp_uf)

map_avp <- uf_muni_sf %>%
  dplyr::left_join(avp_2023, by = "code_muni6")

p_avp <- ggplot2::ggplot(map_avp) +
  ggplot2::geom_sf(ggplot2::aes(fill = avp_uf), color = NA) +
  viridis::scale_fill_viridis(
    option    = "magma",
    direction = -1,
    name      = "AVP (anos)\nref. e0_UF",
    alpha     = 0.8
  ) +
  ggplot2::labs(
    title    = paste0("Anos de vida perdidos vs referência estadual — ", UF_ALVO),
    subtitle = paste0("Ano: ", ANO_FIM_GAIN, " • Sexo: ", SEXO_ALVO),
    x        = NULL,
    y        = NULL
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position   = "bottom",
    legend.background = ggplot2::element_rect(fill = "white", colour = NA),
    panel.grid        = ggplot2::element_blank(),
    axis.text         = ggplot2::element_blank(),
    axis.ticks        = ggplot2::element_blank()
  )

ggplot2::ggsave(
  file.path(FIG_DIR, paste0("mapa_avp_vs_uf_", UF_ALVO, "_", SEXO_ALVO, "_", ANO_FIM_GAIN, ".png")),
  p_avp, width = 6, height = 6, dpi = 300
)

# ----------- 10) Desigualdade na longevidade (Gini da morte, UF) -----------

gini_uf <- purrr::map_dfr(
  ANOS_ANALISE,
  ~ {
    ano_k <- .x
    mx_k  <- uf_mx_por_idade(ano_k)
    lt_k  <- build_lifetable_from_mx(mx_k, idade_col = "idade", mx_col = "mx")
    tibble::tibble(
      ano        = ano_k,
      e0         = calc_e0_from_lt(lt_k),
      gini_morte = gini_morte(idade = lt_k$idade, dx = lt_k$dx)
    )
  }
)

readr::write_csv(
  gini_uf,
  file.path(TAB_DIR, paste0("gini_morte_e0_", UF_ALVO, ".csv"))
)

p_gini <- ggplot2::ggplot(gini_uf, ggplot2::aes(x = ano)) +
  ggplot2::geom_line(ggplot2::aes(y = e0, color = "e0")) +
  ggplot2::geom_line(ggplot2::aes(y = gini_morte * 100, color = "Gini x 100")) +
  ggplot2::labs(
    title    = paste0("Esperança de vida e desigualdade na longevidade — ", UF_ALVO),
    subtitle = "Gini da idade ao óbito multiplicado por 100",
    x        = "Ano",
    y        = "Valor",
    color    = NULL
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position   = "bottom",
    legend.background = ggplot2::element_rect(fill = "white", colour = NA)
  )

ggplot2::ggsave(
  file.path(FIG_DIR, paste0("serie_e0_gini_morte_", UF_ALVO, ".png")),
  p_gini, width = 8, height = 4.5, dpi = 300
)

# ---------------- 11) Pirâmides etárias de mortalidade (dx) — UF ------------

lt_2000 <- build_lifetable_from_mx(uf_mx_por_idade(ANO_INI_GAIN),
                                   idade_col = "idade", mx_col = "mx")
lt_2023 <- build_lifetable_from_mx(uf_mx_por_idade(ANO_FIM_GAIN),
                                   idade_col = "idade", mx_col = "mx")

piramide_df <- dplyr::full_join(
  lt_2000 %>% dplyr::select(idade, dx_2000 = dx),
  lt_2023 %>% dplyr::select(idade, dx_2023 = dx),
  by = "idade"
) %>%
  dplyr::mutate(
    dx_2000 = -dx_2000,
    ano     = NA_integer_
  ) %>%
  tidyr::pivot_longer(
    cols      = c(dx_2000, dx_2023),
    names_to  = "serie",
    values_to = "dx"
  ) %>%
  dplyr::mutate(
    ano = dplyr::case_when(
      serie == "dx_2000" ~ ANO_INI_GAIN,
      serie == "dx_2023" ~ ANO_FIM_GAIN
    )
  )

p_piramide <- ggplot2::ggplot(piramide_df,
                              ggplot2::aes(x = dx, y = idade, fill = factor(ano))) +
  ggplot2::geom_col(width = 1, position = "identity") +
  ggplot2::labs(
    title    = paste0("Pirâmide de mortalidade (dx) — UF ", UF_ALVO),
    subtitle = paste0("Comparação ", ANO_INI_GAIN, " vs ", ANO_FIM_GAIN,
                      " (dx<0 para ", ANO_INI_GAIN, ", dx>0 para ", ANO_FIM_GAIN, ")"),
    x        = "dx (escala relativa)",
    y        = "Idade",
    fill     = "Ano"
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position   = "bottom",
    legend.background = ggplot2::element_rect(fill = "white", colour = NA)
  )

ggplot2::ggsave(
  file.path(FIG_DIR, paste0("piramide_mortalidade_dx_UF_", UF_ALVO, ".png")),
  p_piramide, width = 6, height = 6, dpi = 300
)

# ------------ 12) Curvas de log(mx) por idade — UF + municípios foco --------

# 12a) UF
curvas_logmx_uf <- purrr::map_dfr(
  ANOS_CURVAS_LOGMX,
  ~ {
    ano_k <- .x
    mx_k  <- uf_mx_por_idade(ano_k)
    if (nrow(mx_k) == 0) return(NULL)
    mx_k %>%
      dplyr::mutate(
        ano   = ano_k,
        logmx = dplyr::if_else(mx > 0, log(mx), NA_real_)
      )
  }
)

p_logmx_idade_uf <- ggplot2::ggplot(
  curvas_logmx_uf,
  ggplot2::aes(x = idade, y = logmx, color = factor(ano))
) +
  ggplot2::geom_line() +
  ggplot2::labs(
    title    = paste0("Curvas de log(mx) por idade — UF ", UF_ALVO),
    subtitle = paste0("Anos: ", paste(ANOS_CURVAS_LOGMX, collapse = ", "),
                      " • Sexo: ", SEXO_ALVO),
    x        = "Idade",
    y        = "log(mx)",
    color    = "Ano"
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position   = "bottom",
    legend.background = ggplot2::element_rect(fill = "white", colour = NA)
  )

ggplot2::ggsave(
  file.path(FIG_DIR, paste0("curvas_logmx_por_idade_UF_", UF_ALVO, "_", SEXO_ALVO, ".png")),
  p_logmx_idade_uf, width = 8, height = 4.5, dpi = 300
)

# 12b) UF vs municípios foco
if (nrow(munis_foco) > 0) {
  for (i in seq_len(nrow(munis_foco))) {
    code_i <- munis_foco$code_muni6[i]
    nome_i <- munis_foco$nome_muni[i]
    tipo_i <- munis_foco$tipo[i]
    
    message("Curvas de log(mx) por idade — município foco: ", nome_i,
            " (", code_i, ")")
    
    curvas_i <- purrr::map_dfr(
      ANOS_CURVAS_LOGMX,
      function(ano_k) {
        # UF
        mx_uf_k <- uf_mx_por_idade(ano_k)
        if (nrow(mx_uf_k) == 0) return(NULL)
        
        mx_uf_k <- mx_uf_k %>%
          dplyr::mutate(
            ano   = ano_k,
            local = paste0("UF ", UF_ALVO),
            logmx = dplyr::if_else(mx > 0, log(mx), NA_real_)
          )
        
        # Município
        mx_muni_k <- mx_muni_por_idade(code_muni6_sel = code_i,
                                       ano_sel        = ano_k)
        if (nrow(mx_muni_k) == 0) return(mx_uf_k)
        
        mx_muni_k <- mx_muni_k %>%
          dplyr::mutate(
            ano   = ano_k,
            local = nome_i,
            logmx = dplyr::if_else(mx > 0, log(mx), NA_real_)
          )
        
        dplyr::bind_rows(mx_uf_k, mx_muni_k)
      }
    )
    
    if (nrow(curvas_i) == 0) next
    
    p_logmx_idade_muni <- ggplot2::ggplot(
      curvas_i,
      ggplot2::aes(x = idade, y = logmx, color = local)
    ) +
      ggplot2::geom_line(size = 0.9) +
      ggplot2::facet_wrap(~ ano, nrow = 1) +
      ggplot2::labs(
        title    = paste0("Curvas de log(mx) por idade — ", nome_i),
        subtitle = paste0(
          "Comparação com UF ", UF_ALVO,
          " • Anos: ", paste(ANOS_CURVAS_LOGMX, collapse = ", "),
          " • Sexo: ", SEXO_ALVO,
          " • Tipo de município: ", tipo_i
        ),
        x        = "Idade",
        y        = "log(mx)",
        color    = "Local"
      ) +
      ggplot2::theme_minimal() +
      ggplot2::theme(
        plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
        legend.position   = "bottom",
        legend.background = ggplot2::element_rect(fill = "white", colour = NA)
      )
    
    ggplot2::ggsave(
      file.path(
        FIG_DIR,
        paste0(
          "curvas_logmx_por_idade_muni_",
          UF_ALVO, "_", SEXO_ALVO, "_", code_i, ".png"
        )
      ),
      p_logmx_idade_muni, width = 9, height = 5, dpi = 300
    )
  }
}

# --------- 13) Superfícies idade x ano x log(mx) (UF agregado) --------------

mx_surface <- purrr::map_dfr(
  ANOS_ANALISE,
  ~ {
    ano_k <- .x
    mx_k  <- uf_mx_por_idade(ano_k)
    mx_k$ano <- ano_k
    mx_k
  }
) %>%
  dplyr::mutate(
    logmx = dplyr::if_else(mx > 0, log(mx), NA_real_)
  )

p_surface <- ggplot2::ggplot(mx_surface,
                             ggplot2::aes(x = ano, y = idade, fill = logmx)) +
  ggplot2::geom_tile() +
  viridis::scale_fill_viridis(
    option    = "magma",
    direction = -1,
    name      = "log(mx)",
    alpha     = 0.9
  ) +
  ggplot2::labs(
    title    = paste0("Superfície idade x ano de log(mx) — UF ", UF_ALVO),
    subtitle = paste0("mx estadual (sexo ", SEXO_ALVO, ")"),
    x        = "Ano",
    y        = "Idade"
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position   = "bottom",
    legend.background = ggplot2::element_rect(fill = "white", colour = NA)
  )

ggplot2::ggsave(
  file.path(FIG_DIR, paste0("superficie_logmx_", UF_ALVO, "_", SEXO_ALVO, ".png")),
  p_surface, width = 8, height = 5, dpi = 300
)

# Superfície 3D com plotly (arrumada)
if (has_plotly) {
  message("Gerando também superfície 3D interativa com plotly (opcional)...")
  mx_mat <- stats::xtabs(logmx ~ idade + ano, data = mx_surface)
  anos_vals   <- as.numeric(colnames(mx_mat))
  idade_vals  <- as.numeric(rownames(mx_mat))
  
  plotly_obj <- plotly::plot_ly(
    x = anos_vals,
    y = idade_vals,
    z = mx_mat,
    type = "surface"
  )
  
  htmlwidgets::saveWidget(
    plotly_obj,
    file.path(FIG_DIR, paste0("superficie_logmx_", UF_ALVO, "_", SEXO_ALVO, "_3D.html")),
    selfcontained = TRUE
  )
}

# -------------- 14) “Quantos anos faltam?” (déficit/superávit) --------------

faltam_df <- e0_muni_post %>%
  dplyr::filter(
    uf    == UF_ALVO,
    sexo  == SEXO_ALVO,
    nivel == "municipio",
    ano %in% ANOS_ANALISE
  ) %>%
  dplyr::mutate(
    deficit_uf   = e0_ibge - e0_p50_post,
    superavit_uf = e0_p50_post - e0_ibge
  )

readr::write_csv(
  faltam_df,
  file.path(TAB_DIR, paste0("deficit_superavit_e0_vs_uf_", UF_ALVO, "_", SEXO_ALVO, ".csv"))
)

faltam_2023 <- faltam_df %>%
  dplyr::filter(ano == ANO_FIM_GAIN) %>%
  dplyr::select(code_muni6, deficit_uf, superavit_uf)

map_faltam <- uf_muni_sf %>%
  dplyr::left_join(faltam_2023, by = "code_muni6")

p_faltam <- ggplot2::ggplot(map_faltam) +
  ggplot2::geom_sf(ggplot2::aes(fill = deficit_uf), color = NA) +
  viridis::scale_fill_viridis(
    option    = "magma",
    direction = -1,
    name      = "Déficit (anos)\n e0_muni - e0_UF < 0",
    alpha     = 0.8
  ) +
  ggplot2::labs(
    title    = paste0("“Quantos anos faltam?” — ", UF_ALVO),
    subtitle = paste0("Diferença e0 municipal vs e0 estadual (", ANO_FIM_GAIN, ")"),
    x        = NULL,
    y        = NULL
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
    legend.position   = "bottom",
    legend.background = ggplot2::element_rect(fill = "white", colour = NA),
    panel.grid        = ggplot2::element_blank(),
    axis.text         = ggplot2::element_blank(),
    axis.ticks        = ggplot2::element_blank()
  )

ggplot2::ggsave(
  file.path(FIG_DIR, paste0("mapa_deficit_e0_vs_uf_", UF_ALVO, "_", SEXO_ALVO, "_", ANO_FIM_GAIN, ".png")),
  p_faltam, width = 6, height = 6, dpi = 300
)

# ---------------- 15) Mapa LISA final (usando lisa_clusters_df) --------------

if (nrow(lisa_clusters_df) > 0) {
  map_lisa_plot <- uf_muni_sf %>%
    dplyr::left_join(lisa_clusters_df, by = "code_muni6")
  
  p_lisa <- ggplot2::ggplot(map_lisa_plot) +
    ggplot2::geom_sf(ggplot2::aes(fill = quad), color = NA) +
    ggplot2::scale_fill_manual(
      name   = "Cluster LISA",
      values = c(
        "Alto-Alto"         = "#b2182b",
        "Baixo-Baixo"       = "#2166ac",
        "Alto-Baixo"        = "#ef8a62",
        "Baixo-Alto"        = "#67a9cf",
        "Não significativo" = "#f7f7f7"
      ),
      drop = FALSE
    ) +
    ggplot2::labs(
      title    = paste0("Clusters locais de e0 pós-shrink (LISA) — ", UF_ALVO),
      subtitle = paste0("Ano: ", LISA_ANO_REF, " • Sexo: ", SEXO_ALVO),
      x        = NULL,
      y        = NULL
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      plot.background   = ggplot2::element_rect(fill = "white", colour = NA),
      legend.position   = "bottom",
      legend.background = ggplot2::element_rect(fill = "white", colour = NA),
      panel.grid        = ggplot2::element_blank(),
      axis.text         = ggplot2::element_blank(),
      axis.ticks        = ggplot2::element_blank()
    )
  
  ggplot2::ggsave(
    file.path(FIG_DIR, paste0("mapa_lisa_e0_post_", UF_ALVO, "_", SEXO_ALVO, "_", LISA_ANO_REF, ".png")),
    p_lisa, width = 6, height = 6, dpi = 300
  )
} else {
  message("Sem clusters LISA calculados — mapa LISA não será gerado.")
}

message("06_analises_avancadas_e0.R concluído.")
