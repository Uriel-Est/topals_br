###############################################################################
# 99_mapa_e0_brasil.R
#
# Lê as saídas municipais de TODOS os estados e faz choropleths da e0
# brasileira (escala municipal) para 2000 e 2023:
#   - Versão 1: e0 sem ajuste (raw)
#   - Versão 2: e0 com ajuste (pós-shrink, ou e0_raw se não houver shrink)
###############################################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(purrr)
  library(arrow)
  library(geobr)
  library(ggplot2)
  library(viridis)
  library(readr)
  library(tidyr)
  library(scales)
})

###############################################################################
# 0) CONFIGURAÇÃO
###############################################################################

BASE_DIR  <- "C:/Users/uriel/Documents/UFPB Estatística/Demografia 1/Estudos Demográficos - PB/TOPALS"
ANOS_MAPA <- c(2000L, 2023L)

UFS <- c(
  "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
  "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"
)

OUT_DIR <- file.path(BASE_DIR, "resultados", "BRASIL", "figuras")
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

###############################################################################
# 0.5) Carregar base_muni para calcular e0 crua
###############################################################################

# Lê qualquer arquivo do 05B (parquet ou csv)
read_05B_any <- function(path) {
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    arrow::read_parquet(path)
  } else if (grepl("\\.csv(\\.gz)?$", path, ignore.case = TRUE)) {
    # outputs do pipeline normalmente estão em CSV "americano" (,) e ponto
    readr::read_csv(path, show_col_types = FALSE, progress = FALSE)
  } else {
    stop("Extensão não suportada em 05B_mx_post_e0: ", basename(path))
  }
}

RDATA_PATH <- file.path(
  BASE_DIR,
  "00_prep_topals_output",
  "bases_topals_preparadas.RData"
)

if (!file.exists(RDATA_PATH)) {
  stop("Não encontrei bases_topals_preparadas.RData em: ", RDATA_PATH)
}

load(RDATA_PATH)  # traz base_muni, base_muni_raw, etc.

# e0 a partir de log(mx) – mesma função do pipeline TOPALS
e0_from_logmx <- function(logmx) {
  mx <- exp(logmx)
  px <- exp(-mx)
  lx <- c(1, cumprod(px))
  sum(head(lx, -1) + tail(lx, -1)) / 2
}

###############################################################################
# 1) Função para ler e0 crua e ajustada de UMA UF
###############################################################################

ler_e0_uf <- function(uf) {
  message("\n================ UF = ", uf, " ================")
  
  # --------------------------------------------------------------------
  # 1) e0 crua (raw) a partir de base_muni: óbitos / pop_ambos
  # --------------------------------------------------------------------
  df_raw_base <- base_muni %>%
    dplyr::filter(
      uf_sigla == !!uf,
      ano %in% ANOS_MAPA
    )
  
  if (nrow(df_raw_base) == 0L) {
    message("UF ", uf, " sem dados na base_muni – pulando.")
    return(NULL)
  }
  
  df_raw <- df_raw_base %>%
    dplyr::group_by(
      ano, uf_sigla,
      code_muni6, nome_muni
    ) %>%
    dplyr::summarise(
      e0_raw = {
        df_age <- dplyr::arrange(dplyr::cur_data_all(), idade)
        mx     <- ifelse(df_age$pop_ambos > 0,
                         df_age$obitos / df_age$pop_ambos,
                         0)
        logmx  <- log(pmax(mx, 1e-12))
        e0_from_logmx(logmx)
      },
      .groups = "drop"
    )
  
  # --------------------------------------------------------------------
  # 2) e0 ajustada (pós-shrink) a partir de 05B_mx_post_e0 (se existir)
  # --------------------------------------------------------------------
  dir_05B <- file.path(
    BASE_DIR,
    "resultados", uf, "bancos_de_dados",
    "05B_mx_post_e0"
  )
  
  df_adj <- NULL
  
  if (dir.exists(dir_05B)) {
    
    # AGORA: aceita .parquet, .csv e .csv.gz
    files_all <- list.files(
      dir_05B,
      pattern = "\\.(parquet|csv|csv\\.gz)$",
      full.names = TRUE,
      ignore.case = TRUE
    )
    
    if (length(files_all) == 0L) {
      message("  (05B_mx_post_e0 existe, mas sem parquet/csv para UF = ", uf, ").")
    } else {
      
      # 2.1) Prioridade: e0_municipios_post_shrink.*  (parquet OU csv)
      arq_e0_muni <- files_all[grepl("e0_municipios_post_shrink",
                                     basename(files_all), ignore.case = TRUE)]
      
      if (length(arq_e0_muni) > 0L) {
        arq_e0_muni <- arq_e0_muni[1]
        message("Lendo e0_municipios_post_shrink (", basename(arq_e0_muni),
                ") para UF = ", uf)
        
        tab_e0 <- read_05B_any(arq_e0_muni)
        
        if (!"code_muni6" %in% names(tab_e0)) {
          stop("e0_municipios_post_shrink em ", uf,
               " não tem coluna code_muni6.")
        }
        
        poss_e0_cols <- intersect(
          c("e0_p50_post", "e0_adj", "e0_post", "e0_final"),
          names(tab_e0)
        )
        
        if (length(poss_e0_cols) == 0L) {
          message("  (Arquivo e0_municipios_post_shrink em ", uf,
                  " sem coluna e0_p50_post/e0_adj/e0_post/e0_final.)")
        } else {
          col_e0 <- poss_e0_cols[1]
          
          df_adj <- tab_e0 %>%
            filter(
              ano %in% ANOS_MAPA,
              # se tiver coluna sexo, fica só 'b'; se não, ignora
              !("sexo" %in% names(tab_e0)) | sexo == "b"
            ) %>%
            transmute(
              ano        = as.integer(ano),
              uf_sigla   = uf,
              code_muni6 = as.integer(code_muni6),
              e0_adj     = as.numeric(.data[[col_e0]])
            ) %>%
            distinct()
        }
        
      } else {
        # 2.2) Fallback: juntar todos os arquivos do 05B e tentar ex/mx
        message("e0_municipios_post_shrink não encontrado em ", uf,
                " – tentando tabela_vida/nmx no 05B (parquet/csv).")
        
        tab_05B <- purrr::map_dfr(files_all, read_05B_any)
        
        # 2.2a) ex(idade) -> e0_adj = ex(idade 0)
        if (all(c("idade") %in% names(tab_05B)) &&
            any(c("ex_post", "ex") %in% names(tab_05B))) {
          
          col_ex <- intersect(c("ex_post", "ex"), names(tab_05B))[1]
          poss_code <- intersect(
            c("code_muni6", "code_muni", "cod_muni"),
            names(tab_05B)
          )
          
          if (length(poss_code) == 0L) {
            message("  (Tabela de vida com ex(idade), mas sem código de município em ", uf, ").")
          } else {
            code_col <- poss_code[1]
            
            df_adj <- tab_05B %>%
              filter(ano %in% ANOS_MAPA, idade == 0L) %>%
              mutate(
                code_muni6 = dplyr::case_when(
                  code_col == "code_muni6" ~ as.integer(.data[[code_col]]),
                  code_col %in% c("code_muni", "cod_muni") ~ as.integer(.data[[code_col]] %/% 10L),
                  TRUE ~ as.integer(.data[[code_col]])
                )
              ) %>%
              transmute(
                ano        = as.integer(ano),
                uf_sigla   = uf,
                code_muni6,
                e0_adj     = as.numeric(.data[[col_ex]])
              ) %>%
              distinct()
          }
          
          # 2.2b) mx(idade) -> reconstruir e0_adj via e0_from_logmx
        } else if (all(c("idade") %in% names(tab_05B)) &&
                   any(c("mx_post", "mx_final", "mx") %in% names(tab_05B))) {
          
          col_mx <- intersect(c("mx_post", "mx_final", "mx"), names(tab_05B))[1]
          poss_code <- intersect(
            c("code_muni6", "code_muni", "cod_muni"),
            names(tab_05B)
          )
          
          if (length(poss_code) == 0L) {
            message("  (Tabela de vida com mx(idade), mas sem código de município em ", uf, ").")
          } else {
            code_col <- poss_code[1]
            
            df_adj <- tab_05B %>%
              filter(ano %in% ANOS_MAPA) %>%
              mutate(
                code_muni6 = dplyr::case_when(
                  code_col == "code_muni6" ~ as.integer(.data[[code_col]]),
                  code_col %in% c("code_muni", "cod_muni") ~ as.integer(.data[[code_col]] %/% 10L),
                  TRUE ~ as.integer(.data[[code_col]])
                )
              ) %>%
              group_by(ano, uf_sigla = uf, code_muni6) %>%
              summarise(
                e0_adj = {
                  df_age <- dplyr::arrange(dplyr::cur_data_all(), idade)
                  mx     <- as.numeric(df_age[[col_mx]])
                  logmx  <- log(pmax(mx, 1e-12))
                  e0_from_logmx(logmx)
                },
                .groups = "drop"
              )
          }
          
        } else {
          message("  (Não consegui identificar formato aproveitável para e0_adj em 05B, UF = ", uf, ").")
        }
      }
    }
  } else {
    message("Pasta 05B_mx_post_e0 NÃO encontrada para UF = ", uf)
  }
  
  # --------------------------------------------------------------------
  # 3) Juntar e0_raw com e0_adj (se existir) e garantir fallback
  # --------------------------------------------------------------------
  out <- df_raw
  
  if (!is.null(df_adj) && nrow(df_adj) > 0L) {
    out <- out %>%
      dplyr::left_join(df_adj,
                       by = c("ano", "uf_sigla", "code_muni6")
      )
  } else {
    out <- out %>%
      dplyr::mutate(e0_adj = NA_real_)
  }
  
  # Fallback: onde não houver ajuste, usar e0_raw
  out <- out %>%
    dplyr::mutate(
      e0_adj = dplyr::coalesce(e0_adj, e0_raw)
    )
  
  out
}

###############################################################################
# 2) Agregar todos os estados num único data frame Brasil
###############################################################################

lista_e0 <- purrr::map(UFS, ler_e0_uf)
lista_e0 <- purrr::compact(lista_e0)

if (length(lista_e0) == 0L) {
  stop("Nenhum dado de e0 carregado. Verifique BASE_DIR e quais UFs já têm saída.")
}

e0_br <- dplyr::bind_rows(lista_e0) %>%
  dplyr::filter(ano %in% ANOS_MAPA) %>%
  dplyr::distinct(ano, uf_sigla, code_muni6, .keep_all = TRUE)

message("Resumo de e0_br (linhas por UF e ano):")
print(e0_br %>% dplyr::count(uf_sigla, ano))

message("Dimensão de e0_br após filtro de anos: ", nrow(e0_br), " linhas.")

###############################################################################
# 2.5) Salvar Brasil completo em .parquet
###############################################################################

OUT_DB_DIR <- file.path(BASE_DIR, "resultados", "BRASIL", "bancos_de_dados")
dir.create(OUT_DB_DIR, showWarnings = FALSE, recursive = TRUE)

e0_br_path <- file.path(
  OUT_DB_DIR,
  "e0_municipios_BR_2000_2023_raw_adj.parquet"
)
arrow::write_parquet(e0_br, e0_br_path)
message("Base Brasil salva em: ", e0_br_path)

# Intervalo global de e0 (para usar mesma escala em todos os mapas)
e0_min <- floor(
  min(e0_br$e0_raw, e0_br$e0_adj, na.rm = TRUE)
)
e0_max <- ceiling(
  max(e0_br$e0_raw, e0_br$e0_adj, na.rm = TRUE)
)
e0_breaks <- seq(e0_min, e0_max, by = 5)

# Quebrar e0_adj em quantis globais
e0_breaks_q <- quantile(
  e0_br$e0_adj,
  probs = seq(0, 1, length.out = 8), # 7 classes
  na.rm = TRUE
)

# Arredonda os limites pra deixar bonitinho
e0_breaks_q <- round(e0_breaks_q, 1)

# Rótulos tipo "[65.0, 68.3]"
e0_labels_q <- paste0(
  "[", head(e0_breaks_q, -1), ", ", tail(e0_breaks_q, -1), "]"
)

###############################################################################
# 3) Ler shapefile de municípios do Brasil (geobr) – forçando novo download
###############################################################################

geobr::list_geobr()  # apenas garante metadata ok

mun_br <- geobr::read_municipality(
  code_muni  = "all",
  year       = 2020,
  simplified = TRUE,
  showProgress = TRUE,
  cache      = FALSE
) %>%
  dplyr::mutate(
    code_muni6 = as.integer(code_muni %/% 10L)
  )

###############################################################################
# 4) Função para gerar o mapa (choropleth)
###############################################################################

plot_e0_mapa <- function(df_ano, col_valor, ano, tipo, out_file) {
  df_plot <- df_ano %>%
    dplyr::mutate(
      e0_cat = cut(
        .data[[col_valor]],
        breaks = e0_breaks_q,
        include.lowest = TRUE,
        right = TRUE,
        labels = e0_labels_q
      )
    )
  
  p <- ggplot(df_plot) +
    geom_sf(aes(fill = e0_cat), color = NA, size = 0) +
    scale_fill_viridis_d(
      option   = "magma",
      direction = -1,
      drop      = FALSE,
      name      = "e0 (anos)",
      guide     = guide_legend(
        title.position = "top",
        nrow = 1
      )
    ) +
    theme_void() +
    theme(
      legend.position = "bottom",
      legend.title    = element_text(size = 9),
      legend.text     = element_text(size = 8)
    )
  
  ggsave(
    filename = out_file,
    plot     = p,
    width    = 10,
    height   = 9,
    dpi      = 300
  )
  
  message("Mapa salvo em: ", out_file)
}

###############################################################################
# 5) Gerar mapas para 2000 e 2023 (raw e ajustado) – versão ggplot
###############################################################################

for (ano_i in ANOS_MAPA) {
  df_ano <- mun_br %>%
    dplyr::left_join(
      e0_br %>% dplyr::filter(ano == ano_i),
      by = "code_muni6"
    )
  
  # Versão 1: e0 sem ajuste
  out_raw <- file.path(
    OUT_DIR,
    sprintf("mapa_e0_municipal_%d_sem_ajuste.png", ano_i)
  )
  # aqui uso a função plot_e0_mapa que você já definiu acima no script
  plot_e0_mapa(df_ano, "e0_raw", ano_i, "Sem ajuste (estimativa bruta)", out_raw)
  
  # Versão 2: e0 com ajuste (onde houver; nos demais, = e0_raw)
  out_adj <- file.path(
    OUT_DIR,
    sprintf("mapa_e0_municipal_%d_com_ajuste.png", ano_i)
  )
  plot_e0_mapa(df_ano, "e0_adj", ano_i, "Com ajuste (pós-shrink ou raw)", out_adj)
}

message("✅ Mapas de e0 (2000 e 2023) via ggplot gerados com sucesso.")

###############################################################################
# 5b) Mapas 2x2 com ggplot2 (2000/2023 x raw/ajustado)
###############################################################################

library(ggplot2)
library(tidyr)
library(scales)
library(geobr)

# 5b.1) garantir shapes de UFs (estados) - se ainda não fez isso antes
ufs <- geobr::read_state(year = 2020, simplified = TRUE)

# 5b.2) Se ainda não tiver feito o clamp e a faixa global, garante aqui:

qs <- quantile(e0_br$e0_adj, probs = c(0.02, 0.98), na.rm = TRUE)
e0_lo <- max(60, floor(qs[1]))
e0_hi <- min(90, ceiling(qs[2]))

message("Faixa de cor (e0) usada nos mapas: [", e0_lo, ", ", e0_hi, "]")

sf_e0 <- mun_br %>%
  dplyr::left_join(e0_br, by = "code_muni6") %>%
  dplyr::filter(ano %in% ANOS_MAPA) %>%
  dplyr::mutate(
    e0_raw_plot = pmin(pmax(e0_raw, e0_lo), e0_hi),
    e0_adj_plot = pmin(pmax(e0_adj, e0_lo), e0_hi)
  )

# 5b.3) Deixar em formato "longo" para facetas (sem mexer na geometria!)
sf_e0_long <- sf_e0 %>%
  dplyr::select(ano, uf_sigla, code_muni6, e0_raw_plot, e0_adj_plot) %>%  # <- sem "geometry"
  tidyr::pivot_longer(
    cols      = c(e0_raw_plot, e0_adj_plot),
    names_to  = "tipo_raw",
    values_to = "e0_plot"
  ) %>%
  dplyr::mutate(
    Tipo = dplyr::case_when(
      tipo_raw == "e0_raw_plot" ~ "Sem ajuste",
      tipo_raw == "e0_adj_plot" ~ "Com ajuste",
      TRUE ~ tipo_raw
    ),
    Ano  = factor(ano),
    Tipo = factor(Tipo, levels = c("Sem ajuste", "Com ajuste"))  # ordem certinha no 2x2
  )

# 5b.4) GGplot 2x2: sem borda municipal, só borda estadual
p_2x2 <- ggplot(sf_e0_long) +
  geom_sf(aes(fill = e0_plot), colour = NA) +  # <- sem fronteira municipal
  geom_sf(data = ufs, fill = NA, colour = "black", linewidth = 0.25) +
  facet_grid(rows = vars(Tipo), cols = vars(Ano)) +
  scale_fill_viridis_c(
    option    = "magma",
    direction = -1,
    limits    = c(e0_lo, e0_hi),
    oob       = scales::squish,
    na.value  = "grey90",
    name      = "e0 (anos)"
  ) +
  coord_sf(expand = FALSE) +
  theme_void() +
  theme(
    strip.text.x      = element_text(size = 10, face = "bold"),
    strip.text.y      = element_text(size = 10, face = "bold"),
    legend.position   = "bottom",
    legend.direction  = "horizontal",
    legend.title      = element_text(size = 8),
    legend.text       = element_text(size = 7),
    legend.key.width  = grid::unit(1.2, "lines"),
    legend.key.height = grid::unit(0.5, "lines"),
    plot.background   = element_rect(fill = "white", colour = NA),
    panel.spacing     = grid::unit(0.8, "lines"),
    plot.margin       = margin(5, 5, 5, 5)
  )

# 5b.5) Exportar PNG 3000x2700 px, 300 dpi, fundo branco
arquivo_2x2 <- file.path(
  OUT_DIR,
  "mapa_e0_municipal_BR_2000_2023_raw_vs_ajustado_2x2.png"
)

ggsave(
  filename = arquivo_2x2,
  plot     = p_2x2,
  width    = 3000 / 300,   # 10 in
  height   = 2700 / 300,   # 9 in
  dpi      = 300,
  bg       = "white"
)

message("Mapa 2x2 salvo em: ", arquivo_2x2)

###############################################################################
# 6) Curvas nacionais: log(mx) e e0 mediana
###############################################################################

###############################################################################
# 6.0) Função auxiliar: ler nmx ajustado (por idade) de UMA UF
###############################################################################

ler_nmx_uf_idade <- function(uf) {
  message("Carregando nmx ajustado para UF = ", uf)
  
  dir_05B <- file.path(
    BASE_DIR,
    "resultados", uf, "bancos_de_dados",
    "05B_mx_post_e0"
  )
  
  if (!dir.exists(dir_05B)) {
    message("  -> Pasta 05B_mx_post_e0 inexistente para ", uf, ". Pulando.")
    return(NULL)
  }
  
  files_all <- list.files(
    dir_05B,
    pattern = "\\.(parquet|csv|csv\\.gz)$",
    full.names = TRUE,
    ignore.case = TRUE
  )
  
  if (length(files_all) == 0L) {
    message("  -> Sem arquivos parquet/csv em 05B para ", uf, ". Pulando.")
    return(NULL)
  }
  
  # 1) Prioridade: nmx_final_municipios_idade_simples
  arq_nmx <- files_all[grepl("nmx_final_municipios_idade_simples",
                             basename(files_all),
                             ignore.case = TRUE)]
  
  # 2) Se não tiver, tentar tabela_vida_municipios_idade_simples
  if (length(arq_nmx) == 0L) {
    arq_nmx <- files_all[grepl("tabela_vida_municipios_idade_simples",
                               basename(files_all),
                               ignore.case = TRUE)]
  }
  
  if (length(arq_nmx) == 0L) {
    message("  -> Nenhum nmx/tabela_vida por idade encontrado para ", uf, ".")
    return(NULL)
  }
  
  arq_nmx <- arq_nmx[1]
  message("  -> Lendo ", basename(arq_nmx))
  tab <- read_05B_any(arq_nmx)
  
  if (!all(c("ano", "idade") %in% names(tab))) {
    message("  -> Arquivo sem 'ano' ou 'idade' em ", uf, ". Pulando.")
    return(NULL)
  }
  
  poss_code <- intersect(
    c("code_muni6", "code_muni", "cod_muni"),
    names(tab)
  )
  if (length(poss_code) == 0L) {
    message("  -> Arquivo sem código de município em ", uf, ". Pulando.")
    return(NULL)
  }
  code_col <- poss_code[1]
  
  poss_mx_adj <- intersect(
    c("mx_nmx_final", "mx_topals", "mx_post", "mx_final", "mx"),
    names(tab)
  )
  if (length(poss_mx_adj) == 0L) {
    message("  -> Arquivo sem coluna de mx ajustado em ", uf, ". Pulando.")
    return(NULL)
  }
  mx_adj_col <- poss_mx_adj[1]
  
  if ("sexo" %in% names(tab)) {
    tab <- tab %>% dplyr::filter(sexo == "b")
  }
  
  tab %>%
    dplyr::filter(ano %in% ANOS_MAPA) %>%
    dplyr::mutate(
      uf_sigla   = uf,
      code_muni6 = dplyr::case_when(
        code_col == "code_muni6" ~ as.integer(.data[[code_col]]),
        code_col %in% c("code_muni", "cod_muni") ~ as.integer(.data[[code_col]] %/% 10L),
        TRUE ~ as.integer(.data[[code_col]])
      )
    ) %>%
    dplyr::select(
      ano, uf_sigla, code_muni6, idade,
      mx_adj = dplyr::all_of(mx_adj_col)
    ) %>%
    dplyr::mutate(mx_adj = as.numeric(mx_adj))
}

###############################################################################
# 6.1) Curva log(mx) nacional – bruto vs ajustado (2000 e 2023)
###############################################################################

mx_muni_raw <- base_muni %>%
  dplyr::filter(ano %in% ANOS_MAPA) %>%
  { if ("sexo" %in% names(.)) dplyr::filter(., sexo == "b") else . } %>%
  dplyr::group_by(ano, uf_sigla, code_muni6, nome_muni, idade) %>%
  dplyr::summarise(
    obitos = sum(obitos,    na.rm = TRUE),
    pop    = sum(pop_ambos, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  dplyr::mutate(
    mx_raw = dplyr::if_else(pop > 0, obitos / pop, NA_real_)
  )

lista_nmx <- purrr::map(UFS, ler_nmx_uf_idade)
lista_nmx <- purrr::compact(lista_nmx)

if (length(lista_nmx) == 0L) {
  message("⚠️ Nenhum nmx ajustado encontrado; curva nacional usará apenas mx_raw.")
  mx_muni <- mx_muni_raw %>%
    dplyr::mutate(mx_adj = mx_raw)
} else {
  mx_adj_all <- dplyr::bind_rows(lista_nmx)
  
  mx_muni <- mx_muni_raw %>%
    dplyr::left_join(
      mx_adj_all,
      by = c("ano", "uf_sigla", "code_muni6", "idade")
    ) %>%
    dplyr::mutate(
      mx_adj = dplyr::coalesce(mx_adj, mx_raw)
    )
}

logmx_br <- mx_muni %>%
  dplyr::group_by(ano, idade) %>%
  dplyr::summarise(
    pop_total       = sum(pop, na.rm = TRUE),
    mx_raw_brasil   = sum(mx_raw * pop, na.rm = TRUE) / pop_total,
    mx_adj_brasil   = sum(mx_adj * pop, na.rm = TRUE) / pop_total,
    .groups = "drop"
  ) %>%
  tidyr::pivot_longer(
    cols = c(mx_raw_brasil, mx_adj_brasil),
    names_to  = "tipo",
    values_to = "mx"
  ) %>%
  dplyr::mutate(
    tipo  = dplyr::recode(
      tipo,
      mx_raw_brasil = "Bruto",
      mx_adj_brasil = "Ajustado"
    ),
    logmx = log(pmax(mx, 1e-12))
  )

p_logmx <- ggplot(logmx_br,
                  aes(x = idade, y = logmx,
                      color = factor(ano),
                      linetype = tipo)) +
  geom_line(size = 0.9) +
  theme_minimal() +
  theme(
    panel.grid.minor = element_blank(),
    legend.position  = "bottom"
  ) +
  labs(
    x = "Idade (anos)",
    y = "log(mx)",
    color    = "Ano",
    linetype = "Tipo"
  )

out_logmx <- file.path(
  OUT_DIR,
  "curva_logmx_nacional_bruto_ajustado_2000_2023.png"
)

ggsave(
  filename = out_logmx,
  plot     = p_logmx,
  width    = 8,
  height   = 6,
  dpi      = 300
)

message("Curva log(mx) nacional (bruto vs ajustado) salva em: ", out_logmx)

###############################################################################
# 6.2) e0 mediana nacional (raw vs ajustada), para 2000 e 2023
###############################################################################

e0_nac <- e0_br %>%
  dplyr::group_by(ano) %>%
  dplyr::summarise(
    e0_raw_mediana = median(e0_raw, na.rm = TRUE),
    e0_adj_mediana = median(e0_adj, na.rm = TRUE),
    .groups = "drop"
  )

p_e0 <- ggplot(e0_nac, aes(x = ano)) +
  geom_line(aes(y = e0_raw_mediana, color = "Sem ajuste"), size = 0.9) +
  geom_point(aes(y = e0_raw_mediana, color = "Sem ajuste"), size = 2) +
  geom_line(aes(y = e0_adj_mediana, color = "Com ajuste"), size = 0.9) +
  geom_point(aes(y = e0_adj_mediana, color = "Com ajuste"), size = 2) +
  theme_minimal() +
  theme(
    panel.grid.minor = element_blank(),
    legend.position  = "bottom"
  ) +
  scale_color_discrete(name = "Tipo de e0") +
  labs(
    x = "Ano",
    y = "e0 mediana (anos)"
  )

out_e0 <- file.path(
  OUT_DIR,
  "curva_e0_mediana_nacional_2000_2023.png"
)

ggsave(
  filename = out_e0,
  plot     = p_e0,
  width    = 7,
  height   = 5,
  dpi      = 300
)

message("Curva e0 mediana nacional salva em: ", out_e0)
