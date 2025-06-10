# ==============================================================================
# SCRIPT PARA PREENCHIMENTO E ENVIO DE RELATÓRIOS PAPJ
# ==============================================================================

# Carregar bibliotecas necessárias
library(openxlsx)
library(officer)
library(sendmailR)
library(devtools)

# ==============================================================================
# CONFIGURAÇÕES DE EMAIL
# ==============================================================================
# Defina suas credenciais de email (SUBSTITUA PELOS VALORES REAIS)
username <- ""  # ALTERE AQUI
password <- ""               # ALTERE AQUI

# ==============================================================================
# FUNÇÃO PARA PROCESSAR UM RELATÓRIO
# ==============================================================================
processar_relatorio <- function(dados_linha, template_path, output_dir) {
  
  # Extrair dados da linha
  id_documento <- as.character(dados_linha$id_documento)
  cod_acao <- as.character(dados_linha$COD_ACAO)
  responsavel <- as.character(dados_linha$Responsavel)
  nome_pj <- as.character(dados_linha$NOME_PJ_CONCATENADO)
  proced_sei <- as.character(dados_linha$proced_SEI)
  tema <- as.character(dados_linha$TEMA)
  diretriz <- as.character(dados_linha$DIRETRIZ_CONSOLIDADA)
  resultado_esperado <- as.character(dados_linha$RESULTADOS_ESPERADOS)
  email_destinatario <- as.character(dados_linha$`E-mail`)
  
  # Indicadores (substituir NA por texto vazio)
  ind_01 <- ifelse(is.na(dados_linha$IND_01), "", as.character(dados_linha$IND_01))
  ind_02 <- ifelse(is.na(dados_linha$IND_02), "", as.character(dados_linha$IND_02))
  ind_03 <- ifelse(is.na(dados_linha$IND_03), "", as.character(dados_linha$IND_03))
  ind_04 <- ifelse(is.na(dados_linha$IND_04), "", as.character(dados_linha$IND_04))
  ind_05 <- ifelse(is.na(dados_linha$IND_05), "", as.character(dados_linha$IND_05))
  ind_06 <- ifelse(is.na(dados_linha$IND_06), "", as.character(dados_linha$IND_06))
  ind_07 <- ifelse(is.na(dados_linha$IND_07), "", as.character(dados_linha$IND_07))
  ind_08 <- ifelse(is.na(dados_linha$IND_08), "", as.character(dados_linha$IND_08))
  
  # Ler o modelo do documento
  doc <- read_docx(template_path)
  
  # Lista de substituições
  substituicoes <- list(
    "id_documento" = id_documento,
    "COD_ACAO" = cod_acao,
    "Responsavel" = responsavel,
    "NOME_PJ_CONCATENADO" = nome_pj,
    "proced_SEI" = proced_sei,
    "TEMA" = tema,
    "DIRETRIZ_CONSOLIDADA" = diretriz,
    "RESULTADOS_ESPERADOS" = resultado_esperado,
    "IND_01" = ind_01,
    "IND_02" = ind_02,
    "IND_03" = ind_03,
    "IND_04" = ind_04,
    "IND_05" = ind_05,
    "IND_06" = ind_06,
    "IND_07" = ind_07,
    "IND_08" = ind_08
  )
  
  # Realizar as substituições no documento
  for (placeholder in names(substituicoes)) {
    doc <- body_replace_all_text(doc, placeholder, substituicoes[[placeholder]], fixed = TRUE)
  }
  
  # Gerar nome do arquivo de saída
  nome_arquivo <- paste0("Relatorio_PAPJ_", gsub("[^A-Za-z0-9]", "_", cod_acao), "_", 
                         gsub("[^A-Za-z0-9]", "_", responsavel), ".docx")
  caminho_arquivo <- file.path(output_dir, nome_arquivo)
  
  # Salvar o documento preenchido
  print(doc, target = caminho_arquivo)
  
  # Retornar informações para envio de email
  return(list(
    arquivo = caminho_arquivo,
    email = email_destinatario,
    responsavel = responsavel,
    nome_arquivo = nome_arquivo
  ))
}

# ==============================================================================
# FUNÇÃO PARA ENVIAR EMAIL
# ==============================================================================
enviar_email <- function(info_arquivo) {
  
  # Preparar texto do email
  nome <- info_arquivo$responsavel
  email <- info_arquivo$email
  
  texto <- paste("Exmo(a) Dr(a) ", nome, ", ","\n","\n", 
                 "Segue em anexo o relatório de acompanhamento do Plano de Atuação de Promotoria de Justiça (PAPJ).","\n","\n", 
                 "Para mais informações, acesse o link https://intranet.mpes.mp.br/age/papj-2024/.","\n",
                 "Em caso de dúvidas, entre contato com os servidores Ana Paula Senna Dan Rossoni ou Fabricio Ferraz Pêgo (equipe da Unidade de Planejamento e Projetos) da AGE.","\n","\n",
                 "Respeitosamente","\n","\n",
                 "Equipe da Assessoria de Gestão Estratégica (AGE)", sep="")
  
  cat("Enviando email para:", email, "| Responsável:", nome, "\n")
  
  # Enviar email
  tryCatch({
    sendmail(from = "age@mpes.mp.br", 
             to = c(email), 
             subject = "Relatório de Acompanhamento PAPJ", 
             msg = list(mime_part(texto), mime_part(info_arquivo$arquivo)), 
             engine = "curl", 
             engineopts = list(username = username, password = password), 
             control = list(smtpServer = "smtp://smtp.office365.com:587", verbose = TRUE))
    
    cat("✓ Email enviado com sucesso para:", email, "\n")
    return(TRUE)
    
  }, error = function(e) {
    cat("✗ Erro ao enviar email para:", email, "- Erro:", e$message, "\n")
    return(FALSE)
  })
}

# ==============================================================================
# FUNÇÃO PRINCIPAL
# ==============================================================================
processar_todos_relatorios <- function() {
  
  # Definir caminhos dos arquivos
  planilha_path <- "base_acao_exemplo.xlsx"
  template_path <- "modelo_relatorio.docx"
  output_dir <- "relatorios_gerados"
  
  # Verificar se os arquivos existem
  if (!file.exists(planilha_path)) {
    stop("Arquivo base_acao.xlsx não encontrado!")
  }
  
  if (!file.exists(template_path)) {
    stop("Arquivo modelo_relatorio.docx não encontrado!")
  }
  
  # Criar diretório de saída se não existir
  if (!dir.exists(output_dir)) {
    dir.create(output_dir)
  }
  
  # Ler dados da planilha
  cat("Lendo dados da planilha...\n")
  dados <- read.xlsx(planilha_path, sheet = 1)
  
  cat("Total de registros encontrados:", nrow(dados), "\n")
  
  # Processar cada linha
  resultados <- list()
  emails_enviados <- 0
  emails_falharam <- 0
  
  for (i in 1:nrow(dados)) {
    cat("\n--- Processando registro", i, "de", nrow(dados), "---\n")
    
    tryCatch({
      # Processar relatório
      info_arquivo <- processar_relatorio(dados[i, ], template_path, output_dir)
      cat("✓ Documento gerado:", info_arquivo$nome_arquivo, "\n")
      
      # Enviar email
      if (enviar_email(info_arquivo)) {
        emails_enviados <- emails_enviados + 1
      } else {
        emails_falharam <- emails_falharam + 1
      }
      
      resultados[[i]] <- info_arquivo
      
    }, error = function(e) {
      cat("✗ Erro ao processar registro", i, ":", e$message, "\n")
      emails_falharam <- emails_falharam + 1
    })
  }
  
  # Resumo final
  cat("\n" + rep("=", 50) + "\n")
  cat("RESUMO FINAL\n")
  cat(rep("=", 50) + "\n")
  cat("Total de registros processados:", nrow(dados), "\n")
  cat("Emails enviados com sucesso:", emails_enviados, "\n")
  cat("Emails que falharam:", emails_falharam, "\n")
  cat("Documentos gerados salvos em:", normalizePath(output_dir), "\n")
  
  return(resultados)
}

# ==============================================================================
# EXECUÇÃO DO SCRIPT
# ==============================================================================

# ATENÇÃO: Antes de executar, configure suas credenciais de email no início do script!

cat("Iniciando processamento dos relatórios PAPJ...\n")
cat("IMPORTANTE: Certifique-se de que suas credenciais de email estão configuradas!\n\n")

# Executar processamento
resultados <- processar_todos_relatorios()

cat("\nProcessamento concluído!\n")
