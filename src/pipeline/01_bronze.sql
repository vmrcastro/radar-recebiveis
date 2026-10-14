-- =====================================================================
-- BRONZE — ingestão incremental (Auto Loader via read_files) dos arquivos
-- que chegam na Volume de landing. Nada é transformado: guardamos o dado
-- como chegou + metadados de origem para auditoria e linhagem.
-- =====================================================================

CREATE OR REFRESH STREAMING TABLE bronze_cadastro_lojistas
COMMENT 'Cadastro bruto de lojistas (CSV do CRM/onboarding). Contém PII — acesso restrito à engenharia.'
TBLPROPERTIES ('camada' = 'bronze')
AS SELECT *, _metadata.file_path AS _arquivo_origem, current_timestamp() AS _ingerido_em
FROM STREAM read_files('${landing}/cadastro_lojistas', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze_vendas_diarias
COMMENT 'Vendas diárias por lojista (crédito, débito e Pix recebido na conta), arquivos Parquet do autorizador.'
TBLPROPERTIES ('camada' = 'bronze')
AS SELECT *, _metadata.file_path AS _arquivo_origem, current_timestamp() AS _ingerido_em
FROM STREAM read_files('${landing}/vendas_diarias', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE bronze_registradora
COMMENT 'Agenda de recebíveis de cartão por CNPJ e credenciadora, arquivo semanal da registradora (CSV ; com decimal em vírgula).'
TBLPROPERTIES ('camada' = 'bronze')
AS SELECT *, _metadata.file_path AS _arquivo_origem, current_timestamp() AS _ingerido_em
FROM STREAM read_files('${landing}/registradora', format => 'csv', header => true, sep => ';', inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE bronze_contratos_credito
COMMENT 'Contratos de capital de giro (pagos via trava de % dos recebíveis de cartão).'
TBLPROPERTIES ('camada' = 'bronze')
AS SELECT *, _metadata.file_path AS _arquivo_origem, current_timestamp() AS _ingerido_em
FROM STREAM read_files('${landing}/contratos_credito', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze_atendimentos
COMMENT 'Atendimentos em texto livre (chat, WhatsApp, telefone, visita) exportados da plataforma de atendimento.'
TBLPROPERTIES ('camada' = 'bronze')
AS SELECT *, _metadata.file_path AS _arquivo_origem, current_timestamp() AS _ingerido_em
FROM STREAM read_files('${landing}/atendimentos', format => 'json');
