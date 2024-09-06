BEGIN    ;

DROP      SCHEMA IF EXISTS dashboard_vale CASCADE;

CREATE    SCHEMA dashboard_vale;

SET       search_path TO 'dashboard_vale';

CREATE    MATERIALIZED VIEW dashboard_vale AS
SELECT    p.nome,
          p.processo,
          f.fase,
          f.uf,
          p.area_ha,
          CASE
                    WHEN f.fase IN (
                    'AUTORIZAÇÃO DE PESQUISA',
                    'CONCESSÃO DE LAVRA',
                    'DIREITO DE REQUERER A LAVRA',
                    'LAVRA GARIMPEIRA',
                    'LICENCIAMENTO',
                    'REGISTRO DE EXTRAÇÃO',
                    'REQUERIMENTO DE LAVRA'
                    ) THEN TRUE
                    ELSE FALSE
          END AS titular,
          a.data_arrecadacao AS ultima_arrecadacao
FROM      sigmine.processos p
JOIN      sigmine.fases f ON p.processo = f.processo AND      
          f.uf IS NOT NULL AND      
          f.fase IS NOT NULL
LEFT JOIN cfem.arrecadacoes a ON p.processo = a.processo
ORDER BY  1,
          a.data_arrecadacao DESC;

GRANT     USAGE ON SCHEMA dashboard_vale TO vale;

GRANT    
SELECT    ON dashboard_vale TO vale;

REFRESH   MATERIALIZED VIEW dashboard_vale;

COMMIT   ;