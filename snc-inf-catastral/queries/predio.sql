SELECT
    XMLElement("Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Predio", XMLAttributes(
        CONCAT(
            SUBSTR(CONCAT('CO', 'PREDIO'), 1, 8), SUBSTR(CONCAT(p.id, '00000000'), 1, 8)
        ) as "TID"
    ),
        XMLElement("id", p.id),
        XMLElement("Departamento", p.departamento_codigo),
        XMLElement("Municipio", SUBSTR(p.municipio_codigo, 3, 3)),
        XMLElement("Codigo_Homologado", p.nupre),
        XMLElement("NUPRE", CASE p.interrelacionado_snr WHEN 'NO' THEN null ELSE p.nupre END),
        XMLElement("Codigo_ORIP", CASE WHEN LENGTH(p.CIRCULO_REGISTRAL) > 3 THEN SUBSTR(p.CIRCULO_REGISTRAL, 2, 3) ELSE p.CIRCULO_REGISTRAL END),
        XMLElement("Matricula_Inmobiliaria", CASE p.numero_registro WHEN null THEN p.numero_registro_anterior ELSE p.numero_registro END), 
        XMLElement("Numero_Predial", p.numero_predial),
        XMLElement("Numero_Predial_Anterior", REPLACE(p.numero_registro_anterior, chr(0), '')),
        XMLElement("Direccion", 
            XMLElement("LADM_COL_V3_1.LADM_Nucleo.ExtDireccion",
                XMLElement("Tipo_Direccion", 'No_Estructurada'),
                XMLElement("Es_Direccion_Principal", 'true'),
                XMLElement("Nombre_Predio", 
                    (
                        SELECT
                            TRIM(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(
                                            REPLACE(
                                                pd.direccion, chr(9), ''
                                            ), chr(9), ''
                                        ), chr(0), ''
                                    ), chr(2), ''
                                )
                            ) 
                        FROM predio_direccion pd WHERE pd.predio_id = p.id AND pd.principal = 'SI' AND ROWNUM = 1
                    )
                )
            )
        ),
        XMLElement(
            "Condicion_Predio", DECODE(
                DECODE(
                    SUBSTR(p.numero_predial, 22, 1),
                    '0','11',
                    '9','12',
                    '8','13',
                    '7','14',
                    '5','17',
                    '4','15',
                    '3','18',
                    '2','16',
                    '1','12',
                    ''
                ),
                '11', 'NPH',
                '12', CONCAT(
                    'PH', CASE SUBSTR(p.numero_predial, 22, 6) WHEN '000000' THEN '.Matriz' ELSE '.Unidad_Predial' END
                ),
                '13', CONCAT(
                    'Condominio', CASE SUBSTR(p.numero_predial, 22, 6) WHEN '000000' THEN '.Matriz' ELSE '.Unidad_Predial' END
                ),
                '14', CONCAT(
                    'Parque_Cementerio', CASE SUBSTR(p.numero_predial, 22, 6) WHEN '000000' THEN '.Matriz' ELSE '.Unidad_Predial' END
                ),
                '15', 'Via',
                '16', 'Informal',
                '17', 'Mejoras',
                '18', 'Bien_Uso_Publico'
            )
        ),
        XMLElement(
            "Destinacion_Economica", DECODE(
                DECODE(p.destino,
                    ' ','',
                    'A','20',
                    'AA','22',
                    'AB','25',
                    'AC','24',
                    'B','21',
                    'C','16',
                    'D','14',
                    'E','',
                    'F','17',
                    'G','33',
                    'H','35',
                    'I','27',
                    'J','18',
                    'K','34',
                    'L','12',
                    'M','32',
                    'N','13',
                    'O','19',
                    'P','37',
                    'Q','',
                    'R','29',
                    'S','30',
                    'T','31',
                    'U','11',
                    'V','23',
                    'W','28',
                    'X','26',
                    'Y','36',
                    'Z','15',
                    '0',''
                ),
                '11', 'Acuicola',
                '12', 'Agricola',
                '13', 'Agroindustrial',
                '14', 'Agropecuario',
                '15', 'Agroforestal',
                '16', 'Comercial',
                '17', 'Cultural',
                '18', 'Educativo',
                '19', 'Forestal',
                '20', 'Habitacional',
                '21', 'Industrial',
                '22', 'Infraestructura_Asociada_Produccion_Agropecuaria',
                '23', 'Infraestructura_Hidraulica',
                '24', 'Infraestructura_Saneamiento_Basico',
                '25', 'Infraestructura_Seguridad',
                '26', 'Infraestructura_Transporte',
                '27', 'Institucional',
                '28', 'Mineria_Hidrocarburos',
                '29', 'Lote_Urbanizable_No_Urbanizado',
                '30', 'Lote_Urbanizado_No_Construido',
                '31', 'Lote_No_Urbanizable',
                '32', 'Pecuario',
                '33', 'Recreacional',
                '34', 'Religioso',
                '35', 'Salubridad',
                '36', 'Servicios_Funerarios',
                '37', 'Uso_Publico'
    
            )
        ),
        XMLElement(
            "Tipo", DECODE(
                DECODE(
                    p.tipo,
                    ' ','',
                    'B','12',
                    'D','',
                    'E','13',
                    'M','',
                    'N','',
                    'P','17',
                    'Privado','17',
                    'Publico.Baldio','12',
                    'Publico.Fiscal','13',
                    'Publico.Patrimonial','15',
                    'Publico.Uso_Publico','13',
                    'R','18',
                    'T','18',
                    'Territorio_Colectivo','18',
                    'V','',
                    'Vacante','19',
                    'X','19',
                    p.tipo
                ),
                        '11', 'Predio.Publico',
                        '12', 'Predio.Publico.Baldio',
                        '13', 'Predio.Publico.Ejido',
                        '14', 'Predio.Publico.Fiscal',
                        '15', 'Predio.Publico.Patrimonial',
                        '16', 'Predio.Publico.Uso_Publico',
                        '17', 'Predio.Privado',
                        '18', 'Predio.Territorio_Colectivo',
                        '19', 'Predio.Vacante',
                        p.tipo
            )
        ),
        XMLElement("Avaluo_Catastral", p.avaluo_catastral),
        XMLElement("Zona",
            CASE SUBSTR(p.numero_predial, 6, 2) WHEN '00' THEN 'Rural' ELSE 'Urbana' END
        ),
        XMLElement(
            "Vigencia_Actualizacion_Catastral", CONCAT(
                (SELECT SNC_CONSERVACION.FNC_ANIOULTIMA_ACTUALIZACION(SUBSTR(p.numero_predial, 1, 7)) FROM predio WHERE ROWNUM = 1), '-01-01'
            )
        ),
        XMLElement("Estado", DECODE(
                    p.estado,
                    'ACTIVO','Activo',
                    'CANCELADO','Cerrado'
                )
        ),
        XMLElement(
            "Catastro", DECODE(
                DECODE(
                    p.tipo_catastro,
                    ' ','',
                    'A','11',
                    'L','13',
                    'V','12'
                ),
                '11', 'Autoestimacion',
                '12', 'Fiscal',
                '13', 'Ley14'
            )
        ),
        XMLElement("Espacio_De_Nombres", 'RIC_Predio'),
        XMLElement(
            "Coeficiente", (
                CASE 
                WHEN (SELECT COUNT(DISTINCT(fmp.coeficiente)) FROM ficha_matriz_predio fmp WHERE fmp.numero_predial = p.NUMERO_PREDIAL) = 1 
                    THEN (SELECT DISTINCT(fmp.coeficiente) FROM ficha_matriz_predio fmp WHERE fmp.numero_predial = p.NUMERO_PREDIAL) 
                    ELSE NULL 
                END
            )
        ),
        -- TEMPORAL
        XMLElement("ric_gestorcatastral", XMLAttributes('bfb4f268-e49c-4392-a6d4-b04826bf4724' AS "REF")),
        XMLElement("ric_operadorcatastral", XMLAttributes('a4b05dbe-ed53-48fd-9d37-221525837a6d' AS "REF"))
)
FROM predio p
WHERE p.municipio_codigo = :municipio_codigo AND REGEXP_LIKE(SUBSTR(p.numero_predial, 6, 2), '[0-9]')
AND p.tipo NOT IN (' ', 'A', 'C', 'D', 'F', 'G', 'H', 'M', 'N', 'I', 'V') AND (p.avaluo_catastral IS NOT NULL OR p.avaluo_catastral >= 0)
AND p.nupre NOT IN (
	SELECT nupre_repetido FROM (
		SELECT nupre AS nupre_repetido, count(nupre) AS contar_nupre FROM predio WHERE municipio_codigo = :municipio_codigo GROUP BY nupre
	) WHERE contar_nupre > 1
)
AND (p.nupre IS NOT NULL OR p.nupre != '')
AND p.id IN (
    SELECT pp.predio_id FROM persona_predio pp WHERE pp.persona_id IN (
        SELECT ps.id FROM persona ps WHERE ps.TIPO_IDENTIFICACION IN ('CC', 'CE', 'NIT', 'TI', 'RC', 'P')
    )
)
AND p.id IN(
    SELECT
        P.ID
    FROM PREDIO P
        INNER JOIN PERSONA_PREDIO PP ON (P.MUNICIPIO_CODIGO = :municipio_codigo AND PP.PREDIO_ID = P.ID) 
        INNER JOIN PERSONA_PREDIO_PROPIEDAD PPP ON PPP.PERSONA_PREDIO_ID = PP.ID    
    GROUP BY
        P.ID
){rownum}
