SELECT
    XMLElement("Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_TramiteCatastral", XMLAttributes(sys_guid() as "TID"),
        XMLElement(
            "Clasificacion_Mutacion", DECODE(
                DECODE(t.tipo_tramite,
                    '1',
                    DECODE(t.clase_mutacion,
                            '1','11',
                            '2','12',
                            '3','13',
                            '4','14',
                            '5','15'
                    ), 
                    '2','16',
                    '3','17',
                    '4','18',
                    ''
                ),
                '11', 'Mutacion_Primera_Clase',
                '12', 'Mutacion_Segunda_Clase',
                '13', 'Mutacion_Tercera_Clase',
                '14', 'Mutacion_Cuarta_Clase',
                '15', 'Mutacion_Quinta_Clase',
                '16', 'Rectificaciones',
                '17', 'Complementacion',
                '18', 'Cancelacion',
                '19', 'Cancelacion'
            )
        ),
        XMLElement("Numero_Resolucion", d.numero_documento),
        XMLElement("Fecha_Resolucion", d.fecha_documento),
        XMLElement("Fecha_Radicacion", t.fecha_radicacion),
        XMLElement("predio_id", p.id),
        XMLElement("Numero_Predial", p.numero_predial)
    )
FROM tramite t
JOIN predio p ON t.predio_id = p.id
JOIN documento d ON d.tramite_id = t.id
JOIN h_predio hp ON hp.predio_id = p.id
WHERE p.municipio_codigo = :municipio_codigo AND  d.tipo_documento_id = '3015' AND hp.tipo_historia = 'P' AND REGEXP_LIKE(SUBSTR(p.numero_predial, 6, 2), '[0-9]')
AND t.fecha_radicacion IS NOT NULL AND t.estado = 'FINALIZADO_APROBADO' 
AND t.tipo_tramite NOT IN ('12', '8', '10', '13', '14', '5', '9', '6', '11')
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
){in_clause}