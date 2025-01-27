SELECT
    XMLElement("Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Terreno", XMLAttributes(
        CONCAT(
            SUBSTR(CONCAT('CO', 'PREDIO'), 1, 8), SUBSTR(CONCAT(p.id, '00000000'), 1, 8)
        ) as "TID"
    ),
        XMLElement("Area_Terreno", p.area_terreno),
        XMLElement("Numero_Predial", p.numero_predial),
        XMLElement("Espacio_De_Nombres", 'RIC_Terreno'),
        XMLElement("predio_id", p.id)
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
){in_clause}