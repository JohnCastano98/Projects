SELECT 
    XMLElement("Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_AgrupacionInteresados", XMLAttributes(s),
        XMLElement("id", pp.id),
        XMLElement("Tipo", 
                CASE
                WHEN (SELECT COUNT(DISTINCT(ps.GRUPO_ETNICO)) FROM persona ps WHERE ps.ID IN (SELECT pp2.persona_id FROM persona_predio pp2 WHERE pp2.PREDIO_ID = p.id)) = 1 THEN 'Grupo_Etnico'
                    ELSE CASE
                        WHEN (SELECT COUNT(DISTINCT(ps.TIPO_PERSONA)) FROM persona ps WHERE ps.ID IN (SELECT pp2.persona_id FROM persona_predio pp2 WHERE pp2.PREDIO_ID = p.id)) > 1 THEN 'Grupo_Mixto'
                        ELSE CASE
                            WHEN (SELECT DISTINCT(ps.TIPO_PERSONA) FROM persona ps WHERE ps.ID = pp.PERSONA_ID) = 'NATURAL' THEN 'Grupo_Civil'
                            ELSE 'Grupo_Empresarial'
                        END
                    END
                END
        ),
        XMLElement("Espacio_De_Nombres", 'RIC_AgrupacionInteresados'),
        XMLElement("predio_id", p.id),
        XMLElement("persona_id", pp.persona_id)
    )
FROM persona_predio pp
JOIN predio p on pp.predio_id = p.id
WHERE p.municipio_codigo = :municipio_codigo AND REGEXP_LIKE(SUBSTR(p.numero_predial, 6, 2), '[0-9]')
AND p.tipo NOT IN (' ', 'A', 'C', 'D', 'F', 'G', 'H', 'M', 'N', 'I', 'V') AND (p.avaluo_catastral IS NOT NULL OR p.avaluo_catastral >= 0)
AND p.nupre NOT IN (
	SELECT nupre_repetido FROM (
		SELECT nupre AS nupre_repetido, count(nupre) AS contar_nupre FROM predio WHERE municipio_codigo = :municipio_codigo GROUP BY nupre
	) WHERE contar_nupre > 1
)
AND (p.nupre IS NOT NULL OR p.nupre != '')
AND pp.persona_id IN (SELECT ps.id FROM persona ps WHERE ps.TIPO_IDENTIFICACION IN ('CC', 'CE', 'NIT', 'TI', 'RC', 'P'))
AND p.id IN (
	SELECT ID_PREDIO FROM
	 (
	     SELECT
	        P.ID AS ID_PREDIO,
	        COUNT(1) AS NUMERO_PROPIETARIOS
	     FROM PREDIO P
	        INNER JOIN PERSONA_PREDIO PP ON PP.PREDIO_ID = P.ID 
	     WHERE
	        P.MUNICIPIO_CODIGO = :municipio_codigo AND pp.PERSONA_ID IN (SELECT ps.id FROM persona ps WHERE ps.TIPO_IDENTIFICACION IN ('CC', 'CE', 'NIT', 'TI', 'RC', 'P'))
	     GROUP BY
	        P.ID
	)
WHERE NUMERO_PROPIETARIOS > 1
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
GROUP BY pp.id, p.id, pp.persona_id