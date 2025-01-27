SELECT
    XMLElement("Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Construccion", XMLAttributes(sys_guid() as "TID"),
        XMLElement("id", u.id),
        XMLElement("Identificador", u.unidad),
        XMLElement(
            "Tipo_Construccion", INITCAP(
                LOWER(
                    CASE u.tipo_construccion WHEN 'NO CONVENCIONAL' THEN 'NO_CONVENCIONAL' ELSE u.tipo_construccion END
                )
            )
        ),
        XMLElement(
            "Tipo_Dominio", DECODE(
                'Común', 'Comun',
                INITCAP(LOWER(u.tipo_dominio))
            )
        ),
        XMLElement("Numero_Pisos", u.total_pisos_unidad),
        XMLElement("Numero_Sotanos", u.numero_sotanos),
        XMLElement("Numero_Mezanines", u.numero_mezanines),
        XMLElement("Numero_Semisotanos", u.numero_semisotanos),
        XMLElement("Anio_Construccion", u.anio_construccion),
        XMLElement("Avaluo_Construccion", CASE WHEN u.avaluo < 0 THEN NULL ELSE u.avaluo END),
        XMLElement("Area_Construccion", u.area_construida),
        XMLElement("Altura", CASE WHEN (u. <= 0 OR u.altura > 1000) THEN NULL ELSE u.altura END),
        XMLElement("Observaciones", TRIM(u.observaciones)),
        XMLElement("Codigo_Construccion", p.numero_predial),
        XMLElement("Espacio_De_Nombres", 'RIC_CONSTRUCCION'),
        XMLElement("predio_id", p.id),
        XMLElement("Numero_Predial", p.numero_predial),
        XMLElement("Unidad_Predial", u.unidad)
    )
FROM unidad_construccion u
JOIN predio p ON u.predio_id = p.id
WHERE p.municipio_codigo = :municipio_codigo 
AND REGEXP_LIKE(SUBSTR(p.numero_predial, 6, 2), '[0-9]') AND (u.numero_mezanines >= 0 AND u.numero_mezanines < 100)
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