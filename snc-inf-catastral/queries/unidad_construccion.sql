SELECT
    XMLElement("Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_UnidadConstruccion", XMLAttributes(sys_guid() as "TID"),
        --XMLElement("t_id", u.id),
        --XMLElement("t_basket", '318'),
        XMLElement("id", u.id),
        XMLElement("Planta_Ubicacion", DECODE(
                u.piso_ubicacion,
                'PS-01', '1',
                'PS-02', '2',
                'PS-03', '3',
                'PS-04', '4',
                'PS-05', '5',
                'PS-06', '6',
                'PS-07', '7',
                'PS-08', '8',
                'PS-09', '9',
                'PS-10', '10',
                'PS-11', '11',
                'PS-12', '12',
                'PS-13', '13',
                'PS-14', '14',
                'PS-15', '15',
                'PS-16', '16',
                'PS-17', '17',
                'PS-18', '18',
                'PS-19', '19',
                'PS-20', '20',
                'PS-21', '21',
                'PS-22', '22',
                'PS-23', '23',
                'PS-24', '24',
                'PS-25', '25',
                'PS-26', '26',
                'PS-27', '27',
                'PS-28', '28',
                'PS-29', '29',
                'PS-30', '30',
                'PS-31', '31',
                'PS-32', '32',
                'PS-33', '33',
                'PS-34', '34',
                'PS-35', '35',
                'PS-36', '36',
                'PS-37', '37',
                'PS-38', '38',
                'PS-39', '39',
                'PS-40', '40',
                'PS-41', '41',
                'ST-01', '1',
                'ST-02', '2',
                'ST-03', '3',
                'ST-04', '4',
                'ST-05', '5',
                'ST-06', '6',
                'ST-07', '7',
                'ST-08', '8',
                '0', '1',
                '1', '1',
                '1', '1',
                '1', '1',
                '10', '10',
                '11', '11',
                '12', '12',
                '13', '13',
                '14', '14',
                '15', '15',
                '16', '16',
                '17', '17',
                '18', '18',
                '19', '19',
                '2', '2',
                '20', '20',
                '21', '21',
                '22', '22',
                '3', '3',
                '4', '4',
                '41', '1',
                '47', '1',
                '5', '5',
                '6', '6',
                '7', '7',
                '8', '8',
                '9', '9',
                '98', '2',
                '99', '1',
                '', '1'
            )
        ),
        XMLElement("Area_Construida", u.area_construida),
        XMLElement("Altura", CASE WHEN (u.altura < 1 OR u.altura > 1000) THEN NULL ELSE u.altura END),
        XMLElement("predio_id", p.id),
        XMLElement("Espacio_De_Nombres", 'RIC_UNIDADCONSTRUCCION'),
        XMLElement("Numero_Predial", p.numero_predial),
        XMLElement("Unidad_Predial", u.unidad),
        XMLElement(
            "Tipo_Construccion", INITCAP(
                LOWER(
                    CASE u.tipo_construccion WHEN 'NO CONVENCIONAL' THEN 'NO_CONVENCIONAL' ELSE u.tipo_construccion END
                )
            )
        )
    )
FROM unidad_construccion u
JOIN predio p ON u.predio_id = p.id
WHERE p.municipio_codigo = :municipio_codigo AND (u.numero_mezanines >= 0 AND u.numero_mezanines < 100)
AND REGEXP_LIKE(SUBSTR(p.numero_predial, 6, 2), '[0-9]')
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