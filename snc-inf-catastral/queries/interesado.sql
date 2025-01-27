SELECT
    XMLElement("Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Interesado", XMLAttributes(sys_guid() as "TID"),
        XMLElement("id", ps.id),
        XMLElement(
            "Tipo",  DECODE(
                ps.tipo_persona, 
                'NATURAL', 'Persona_Natural',
                'JURIDICA', 'Persona_Juridica',
                'Persona_Natural'
            )
        ),
        XMLElement(
            "Tipo_Documento", DECODE(
                DECODE(
                    UPPER(ps.tipo_identificacion),
                    'CC','11',
                    'CE','12',
                    'NIT','13',
                    'TI','14',
                    'RC','15',
                    'P','16',
                    'NU', '17',
                    'NIUP', '17',
                    'LM', '17',
                    'X', '17',
                    'CM', '17',
                    'NUIP', '17',
                    ''
                ),
                '10', 'No se tiene',
                '11', 'Cedula_Ciudadania',
                '12', 'Cedula_Extranjeria',
                '13', 'NIT',
                '14', 'Tarjeta_Identidad',
                '15', 'Registro_Civil',
                '16', 'Pasaporte',
                '17', 'Secuencial'
            )
        ),
        XMLElement("Documento_Identidad", ps.numero_identificacion),
        XMLElement("Primer_Nombre", TRIM(ps.primer_nombre)),
        XMLElement("Segundo_Nombre", TRIM(ps.segundo_nombre)),
        XMLElement("Primer_Apellido", TRIM(ps.primer_apellido)),
        XMLElement("Segundo_Apellido", TRIM(ps.segundo_apellido)),
        XMLElement(
            "Sexo", DECODE(
                ps.sexo,
                DECODE(length(ps.sexo),0,''), 'Sin_Determinar',
                ps.sexo
            )
        ),
        XMLElement("GrupoEtnico",
            DECODE(
                ps.grupo_etnico,
                'Indígena', 'Indigena',
                ps.grupo_etnico
            )
        ),
        XMLElement("Razon_Social", ps.razon_social),
        XMLElement(
            "Estado_Civil", DECODE(
                ps.estado_civil,
                'V', 'Viudo',
                'C', 'Casado'
            )
        ),
        XMLElement("Comienzo_Vida_Util_Version", TO_CHAR(SYSDATE, 'MM-DD-YYYY HH24:MI:SS')),
        XMLElement("Espacio_De_Nombres", 'RIC_INTERESADO'),
        XMLElement("predio_id", p.id)
    )
FROM persona ps
JOIN persona_predio pp ON ps.id = pp.persona_id
JOIN predio p on pp.predio_id = p.id
WHERE p.municipio_codigo = :municipio_codigo AND REGEXP_LIKE(SUBSTR(p.numero_predial, 6, 2), '[0-9]')
AND ps.TIPO_IDENTIFICACION IN ('CC', 'CE', 'NIT', 'TI', 'RC', 'P')
AND p.tipo NOT IN (' ', 'A', 'C', 'D', 'F', 'G', 'H', 'M', 'N', 'I', 'V') AND (p.avaluo_catastral IS NOT NULL OR p.avaluo_catastral >= 0)
AND p.nupre NOT IN (
	SELECT nupre_repetido FROM (
		SELECT nupre AS nupre_repetido, count(nupre) AS contar_nupre FROM predio WHERE municipio_codigo = :municipio_codigo GROUP BY nupre
	) WHERE contar_nupre > 1
)
AND (p.nupre IS NOT NULL OR p.nupre != '')
AND p.id IN(
    SELECT
        P.ID
    FROM PREDIO P
        INNER JOIN PERSONA_PREDIO PP ON (P.MUNICIPIO_CODIGO = :municipio_codigo AND PP.PREDIO_ID = P.ID) 
        INNER JOIN PERSONA_PREDIO_PROPIEDAD PPP ON PPP.PERSONA_PREDIO_ID = PP.ID    
    GROUP BY
        P.ID
){in_clause}
GROUP BY ps.id, ps.tipo_persona, ps.tipo_identificacion, ps.numero_identificacion, ps.primer_nombre, ps.segundo_nombre, ps.primer_apellido, ps.segundo_apellido, ps.sexo, ps.grupo_etnico, ps.razon_social, ps.estado_civil, p.id