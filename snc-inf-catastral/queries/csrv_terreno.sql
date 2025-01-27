
TRUNCATE TABLE csrv_popayan.csrv_terreno cascade;
-- AJUSTAR EL SISTEMA DE REFERENCIA AL OFICIAL -- 
select UpdateGeometrySRID('prueba_levcat_1', 'csrv_terreno', 'geometria', 9377);

---- CREAR UNA NUEVA COLUMNA DE GEOMETRIA PARA CARGAR LOS DATOS CORREGIDOS --
ALTER TABLE prueba_levcat_1."csrv_terreno" ADD geometria2 public.geometry(multipolygonz, 9377) NULL;

update prueba_levcat_1.prueba_levcat_1."csrv_terreno" set geometria2 = st_force3d(st_multi(st_curvetoline(geometria)));
ALTER TABLE csrv_popayan.csrv_terreno ADD predio_id int8 NULL;
ALTER TABLE csrv_popayan.csrv_terreno ADD numero_predial varchar(200) NULL;
insert into csrv_popayan.csrv_terreno (t_basket , predio_id , numero_predial , area_terreno , geometria , comienzo_vida_util_version , espacio_de_nombres , local_id )
select 
(select t_id from csrv_popayan.t_ili2db_basket tib where tib.topic = 'Modelo_Aplicacion_LADMCOL_CSRV_V0_2.Conservacion_Catastral') t_basket
, cast (t.id as int8)
, t.numero_predial
, cast (t.area_terreno as numeric(15, 1))
, ct.geometria2 geometria
, current_date comienzo_vida_util_version
, t.espacio_de_nombres 
, uuid_generate_v4()
from
prueba_levcat_1.prueba_levcat_1."Terreno" t 
join prueba_levcat_1.prueba_levcat_1.csrv_terreno ct on t.numero_predial = ct.codigo
where ct.codigo  is not null;