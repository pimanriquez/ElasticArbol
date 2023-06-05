select distinct 
	t.busqueda_real termino_busqueda, 
	SUM(t.resultados_bd) as cnt_resultados_bd, 
	sum(t.resultados_elastic) as ctn_resultados_elastic
from (
	select 
		db.busqueda_real, db.resultados as resultados_bd, 0 as resultados_elastic, db.fecha_hora 
	from metricas.buscador_db db
	--INNER JOIN metricas.buscador_elastic el ON el.busqueda_real = db.busqueda_real
	UNION ALL
	select 
		el.busqueda_real, 0 as resultados_bd, el.resultados as resultados_elastic, el.fecha_hora
	from metricas.buscador_elastic el
	--INNER JOIN metricas.buscador_db db ON db.busqueda_real = el.busqueda_real
) t
GROUP BY t.busqueda_real
--WHERE t.fecha_hora = '2023-04-03 00:00:00'
--order by resultados_bd desc