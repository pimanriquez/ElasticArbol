SELECT distinct busqueda, busqueda_real, convert(date, fecha_hora) as fecha_hora
FROM catalogos.dbo.busqueda_resultado
order by fecha_hora asc
--where convert(date, fecha_hora) = DATEADD(day, -1, CAST(GETDATE() AS date))
