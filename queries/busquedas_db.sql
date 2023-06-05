SELECT distinct busqueda, busqueda_real, convert(date, fecha_hora) as fecha_hora
FROM catalogos_PE.dbo.busqueda_resultado
WHERE fecha_hora >= '2023-04-02' AND fecha_hora <= '2023-04-04'
order by fecha_hora asc
--where convert(date, fecha_hora) = DATEADD(day, -1, CAST(GETDATE() AS date))
