SELECT 
    p.numero_refax,
    id_aplicacion,
    nombre_producto,
    modelo
FROM catalogos_PE.dbo.producto p
INNER JOIN catalogos_PE.dbo.aplicacion a ON a.Numero_Refax = p.Numero_Refax
INNER JOIN catalogos_PE.dbo.modelo m ON m.id_modelo = a.id_modelo
INNER JOIN catalogos_PE.dbo.nombre_producto np ON np.id_nombre_producto = p.id_nombre_producto 