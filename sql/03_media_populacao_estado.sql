SELECT
    e.nome_estado,
    AVG(p.populacao) AS media_populacao
FROM populacao p
JOIN municipios m ON m.id_municipio = p.id_municipio
JOIN estados e ON e.id_estado = m.id_estado
GROUP BY e.nome_estado
ORDER BY media_populacao DESC;