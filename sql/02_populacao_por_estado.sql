SELECT
    e.nome_estado,
    SUM(p.populacao) AS populacao_total
FROM populacao p
JOIN municipios m ON m.id_municipio = p.id_municipio
JOIN estados e ON e.id_estado = m.id_estado
GROUP BY e.nome_estado
ORDER BY populacao_total DESC;