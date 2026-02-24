SELECT
    e.nome_estado,
    SUM(p.populacao) AS populacao_total
FROM populacao p
JOIN municipios m
    ON p.id_municipio = m.id_municipio
JOIN estados e
    ON m.id_estado = e.id_estado
WHERE p.ano = 2022
GROUP BY e.nome_estado
ORDER BY populacao_total DESC;