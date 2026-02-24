SELECT
    e.nome_estado,
    AVG(p.populacao) AS media_populacao_municipio
FROM populacao p
JOIN municipios m
    ON p.id_municipio = m.id_municipio
JOIN estados e
    ON m.id_estado = e.id_estado
WHERE p.ano = 2022
GROUP BY e.nome_estado
ORDER BY media_populacao_municipio DESC
LIMIT 1;