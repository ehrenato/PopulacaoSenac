SELECT
    m.nome_municipio,
    e.nome_estado,
    p.populacao
FROM populacao p
JOIN municipios m
    ON p.id_municipio = m.id_municipio
JOIN estados e
    ON m.id_estado = e.id_estado
WHERE p.ano = 2022
ORDER BY p.populacao DESC
LIMIT 10;