SELECT
    m.id_municipio,
    m.nome_municipio,
    p.populacao
FROM populacao p
JOIN municipios m ON m.id_municipio = p.id_municipio
ORDER BY p.populacao DESC
LIMIT 10;