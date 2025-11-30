CREATE OR REPLACE VIEW view_distribuicao_etaria_rpa AS
SELECT
    dtemp.ano AS ano_registro,
    dl.desc_rpa,
    da.faixa_etaria,
    COUNT(fm.matricula) AS total_alunos
FROM
    fato_matricula fm
JOIN
    dim_aluno da ON fm.id_aluno = da.id_aluno
JOIN
    dim_localizacao dl ON fm.id_locali = dl.id_locali
JOIN
    dim_tempo dtemp ON fm.id_tempo = dtemp.id_tempo
GROUP BY
    dtemp.ano, dl.desc_rpa, da.faixa_etaria
ORDER BY
    dtemp.ano, dl.desc_rpa, da.faixa_etaria;