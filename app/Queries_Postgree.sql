

SELECT * FROM questions;

SELECT COUNT(*) AS total_respostas FROM answers;


--Alternativas Mais Votadas: Alternativas que receberam mais votos por questão;
SELECT 
  question_id,
  alternativa_escolhida,
  COUNT(*) AS votos
FROM answers
GROUP BY question_id, alternativa_escolhida
ORDER BY question_id, votos DESC;


--Questões mais acertadas: Questões com maior índice de acerto;
SELECT 
  q.question_id,
  q.question_text,
  COUNT(a.answer_id) AS total_respostas,
  SUM(CASE WHEN a.alternativa_escolhida = q.alternativa_correta THEN 1 ELSE 0 END) AS total_acertos,
  ROUND(100.0 * SUM(CASE WHEN a.alternativa_escolhida = q.alternativa_correta THEN 1 ELSE 0 END) / COUNT(a.answer_id), 2) AS percentual_acerto
FROM questions q
LEFT JOIN answers a ON q.question_id = a.question_id
GROUP BY q.question_id, q.question_text
ORDER BY percentual_acerto DESC
LIMIT 10;

--Questões com mais abstenções: Ou seja, que tiveram menos votos válidos;
SELECT 
  q.question_id,
  q.question_text,
  COUNT(a.answer_id) AS total_respostas
FROM questions q
LEFT JOIN answers a ON q.question_id = a.question_id
GROUP BY q.question_id, q.question_text
ORDER BY total_respostas ASC
LIMIT 10;


--Alunos com Maior Acerto e Mais Rápidos (ranking combinado)
WITH tempos AS (
  SELECT 
    a.usuario,
    a.question_id,
    EXTRACT(EPOCH FROM (a.datahora::timestamp - LAG(a.datahora::timestamp) OVER (PARTITION BY a.usuario ORDER BY a.datahora::timestamp))) AS diff_segundos,
    a.alternativa_escolhida,
    q.alternativa_correta
  FROM answers a
  JOIN questions q ON a.question_id = q.question_id
),
desempenho AS (
  SELECT
    usuario,
    COUNT(*) AS total_respostas,
    SUM(CASE WHEN alternativa_escolhida = alternativa_correta THEN 1 ELSE 0 END) AS total_acertos,
    AVG(diff_segundos) AS tempo_medio_segundos
  FROM tempos
  GROUP BY usuario
)
SELECT 
  usuario,
  total_respostas,
  total_acertos,
  ROUND(100.0 * total_acertos / total_respostas, 2) AS taxa_acerto_percentual,
  ROUND(tempo_medio_segundos, 2) AS tempo_medio_segundos,
  RANK() OVER (ORDER BY total_acertos DESC, tempo_medio_segundos ASC NULLS LAST) AS rank_final
FROM desempenho
ORDER BY rank_final
LIMIT 10;

