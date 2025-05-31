import redis
import psycopg2

# Teste Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
print("Chaves no Redis:", r.keys('*'))

# Teste PostgreSQL
conn = psycopg2.connect(host='localhost', database='InMemory_Db', user='postgres', password='senha')
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM questions;")
print("Número de perguntas no PostgreSQL:", cur.fetchone()[0])
cur.close()
conn.close()


import redis
import psycopg2

def testar_insercao_perguntas():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    conn = psycopg2.connect(host='localhost', database='InMemory_Db', user='postgres', password='senha')
    cur = conn.cursor()

    for key in r.scan_iter("question:*"):
        q = r.hgetall(key)
        print(f"Lendo pergunta: {q}")
        try:
            question_id = int(q.get('question_id') or q.get('id') or q.get('qid'))
            question_text = q.get('question_text') or ''
            alternativa_a = q.get('alternativa_a') or ''
            alternativa_b = q.get('alternativa_b') or ''
            alternativa_c = q.get('alternativa_c') or ''
            alternativa_d = q.get('alternativa_d') or ''
            alternativa_correta = q.get('alternativa_correta') or ''
            dificuldade = q.get('dificuldade') or ''
            assunto = q.get('assunto') or ''

            cur.execute("""
                INSERT INTO questions (
                    question_id, question_text, alternativa_a, alternativa_b, alternativa_c,
                    alternativa_d, alternativa_correta, dificuldade, assunto
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (question_id) DO NOTHING
            """, (
                question_id, question_text, alternativa_a, alternativa_b, alternativa_c,
                alternativa_d, alternativa_correta, dificuldade, assunto
            ))
            conn.commit()
            print(f"Pergunta {question_id} inserida com sucesso.")
        except Exception as e:
            conn.rollback()
            print(f"Erro ao inserir pergunta {q}: {e}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    testar_insercao_perguntas()
