import redis
import psycopg2
from datetime import datetime

# Configurações de conexão 
redis_host = 'localhost'
redis_port = 6379
pg_host = 'localhost'
pg_database = 'InMemory_Db'
pg_user = 'postgres'
pg_password = 'senha'

def conectar_redis():
    return redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

def conectar_postgres():
    return psycopg2.connect(
        host=pg_host,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )

def migrar_perguntas(r, cur, conn):
    print("Iniciando migração das perguntas...")
    questions = []
    for key in r.scan_iter("question:*"):
        q_hash = r.hgetall(key)
        if q_hash:
            questions.append((key, q_hash))  # guardar a chave para extrair o ID

    if not questions:
        print("Nenhuma pergunta encontrada no Redis.")
        return

    for key, q in questions:
        try:
            question_id = int(key.split(':')[1])  # extrai o ID da chave Redis

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
            print(f"Pergunta {question_id} inserida.")
        except Exception as e:
            conn.rollback()
            print(f"Erro ao inserir pergunta {q}: {e}")

def migrar_respostas(r, cur, conn):
    print("Iniciando migração das respostas...")
    answers = []
    for key in r.scan_iter("answer:*"):
        a_hash = r.hgetall(key)
        if a_hash:
            answers.append(a_hash)

    if not answers:
        print("Nenhuma resposta encontrada no Redis.")
        return

    for a in answers:
        try:
            question_id = int(a.get('question_id') or a.get('id') or a.get('qid'))
            alternativa_escolhida = a.get('alternativa_escolhida') or ''
            datahora_str = a.get('datahora') or ''
            usuario = a.get('usuario') or ''
            nro_tentativa = int(a.get('nro_tentativa') or 1)

            try:
                datahora_obj = datetime.strptime(datahora_str, '%Y-%m-%d %H:%M:%S')
            except Exception:
                datahora_obj = None

            cur.execute("""
                INSERT INTO answers (
                    question_id, alternativa_escolhida, datahora, usuario, nro_tentativa
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                question_id, alternativa_escolhida, datahora_obj, usuario, nro_tentativa
            ))
            conn.commit()
            print(f"Resposta para pergunta {question_id} inserida.")
        except Exception as e:
            conn.rollback()
            print(f"Erro ao inserir resposta {a}: {e}")

def main():
    r = conectar_redis()
    conn = conectar_postgres()
    cur = conn.cursor()

    migrar_perguntas(r, cur, conn)
    migrar_respostas(r, cur, conn)

    cur.close()
    conn.close()
    print("Migração concluída com sucesso!")

if __name__ == "__main__":
    main()
