import sqlite3
import pandas as pd
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "natacion.db")

def run_deduplicator():
    print(f"Abriendo DB para deduplicacion: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Cargar todos los resultados y torneos
    results_query = """
        SELECT r.id as result_id, r.swimmer_id, r.event_name, r.time, r.meet_id, m.name as meet_name, m.date as meet_date
        FROM results r
        JOIN meets m ON r.meet_id = m.id
    """
    df = pd.read_sql(results_query, conn)
    
    if df.empty:
        print("No hay resultados para deduplicar.")
        conn.close()
        return {"meets_merged": 0, "results_deleted": 0}

    # Limpiamos el texto del evento por si hay pequeñas diferencias de espacios
    df['event_norm'] = df['event_name'].str.strip().str.lower()
    df['time_norm'] = df['time'].str.strip()
    
    # 2. Agrupar por (swimmer_id, event_norm, time_norm)
    # Buscamos grupos que pertenezcan a MÁS DE UN MEET DIFERENTE.
    groups = df.groupby(['swimmer_id', 'event_norm', 'time_norm'])
    
    # Mapeo de meet_id a consolidar: meet_a_eliminar -> meet_destino
    meet_merges_proposed = set()
    
    for (sid, en, tn), group in groups:
        if len(group) > 1:
            unique_meets = group['meet_id'].unique()
            if len(unique_meets) > 1:
                # ¡Hemos encontrado un match matemático! El nadador hizo el mismo tiempo en la misma prueba
                # pero está guardado bajo dos (o más) IDs de torneo distintos.
                # Asumimos que son el mismo torneo si los dates son parecidos o simplemente porque es un match.
                
                # Coleccionamos los nombres de los torneos para resolver quién gana.
                these_meets = group[['meet_id', 'meet_name']].drop_duplicates()
                
                meet_list = these_meets.to_dict('records')
                
                # Lógica para elegir "Ganador"
                # Regla 1: Torneos que empiezan con "Resultados Completos" ganan.
                # Regla 2: El torneo que NO tenga ID numerico crudo (Swimcloud)
                
                winner = None
                for m in meet_list:
                    if "resultados completos" in m['meet_name'].lower():
                        winner = m
                        break
                
                # Si ninguno dice "resultados completos", gana el de Fechida (empieza con F_ o MM_)
                if not winner:
                    for m in meet_list:
                        if m['meet_id'].startswith("F_") or m['meet_id'].startswith("MM_"):
                            winner = m
                            break
                            
                # Si sigue sin haber ganador, elegimos el primero
                if not winner:
                    winner = meet_list[0]
                    
                winner_id = winner['meet_id']
                
                current_merges = [(m['meet_id'], winner_id) for m in meet_list if m['meet_id'] != winner_id]
                for cm in current_merges:
                    meet_merges_proposed.add(cm)

    # 3. Aplicar las fusiones de torneos a nivel global
    c = conn.cursor()
    merged_meets_count = 0
    meet_drops = set()
    
    for loser_id, winner_id in meet_merges_proposed:
        if loser_id == winner_id:
            continue
            
        print(f"[MEET MERGE DETECTADO] Transfiriendo del torneo '{loser_id}' al oficial '{winner_id}' debido a huella de tiempos.")
        
        # Actualizamos TODOS los resultados (no solo el que hizo match, asumimos que si hizo match, el TORNEO entero es duplicado)
        c.execute("UPDATE results SET meet_id = ? WHERE meet_id = ?", (winner_id, loser_id))
        
        # Eliminamos el torneo perdedor
        c.execute("DELETE FROM meets WHERE id = ?", (loser_id,))
        merged_meets_count += 1
        meet_drops.add(loser_id)

    # 4. Ahora que todos están en el mismo torneo (el ganador), DEDUPLICAR filas exactas en `results`
    # Porque si fusionamos los torneos, el nadador quedó con el mismo evento y tiempo 2 veces dentro del MISMO torneo.
    print("\nBuscando filas duplicadas exactas dentro de los resultados consolidados...")
    # Buscamos registros donde (swimmer_id, meet_id, event_name, time) se repitan.
    c.execute("""
        SELECT MIN(id), swimmer_id, meet_id, event_name, time
        FROM results
        GROUP BY swimmer_id, meet_id, event_name, time
        HAVING COUNT(*) > 1
    """)
    dupes_groups = c.fetchall()
    
    results_deleted = 0
    for min_id, sid, mid, en, tn in dupes_groups:
        # Dejar MIN(id) vivo, y borrar los demás que tengan exacta misma combinación
        c.execute("""
            DELETE FROM results 
            WHERE swimmer_id = ? AND meet_id = ? AND event_name = ? AND time = ? AND id != ?
        """, (sid, mid, en, tn, min_id))
        results_deleted += c.rowcount

    conn.commit()
    conn.close()
    
    print(f"\n--- Resumen de Limpieza ---")
    print(f"Torneos fusionados/eliminados: {merged_meets_count}")
    print(f"Resultados idénticos repetidos eliminados: {results_deleted}")
    return {"meets_merged": merged_meets_count, "results_deleted": results_deleted}

if __name__ == "__main__":
    run_deduplicator()
