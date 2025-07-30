
import sqlite3
import json
import os
import sys
from datetime import datetime

def load_config(config_file='config.json'):
    """Cargar configuración desde archivo JSON"""
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            return json.load(f)
    else:
        print(f"Archivo de configuración no encontrado: {config_file}")
        sys.exit(1)

def connect_db(config):
    """Conectar a la base de datos SQLite"""
    try:
        db_file = config['database']['file']
        connection = sqlite3.connect(db_file)
        connection.row_factory = sqlite3.Row
        return connection
    except Exception as e:
        print(f"Error conectando a SQLite: {e}")
        return None

def show_stats():
    """Mostrar estadísticas de procesamiento"""
    config = load_config()
    connection = connect_db(config)
    
    if not connection:
        return
    
    try:
        cursor = connection.execute("""
            SELECT 
                COUNT(*) as total_events,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_events,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_events,
                SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) as partial_events,
                SUM(matches_count) as total_matches,
                MIN(processed_at) as first_processed,
                MAX(processed_at) as last_processed
            FROM processed_events
        """)
        stats = cursor.fetchone()
        
        if stats:
            print("ESTADÍSTICAS DE PROCESAMIENTO")
            print("=" * 40)
            print(f"Total de eventos: {stats[0]}")
            print(f"Eventos completados: {stats[1]}")
            print(f"Eventos fallidos: {stats[2]}")
            print(f"Eventos parciales: {stats[3]}")
            print(f"Total de matches: {stats[4]}")
            print(f"Primer procesamiento: {stats[5]}")
            print(f"Último procesamiento: {stats[6]}")
        else:
            print("No hay estadísticas disponibles")
        
        # Conteo de matches
        cursor = connection.execute("SELECT COUNT(*) FROM matches")
        matches_count = cursor.fetchone()[0]
        print(f"\nMatches en la base de datos: {matches_count}")
        
        # Últimos eventos procesados
        cursor = connection.execute("""
            SELECT event_id, event_name, year, status, matches_count, processed_at 
            FROM processed_events 
            ORDER BY processed_at DESC 
            LIMIT 10
        """)
        recent_events = cursor.fetchall()
        
        if recent_events:
            print("\nÚLTIMOS 10 EVENTOS PROCESADOS")
            print("-" * 80)
            print(f"{'ID':<5} {'Nombre':<30} {'Año':<6} {'Estado':<10} {'Matches':<8} {'Fecha'}")
            print("-" * 80)
            for event in recent_events:
                print(f"{event[0]:<5} {event[1][:28]:<30} {event[2]:<6} {event[3]:<10} {event[4]:<8} {event[5]}")
    
    except Exception as e:
        print(f"Error obteniendo estadísticas: {e}")
    finally:
        connection.close()

def show_tables():
    """Mostrar estructura de las tablas"""
    config = load_config()
    connection = connect_db(config)
    
    if not connection:
        return
    
    try:
        cursor = connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print("TABLAS EN LA BASE DE DATOS")
        print("=" * 30)
        for table in tables:
            print(f"• {table[0]}")
        
        # Mostrar estructura de cada tabla
        for table in tables:
            table_name = table[0]
            print(f"\nESTRUCTURA DE LA TABLA: {table_name}")
            print("-" * 50)
            
            cursor = connection.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            print(f"{'Campo':<20} {'Tipo':<20} {'Nulo':<5} {'Llave':<10} {'Default':<10}")
            print("-" * 70)
            for column in columns:
                print(f"{column[1]:<20} {column[2]:<20} {column[3]:<5} {column[5]:<10} {str(column[4]):<10}")
            
            # Mostrar conteo de registros
            cursor = connection.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"\nRegistros en {table_name}: {count}")
    
    except Exception as e:
        print(f"Error mostrando tablas: {e}")
    finally:
        connection.close()

def reset_database():
    """Reiniciar la base de datos (eliminar todos los datos)"""
    config = load_config()
    connection = connect_db(config)
    
    if not connection:
        return
    
    try:
        confirm = input("¿Estás seguro de que quieres eliminar TODOS los datos? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Operación cancelada")
            return
        
        with connection:
            # Eliminar datos de las tablas
            connection.execute("DELETE FROM matches")
            connection.execute("DELETE FROM processed_events")
            connection.execute("DELETE FROM scraping_logs")
            
        print("Base de datos reiniciada exitosamente")
    
    except Exception as e:
        print(f"Error reiniciando base de datos: {e}")
    finally:
        connection.close()

def export_data():
    """Exportar datos a archivos CSV"""
    import pandas as pd
    
    config = load_config()
    connection = connect_db(config)
    
    if not connection:
        return
    
    try:
        # Crear directorio de exportación
        export_dir = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(export_dir, exist_ok=True)
        
        # Exportar matches
        matches_df = pd.read_sql("SELECT * FROM matches", connection)
        matches_file = f"{export_dir}/matches.csv"
        matches_df.to_csv(matches_file, index=False)
        print(f"Matches exportados a: {matches_file}")
        
        # Exportar eventos procesados
        events_df = pd.read_sql("SELECT * FROM processed_events", connection)
        events_file = f"{export_dir}/processed_events.csv"
        events_df.to_csv(events_file, index=False)
        print(f"Eventos procesados exportados a: {events_file}")
        
        # Exportar logs
        logs_df = pd.read_sql("SELECT * FROM scraping_logs", connection)
        logs_file = f"{export_dir}/scraping_logs.csv"
        logs_df.to_csv(logs_file, index=False)
        print(f"Logs exportados a: {logs_file}")
        
        print(f"\nTodos los datos exportados al directorio: {export_dir}")
    
    except Exception as e:
        print(f"Error exportando datos: {e}")
    finally:
        connection.close()

def main():
    """Función principal"""
    if len(sys.argv) < 2:
        print("UTILIDADES DE BASE DE DATOS SQLite - AJP Scraper")
        print("=" * 50)
        print("Uso: python db_utils.py <comando>")
        print("\nComandos disponibles:")
        print("  stats     - Mostrar estadísticas de procesamiento")
        print("  tables    - Mostrar estructura de las tablas")
        print("  reset     - Reiniciar base de datos (eliminar todos los datos)")
        print("  export    - Exportar datos a archivos CSV")
        return
    
    command = sys.argv[1].lower()
    
    if command == 'stats':
        show_stats()
    elif command == 'tables':
        show_tables()
    elif command == 'reset':
        reset_database()
    elif command == 'export':
        export_data()
    else:
        print(f"Comando no reconocido: {command}")

if __name__ == "__main__":
    main() 