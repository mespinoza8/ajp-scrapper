
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
import os
from datetime import datetime
import time
import json
from pathlib import Path

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

class DatabaseManager:
    """Gestor de base de datos SQLite"""
    
    def __init__(self, config_file='config.json'):
        self.config = self.load_config(config_file)
        self.connection = None
        
    def load_config(self, config_file):
        """Cargar configuración desde archivo JSON"""
        default_config = {
            'database': {
                'type': 'sqlite',
                'file': 'ajp_data.db'
            },
            'scraper': {
                'max_workers': 16,
                'timeout': 10,
                'max_events': 1302,
                'chunk_size': 1000
            }
        }
        
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    # Merge con configuración por defecto
                    for key, value in default_config.items():
                        if key not in config:
                            config[key] = value
                        elif isinstance(value, dict):
                            for subkey, subvalue in value.items():
                                if subkey not in config[key]:
                                    config[key][subkey] = subvalue
                    return config
            except Exception as e:
                logging.warning(f"Error cargando configuración: {e}. Usando configuración por defecto.")
                return default_config
        else:
            # Crear archivo de configuración por defecto
            with open(config_file, 'w') as f:
                json.dump(default_config, f, indent=2)
            logging.info(f"Archivo de configuración creado: {config_file}")
            return default_config
    
    def connect(self):
        """Conectar a la base de datos SQLite"""
        try:
            db_file = self.config['database']['file']
            self.connection = sqlite3.connect(db_file)
            self.connection.row_factory = sqlite3.Row  # Para acceso por nombre de columna
            
            logging.info(f"Conexión a SQLite establecida: {db_file}")
            return True
        except Exception as e:
            logging.error(f"Error conectando a SQLite: {e}")
            return False
    
    def close(self):
        """Cerrar conexión a la base de datos"""
        if self.connection:
            self.connection.close()
    
    def create_tables(self):
        """Crear tablas si no existen"""
        try:
            with self.connection:
                # Tabla processed_events
                self.connection.execute("""
                    CREATE TABLE IF NOT EXISTS processed_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id INTEGER UNIQUE NOT NULL,
                        event_name TEXT,
                        year INTEGER,
                        status TEXT DEFAULT 'completed' CHECK (status IN ('completed', 'failed', 'partial')),
                        matches_count INTEGER DEFAULT 0,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Tabla matches
                self.connection.execute("""
                    CREATE TABLE IF NOT EXISTS matches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        athlete1 TEXT,
                        team1 TEXT,
                        athlete2 TEXT,
                        team2 TEXT,
                        winner TEXT,
                        winner_via TEXT,
                        time TEXT,
                        category TEXT,
                        belt TEXT,
                        type TEXT,
                        weight TEXT,
                        day TEXT,
                        event_name TEXT,
                        year INTEGER,
                        event_id INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Tabla scraping_logs
                self.connection.execute("""
                    CREATE TABLE IF NOT EXISTS scraping_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id INTEGER,
                        status TEXT CHECK (status IN ('success', 'error', 'skipped')),
                        message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Crear índices
                self.connection.execute("CREATE INDEX IF NOT EXISTS idx_event_id ON processed_events(event_id)")
                self.connection.execute("CREATE INDEX IF NOT EXISTS idx_status ON processed_events(status)")
                self.connection.execute("CREATE INDEX IF NOT EXISTS idx_year ON processed_events(year)")
                self.connection.execute("CREATE INDEX IF NOT EXISTS idx_matches_event_id ON matches(event_id)")
                self.connection.execute("CREATE INDEX IF NOT EXISTS idx_matches_event_name ON matches(event_name)")
                self.connection.execute("CREATE INDEX IF NOT EXISTS idx_matches_year ON matches(year)")
                
            logging.info("Tablas creadas/verificadas exitosamente")
            return True
        except Exception as e:
            logging.error(f"Error creando tablas: {e}")
            return False
    
    def is_event_processed(self, event_id):
        """Verificar si un evento ya fue procesado completamente"""
        try:
            cursor = self.connection.execute(
                "SELECT status FROM processed_events WHERE event_id = ?", 
                (event_id,)
            )
            result = cursor.fetchone()
            return result is not None and result[0] == 'completed'
        except Exception as e:
            logging.error(f"Error verificando evento {event_id}: {e}")
            return False
    
    def mark_event_processed(self, event_id, event_name, year, matches_count, status='completed'):
        """Marcar un evento como procesado"""
        try:
            with self.connection:
                self.connection.execute("""
                    INSERT OR REPLACE INTO processed_events 
                    (event_id, event_name, year, status, matches_count, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (event_id, event_name, year, status, matches_count))
                logging.info(f"Evento {event_id} marcado como {status} con {matches_count} matches")
        except Exception as e:
            logging.error(f"Error marcando evento {event_id}: {e}")
    
    def get_unprocessed_events(self):
        """Obtener lista de eventos no procesados"""
        max_events = self.config['scraper']['max_events']
        unprocessed = []
        for event_id in range(max_events):
            if not self.is_event_processed(event_id):
                unprocessed.append(event_id)
        return unprocessed
    
    def insert_matches(self, matches_data, event_id):
        """Insertar matches y eliminar duplicados del mismo evento"""
        if not matches_data:
            return 0
        
        try:
            with self.connection:
                # Primero eliminar matches existentes del evento
                cursor = self.connection.execute(
                    "DELETE FROM matches WHERE event_id = ?", 
                    (event_id,)
                )
                if cursor.rowcount > 0:
                    logging.info(f"Eliminados {cursor.rowcount} matches existentes del evento {event_id}")
                
                # Insertar nuevos matches
                for match in matches_data:
                    self.connection.execute("""
                        INSERT INTO matches (
                            athlete1, team1, athlete2, team2, winner, winner_via, time,
                            category, belt, type, weight, day, event_name, year, event_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        match.get('athlete1', ''), match.get('team1', ''), 
                        match.get('athlete2', ''), match.get('team2', ''),
                        match.get('winner', ''), match.get('winner_via', ''), 
                        match.get('time', ''), match.get('category', ''),
                        match.get('belt', ''), match.get('type', ''), 
                        match.get('weight', ''), match.get('day', ''),
                        match.get('event', ''), match.get('year', ''), event_id
                    ))
                
                logging.info(f"Insertados {len(matches_data)} matches para el evento {event_id}")
                return len(matches_data)
                
        except Exception as e:
            logging.error(f"Error insertando matches para evento {event_id}: {e}")
            return 0
    
    def log_scraping_event(self, event_id, status, message):
        """Registrar log de scraping"""
        try:
            with self.connection:
                self.connection.execute(
                    "INSERT INTO scraping_logs (event_id, status, message) VALUES (?, ?, ?)",
                    (event_id, status, message)
                )
        except Exception as e:
            logging.error(f"Error loggeando evento {event_id}: {e}")
    
    def get_processing_stats(self):
        """Obtener estadísticas de procesamiento"""
        try:
            cursor = self.connection.execute("""
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
            return cursor.fetchone()
        except Exception as e:
            logging.error(f"Error obteniendo estadísticas: {e}")
            return None

def extraer_nombre_puro(participant_tag):
    """Extraer nombre limpio del participante"""
    if participant_tag:
        return participant_tag.find(string=True, recursive=False).strip()
    return ''

def extraer_categoria_info(categoria):
    """Extraer información de categoría del match"""
    partes = [p.strip() for p in categoria.split('/')]
    category = partes[0] if len(partes) > 0 else ''
    belt = partes[1] if len(partes) > 1 else ''
    tipo = partes[2] if len(partes) > 2 else ''
    weight = ''
    day = ''
    if len(partes) > 3:
        m = re.match(r'(\d+KG)(?:\s*\((\w+)\))?', partes[3])
        if m:
            weight = m.group(1)
            if m.group(2):
                day = m.group(2)
        else:
            weight = partes[3]
    return category, belt, tipo, weight, day

def extraer_via_y_tiempo(texto):
    """Extraer método de victoria y tiempo del match"""
    m = re.search(r'Won by ([\w ]+)\s*-\s*(\d{2}:\d{2})', texto)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    m2 = re.search(r'Won by ([\w ]+)', texto)
    if m2:
        return m2.group(1).strip(), ''
    return '', ''

def procesar_evento(event_id, head, timeout):
    """Procesar un evento específico"""
    data = []
    event_name = ''
    year = ''
    
    try:
        url = f'https://ajptour.com/en/event/{event_id}/schedule/matchlist'
        logging.info(f'Procesando evento {event_id}: {url}')
        
        response = requests.get(url, headers=head, allow_redirects=False, timeout=timeout)
        if response.status_code != 200:
            logging.warning(f"Evento {event_id} no encontrado (status: {response.status_code})")
            return [], {'event_name': '', 'year': ''}
        
        soup = bs(response.text, 'html.parser')

        # Extraer nombre del evento
        h1 = soup.find('h1')
        if h1:
            event_name = h1.text.strip()
        
        # Extraer año del evento
        date_span = soup.find('div', class_='event-header-date')
        if not date_span:
            for tag in soup.find_all(['div', 'span', 'h2', 'h3']):
                if tag and re.search(r'\d{2,4}', tag.text):
                    year_search = re.search(r'(\d{4})', tag.text)
                    if year_search:
                        year = int(year_search.group(1))
                        break
        else:
            year_search = re.search(r'(\d{4})', date_span.text)
            if year_search:
                year = int(year_search.group(1))

        # Determinar número de páginas
        paginacion = soup.find('ul', {'class': 'pagination'})
        if paginacion:
            paginas = len(paginacion.find_all('li'))
        else:
            paginas = 2

        # Procesar cada página
        for ps in range(1, paginas):
            url2 = url if ps == 1 else f'{url}?page={ps}'
            logging.info(f'  Evento {event_id} - Página {ps}/{paginas-1}')
            
            try:
                response = requests.get(url2, headers=head, allow_redirects=False, timeout=timeout)
                if response.status_code != 200:
                    continue
                    
                soup = bs(response.text, 'html.parser')
                matches = soup.find_all('div', class_="match-row well well-inverted well-extra-condensed end")
                
                logging.info(f'    Evento {event_id} - Matches encontrados en página: {len(matches)}')
                
                # Procesar cada match
                for match in matches:
                    categoria_tag = match.find_previous('div', class_='category-row')
                    categoria = categoria_tag.text.strip() if categoria_tag else ''
                    category, belt, tipo, weight, day = extraer_categoria_info(categoria)
                    
                    participantes = match.find_all('span', class_='participant')
                    clubs = match.find_all('span', class_='club')
                    
                    athlete1 = extraer_nombre_puro(participantes[0]) if len(participantes) > 0 else ''
                    athlete2 = extraer_nombre_puro(participantes[1]) if len(participantes) > 1 else ''
                    team1 = clubs[0].text.strip() if len(clubs) > 0 else ''
                    team2 = clubs[1].text.strip() if len(clubs) > 1 else ''
                    
                    # Extraer información de victoria
                    winner_via = ''
                    time = ''
                    via_tag = match.find('span', class_='text-success')
                    if via_tag:
                        winner_via, time = extraer_via_y_tiempo(via_tag.text.strip())
                    
                    if not winner_via or not time:
                        for p in participantes:
                            wv, t = extraer_via_y_tiempo(p.text.strip())
                            if wv and t:
                                winner_via, time = wv, t
                                break
                    
                    # Determinar ganador
                    winner = ''
                    for idx, p in enumerate(participantes):
                        if 'ok' in p.get('class', []):
                            winner = extraer_nombre_puro(p)
                            break
                    
                    # Agregar match a los datos
                    data.append({
                        'athlete1': athlete1,
                        'team1': team1,
                        'athlete2': athlete2,
                        'team2': team2,
                        'winner': winner,
                        'winner_via': winner_via,
                        'time': time,
                        'category': category,
                        'belt': belt,
                        'type': tipo,
                        'weight': weight,
                        'day': day,
                        'event': event_name,
                        'year': year
                    })
                    
            except Exception as e:
                logging.error(f"Error procesando página {ps} del evento {event_id}: {e}")
                continue
        
        # Retornar datos e información del evento
        event_info = {
            'event_name': event_name,
            'year': year
        }
        
        return data, event_info
        
    except Exception as e:
        logging.error(f"Error en evento {event_id}: {e}")
        return [], {'event_name': '', 'year': ''}

def main():
    """Función principal del scraper"""
    print("AJP Data Scraper")
    print("=" * 50)
    
    # Inicializar gestor de base de datos
    db_manager = DatabaseManager()
    
    # Conectar a la base de datos
    if not db_manager.connect():
        logging.error("No se pudo conectar a la base de datos. Saliendo...")
        return
    
    # Crear tablas si no existen
    if not db_manager.create_tables():
        logging.error("Error creando tablas. Saliendo...")
        return
    
    # Configuración del scraper
    scraper_config = db_manager.config['scraper']
    max_workers = scraper_config['max_workers']
    timeout = scraper_config['timeout']
    
    # Headers para requests
    head = {
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Cache-Control': 'max-age=0', 
        'Connection': 'keep-alive',
    }

    try:
        # Obtener eventos no procesados
        unprocessed_events = db_manager.get_unprocessed_events()
        logging.info(f"Eventos no procesados encontrados: {len(unprocessed_events)}")
        
        if not unprocessed_events:
            logging.info("¡Todos los eventos ya han sido procesados!")
            stats = db_manager.get_processing_stats()
            if stats:
                logging.info(f"Estadísticas: {stats}")
            return
        
        # Mostrar estadísticas actuales
        stats = db_manager.get_processing_stats()
        if stats:
            logging.info(f"Estadísticas actuales: {stats}")
        
        # Crear directorio para datos
        Path('data').mkdir(exist_ok=True)
        
        # Procesar eventos en paralelo
        all_matches = []
        processed_events = []
        
        logging.info(f"Procesando {len(unprocessed_events)} eventos usando {max_workers} workers en paralelo")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Crear futures para todos los eventos
            future_to_event = {
                executor.submit(procesar_evento, event_id, head, timeout): event_id 
                for event_id in unprocessed_events
            }
            
            # Procesar resultados conforme se completan
            completed_count = 0
            for future in as_completed(future_to_event):
                event_id = future_to_event[future]
                completed_count += 1
                
                try:
                    matches_data, event_info = future.result()
                    
                    if matches_data:
                        all_matches.extend(matches_data)
                        event_record = {
                            'event_id': event_id,
                            'event_name': event_info.get('event_name', ''),
                            'year': event_info.get('year', ''),
                            'matches_count': len(matches_data),
                            'status': 'completed'
                        }
                        processed_events.append(event_record)
                        
                        # Guardar evento procesado inmediatamente en la base de datos
                        year_value = event_info.get('year', '')
                        if year_value == '':
                            year_value = 0
                        db_manager.mark_event_processed(
                            event_id, 
                            event_info.get('event_name', ''), 
                            year_value, 
                            len(matches_data), 
                            'completed'
                        )
                        
                        # Guardar matches inmediatamente en la base de datos
                        db_manager.insert_matches(matches_data, event_id)
                        
                        db_manager.log_scraping_event(event_id, 'success', f'Procesados {len(matches_data)} matches')
                        
                        logging.info(f"Evento {event_id}: {len(matches_data)} matches procesados ({completed_count}/{len(unprocessed_events)})")
                    else:
                        event_record = {
                            'event_id': event_id,
                            'event_name': '',
                            'year': '',
                            'matches_count': 0,
                            'status': 'failed'
                        }
                        processed_events.append(event_record)
                        
                        # Guardar evento fallido inmediatamente en la base de datos
                        db_manager.mark_event_processed(event_id, '', 0, 0, 'failed')
                        db_manager.log_scraping_event(event_id, 'error', 'No se encontraron matches')
                        
                        logging.warning(f"Evento {event_id}: No se encontraron matches ({completed_count}/{len(unprocessed_events)})")
                        
                except Exception as e:
                    event_record = {
                        'event_id': event_id,
                        'event_name': '',
                        'year': '',
                        'matches_count': 0,
                        'status': 'failed'
                    }
                    processed_events.append(event_record)
                    
                    # Guardar evento con error inmediatamente en la base de datos
                    db_manager.mark_event_processed(event_id, '', 0, 0, 'failed')
                    db_manager.log_scraping_event(event_id, 'error', str(e))
                    
                    logging.error(f"Error procesando evento {event_id}: {e}")
                
                # Mostrar progreso cada 10 eventos
                if completed_count % 10 == 0:
                    logging.info(f"Progreso: {completed_count}/{len(unprocessed_events)} eventos procesados")
        
        # Guardar datos en archivos
        if all_matches:
            matches_df = pd.DataFrame(all_matches)
            matches_file = f'data/ajp_matches_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
            matches_df.to_parquet(matches_file, index=False)
            logging.info(f"Matches guardados en: {matches_file}")
            logging.info(f"Total de matches procesados: {len(all_matches)}")
        
        if processed_events:
            events_df = pd.DataFrame(processed_events)
            events_file = f'data/ajp_events_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
            events_df.to_parquet(events_file, index=False)
            logging.info(f"Eventos procesados guardados en: {events_file}")
        
        # Mostrar estadísticas finales
        stats = db_manager.get_processing_stats()
        if stats:
            logging.info(f"Estadísticas finales: {stats}")
        
        print("\n¡Procesamiento completado exitosamente!")
        
    except KeyboardInterrupt:
        logging.info("Procesamiento interrumpido por el usuario")
    except Exception as e:
        logging.error(f"Error general: {e}")
    finally:
        db_manager.close()
        logging.info("Conexión a la base de datos cerrada")

if __name__ == "__main__":
    main() 