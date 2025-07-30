# AJP Data Scraper

Web scraping para extraer datos de eventos de AJP (Abu Dhabi Jiu-Jitsu Pro) usando SQLite.

## Características

- **Base de datos SQLite**: No requiere servidor, solo un archivo
- **Scraping incremental**: Solo procesa eventos nuevos
- **Procesamiento paralelo**: Usa ThreadPool para mayor velocidad
- **Exportación de datos**: Guarda en archivos .parquet y CSV
- **Logging completo**: Registra todo el proceso

## Requisitos

- Python 3.10+
- pip3

## Instalación

### Instalación automática:
```bash
chmod +x install.sh
./install.sh
```

### Instalación manual:
```bash
# Instalar dependencias
pip3 install -r requirements.txt

# Crear directorios
mkdir -p data logs

# Hacer ejecutables
chmod +x scraper.py db_utils.py
```

## Uso

### Ejecutar el scraper:
```bash
python3 scraper.py
```

### Usar Makefile:
```bash
# Ver todos los comandos
make help

# Ejecutar scraper
make run

# Ver estadísticas
make stats

# Ver estructura de tablas
make tables

# Exportar datos
make export
```

## Estructura de la Base de Datos

### Tabla `processed_events`
- `event_id`: ID del evento
- `event_name`: Nombre del evento
- `year`: Año del evento
- `status`: Estado (completed, failed, partial)
- `matches_count`: Número de matches procesados

### Tabla `matches`
- `athlete1`, `athlete2`: Nombres de los atletas
- `team1`, `team2`: Equipos
- `winner`: Ganador del match
- `winner_via`: Método de victoria
- `time`: Tiempo del match
- `category`, `belt`, `type`, `weight`: Categoría del match
- `event_name`, `year`, `event_id`: Información del evento

### Tabla `scraping_logs`
- Registro de logs del proceso de scraping

## Configuración

El archivo `config.json` contiene:

```json
{
  "database": {
    "type": "sqlite",
    "file": "ajp_data.db"
  },
  "scraper": {
    "max_workers": 32,
    "timeout": 10,
    "max_events": 1302,
    "chunk_size": 1000
  }
}
```

## Comandos Útiles

### Ver estadísticas:
```bash
python3 db_utils.py stats
```

### Ver estructura de tablas:
```bash
python3 db_utils.py tables
```

### Exportar datos:
```bash
python3 db_utils.py export
```

### Reiniciar base de datos:
```bash
python3 db_utils.py reset
```



## Archivos Generados

- `ajp_data.db`: Base de datos SQLite
- `data/ajp_matches_*.parquet`: Datos de matches
- `data/ajp_events_*.parquet`: Datos de eventos
- `scraper.log`: Logs del proceso

## Solución de Problemas

### Base de datos vacía:
```bash
make reset  # Reiniciar base de datos
make run    # Ejecutar scraper
```

### Ver logs:
```bash
make logs
```

### Verificar estado:
```bash
make status
```

