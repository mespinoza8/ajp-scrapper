# AJP Data Scraper - Makefile
# Sistema de web scraping para extraer datos de eventos de AJP

.PHONY: help install setup run stats tables reset export clean logs

# Variables
PYTHON = python3
PIP = pip3
SCRAPER = scraper.py
DB_UTILS = db_utils.py
CONFIG = config.json

# Colores para output
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m # No Color

help: ## Mostrar esta ayuda
	@echo "$(GREEN)AJP Data Scraper - Comandos disponibles$(NC)"
	@echo "=========================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-15s$(NC) %s\n", $$1, $$2}'

install: ## Instalar dependencias de Python
	@echo "$(GREEN)Instalando dependencias...$(NC)"
	$(PIP) install -r requirements.txt
	@echo "$(GREEN)Dependencias instaladas$(NC)"

setup: ## Configurar el proyecto (instalar dependencias y crear directorios)
	@echo "$(GREEN)Configurando proyecto...$(NC)"
	$(MAKE) install
	mkdir -p data
	mkdir -p logs
	@echo "$(GREEN)Proyecto configurado$(NC)"

run: ## Ejecutar el scraper
	@echo "$(GREEN)Ejecutando scraper...$(NC)"
	$(PYTHON) $(SCRAPER)

run-background: ## Ejecutar el scraper en segundo plano
	@echo "$(GREEN)Ejecutando scraper en segundo plano...$(NC)"
	nohup $(PYTHON) $(SCRAPER) > logs/scraper_$(shell date +%Y%m%d_%H%M%S).log 2>&1 &
	@echo "$(GREEN)Scraper ejecutándose en segundo plano$(NC)"

stats: ## Mostrar estadísticas de la base de datos
	@echo "$(GREEN)Mostrando estadísticas...$(NC)"
	$(PYTHON) $(DB_UTILS) stats

tables: ## Mostrar estructura de las tablas
	@echo "$(GREEN)Mostrando estructura de tablas...$(NC)"
	$(PYTHON) $(DB_UTILS) tables

reset: ## Reiniciar base de datos (eliminar todos los datos)
	@echo "$(RED)Reiniciando base de datos...$(NC)"
	$(PYTHON) $(DB_UTILS) reset

export: ## Exportar datos a archivos CSV
	@echo "$(GREEN)Exportando datos...$(NC)"
	$(PYTHON) $(DB_UTILS) export

logs: ## Mostrar logs del scraper
	@echo "$(GREEN)Mostrando logs...$(NC)"
	@if [ -f scraper.log ]; then \
		tail -f scraper.log; \
	else \
		echo "$(YELLOW)No hay archivo de logs disponible$(NC)"; \
	fi

logs-recent: ## Mostrar logs recientes (últimas 50 líneas)
	@echo "$(GREEN)Mostrando logs recientes...$(NC)"
	@if [ -f scraper.log ]; then \
		tail -50 scraper.log; \
	else \
		echo "$(YELLOW)No hay archivo de logs disponible$(NC)"; \
	fi

clean: ## Limpiar archivos temporales y logs
	@echo "$(YELLOW)Limpiando archivos temporales...$(NC)"
	rm -f scraper.log
	rm -rf __pycache__
	rm -rf *.pyc
	@echo "$(GREEN)Limpieza completada$(NC)"

clean-data: ## Limpiar directorio de datos
	@echo "$(YELLOW)Limpiando directorio de datos...$(NC)"
	rm -rf data/*
	@echo "$(GREEN)Datos limpiados$(NC)"

clean-db: ## Eliminar archivo de base de datos
	@echo "$(RED)Eliminando base de datos...$(NC)"
	rm -f ajp_data.db
	@echo "$(GREEN)Base de datos eliminada$(NC)"

status: ## Mostrar estado del proyecto
	@echo "$(GREEN)ESTADO DEL PROYECTO$(NC)"
	@echo "=========================="
	@echo "Archivos principales:"
	@ls -la $(SCRAPER) $(DB_UTILS) $(CONFIG) 2>/dev/null || echo "$(RED)Faltan archivos principales$(NC)"
	@echo ""
	@echo "Dependencias:"
	@$(PIP) list | grep -E "(pandas|requests|beautifulsoup4)" || echo "$(RED)Dependencias no instaladas$(NC)"
	@echo ""
	@echo "Base de datos:"
	@if [ -f ajp_data.db ]; then \
		echo "$(GREEN)Archivo de base de datos encontrado$(NC)"; \
		ls -lh ajp_data.db; \
	else \
		echo "$(YELLOW)Archivo de base de datos no encontrado$(NC)"; \
	fi

test-connection: ## Probar conexión a la base de datos
	@echo "$(GREEN)Probando conexión a la base de datos...$(NC)"
	@$(PYTHON) -c "from scraper import DatabaseManager; db = DatabaseManager(); print('Conexión exitosa' if db.connect() else 'Error de conexión'); db.close()"

config: ## Mostrar configuración actual
	@echo "$(GREEN)Configuración actual:$(NC)"
	@if [ -f $(CONFIG) ]; then \
		cat $(CONFIG) | python3 -m json.tool; \
	else \
		echo "$(RED)Archivo de configuración no encontrado$(NC)"; \
	fi

monitor: ## Monitorear el scraper en tiempo real
	@echo "$(GREEN)Monitoreando scraper...$(NC)"
	@echo "$(YELLOW)Presiona Ctrl+C para detener$(NC)"
	@tail -f scraper.log

# Comandos de desarrollo
dev-install: ## Instalar dependencias de desarrollo
	@echo "$(GREEN)Instalando dependencias de desarrollo...$(NC)"
	$(PIP) install -r requirements.txt
	$(PIP) install black flake8 pytest
	@echo "$(GREEN)Dependencias de desarrollo instaladas$(NC)"

format: ## Formatear código con black
	@echo "$(GREEN)Formateando código...$(NC)"
	black $(SCRAPER) $(DB_UTILS)

lint: ## Verificar código con flake8
	@echo "$(GREEN)Verificando código...$(NC)"
	flake8 $(SCRAPER) $(DB_UTILS)

test: ## Ejecutar tests
	@echo "$(GREEN)Ejecutando tests...$(NC)"
	pytest tests/ -v

# Comandos de respaldo
backup: ## Crear respaldo de la base de datos
	@echo "$(GREEN)Creando respaldo...$(NC)"
	@$(PYTHON) $(DB_UTILS) export
	@echo "$(GREEN)Respaldo creado$(NC)"

restore: ## Restaurar datos desde archivo CSV (requiere archivo)
	@echo "$(RED)Función de restauración no implementada$(NC)"
	@echo "Usa el comando export para crear respaldos"

# Información del proyecto
info: ## Mostrar información del proyecto
	@echo "$(GREEN)INFORMACIÓN DEL PROYECTO$(NC)"
	@echo "========================"
	@echo "Descripción: Sistema de web scraping para AJP"
	@echo "Python: $(shell python3 --version)"
	@echo "Pip: $(shell pip3 --version)"
	@echo "Directorio: $(shell pwd)"
	@echo "Última modificación: $(shell date -r $(SCRAPER) 2>/dev/null || echo 'N/A')" 