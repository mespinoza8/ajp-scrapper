#!/bin/bash

# AJP Data Scraper - Script de Instalación
# Versión simplificada sin MySQL

set -e

# Colores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}AJP Data Scraper - Instalación${NC}"
echo "=========================================="

# Verificar Python
echo -e "${YELLOW}Verificando requisitos...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python 3 no está instalado${NC}"
    echo "Por favor instala Python 3.8+ y vuelve a intentar"
    exit 1
fi

if ! command -v pip3 &> /dev/null; then
    echo -e "${RED}pip3 no está instalado${NC}"
    echo "Por favor instala pip3 y vuelve a intentar"
    exit 1
fi

echo -e "${GREEN}Python $(python3 --version) encontrado${NC}"
echo -e "${GREEN}pip3 $(pip3 --version) encontrado${NC}"

# Instalar dependencias
echo -e "${YELLOW}Instalando dependencias...${NC}"
pip3 install -r requirements.txt
echo -e "${GREEN}Dependencias instaladas${NC}"

# Crear directorios
echo -e "${YELLOW}Creando directorios...${NC}"
mkdir -p data
mkdir -p logs
echo -e "${GREEN}Directorios creados${NC}"

# Verificar archivo de configuración
if [ ! -f "config.json" ]; then
    echo -e "${YELLOW}Creando archivo de configuración...${NC}"
    cat > config.json << EOF
{
  "database": {
    "type": "sqlite",
    "file": "ajp_data.db"
  },
  "scraper": {
    "max_workers": 16,
    "timeout": 10,
    "max_events": 1302,
    "chunk_size": 1000
  }
}
EOF
    echo -e "${GREEN}Archivo de configuración creado${NC}"
else
    echo -e "${GREEN}Archivo de configuración encontrado${NC}"
fi

# Hacer ejecutables los scripts
chmod +x scraper.py
chmod +x db_utils.py

echo -e "${GREEN}¡Instalación completada!${NC}"
echo ""
echo -e "${YELLOW}Próximos pasos:${NC}"
echo "1. Ejecuta: make run"
echo ""
echo -e "${YELLOW}Comandos útiles:${NC}"
echo "  make help      - Ver todos los comandos"
echo "  make status    - Ver estado del proyecto"
echo "  make stats     - Ver estadísticas"
echo ""
echo -e "${GREEN}¡Listo para usar!${NC}" 