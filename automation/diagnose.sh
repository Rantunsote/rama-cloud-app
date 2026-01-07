#!/bin/bash
# Navegar al directorio donde esta este script
cd "$(dirname "$0")"
# Ejecutar diagnose.py usando el Python del entorno virtual
../venv/bin/python3 diagnose.py
