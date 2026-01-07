import pyautogui
import time
import sys

print("🩺 DIAGNOSTICO DE MOUSE")
print("-----------------------")
print("Voy a intentar mover el mouse en un cuadrado.")
print("Si el mouse NO se mueve, es un problema de PERMISOS de macOS.")
print("Tienes 5 segundos para soltar el mouse...")

for i in range(5, 0, -1):
    print(f"{i}...", end=" ", flush=True)
    time.sleep(1)
print("\n🚀 MOVIENDO...")

try:
    # Get current size
    w, h = pyautogui.size()
    print(f"Pantalla detectada: {w}x{h}")

    # Move to center
    cx, cy = w // 2, h // 2
    pyautogui.moveTo(cx, cy, duration=0.5)
    print(f"-> Centro ({cx}, {cy})")
    
    # Move square relative
    pyautogui.moveRel(100, 0, duration=0.5)
    print("-> Derecha")
    pyautogui.moveRel(0, 100, duration=0.5)
    print("-> Abajo")
    pyautogui.moveRel(-100, 0, duration=0.5)
    print("-> Izquierda")
    pyautogui.moveRel(0, -100, duration=0.5)
    print("-> Arriba")
    
    print("\n✅ PRUEBA FINALIZADA.")
    print("¿Se movió el cursor? (Si/No)")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("Esto suele pasar si la Terminal no tiene permisos.")
