from pynput.mouse import Controller, Button
import time
import sys

print("🎯 TEST PYNPUT (Librería del Player)")
print("Voy a mover el mouse a la posición (100, 100) - Arriba a la izquierda.")
print("Luego haré click.")
print("Tienes 3 segundos...")
time.sleep(3)

try:
    mouse = Controller()
    curr = mouse.position
    print(f"Posición actual: {curr}")
    
    # Force Integer
    target = (100, 100)
    print(f"Moviendo a: {target}")
    
    mouse.position = target
    time.sleep(0.5)
    
    new_pos = mouse.position
    print(f"Nueva posición detectada: {new_pos}")
    
    if new_pos == target:
        print("✅ EL MOUSE SE MOVIÓ CORRECTAMENTE.")
    else:
        print("⚠️ EL MOUSE NO LLEGÓ (O macOS bloqueó el evento).")
        
    print("Haciendo Click...")
    mouse.click(Button.left, 1)
    print("Click enviado.")

except Exception as e:
    print(f"❌ Error: {e}")
