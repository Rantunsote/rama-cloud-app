import json
import time
from pynput import mouse, keyboard

events = []
start_time = None

print("🔴 RECORDER STARTED")
print("Press ESC to stop recording.")
print("Recording mouse clicks and keystrokes...")

def on_click(x, y, button, pressed):
    if pressed:
        global start_time
        if start_time is None: start_time = time.time()
        
        elapsed = time.time() - start_time
        events.append({
            "type": "click",
            "x": x,
            "y": y,
            "button": str(button),
            "time": elapsed
        })
        print(f"Click at ({x}, {y})")

def on_press(key):
    global start_time
    if start_time is None: start_time = time.time()
    
    try:
        k = key.char
    except AttributeError:
        k = str(key)
        
    elapsed = time.time() - start_time
    events.append({
        "type": "press",
        "key": k,
        "time": elapsed
    })
    
    if key == keyboard.Key.esc:
        print("\n⏹ STOPPING RECORDING...")
        return False

def on_scroll(x, y, dx, dy):
    global start_time
    if start_time is None: start_time = time.time()
    
    elapsed = time.time() - start_time
    events.append({
        "type": "scroll",
        "dx": dx,
        "dy": dy,
        "x": x,
        "y": y,
        "time": elapsed
    })
    print(f"Scroll {dy} at ({x}, {y})")

# Setup listeners
mouse_listener = mouse.Listener(on_click=on_click, on_scroll=on_scroll)
keyboard_listener = keyboard.Listener(on_press=on_press)

mouse_listener.start()
try:
    with keyboard_listener:
        keyboard_listener.join()
except KeyboardInterrupt:
    print("\n⚠️ Interrupted by User (Ctrl+C). Saving what we have...")

# Save
with open("macro.json", "w") as f:
    json.dump(events, f, indent=2)

print(f"✅ Saved {len(events)} events to macro.json")
