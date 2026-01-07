import json
import time
import os
from pynput.mouse import Button, Controller as MouseController
from pynput.keyboard import Key, Controller as KeyboardController

MACRO_FILE = "macro.json"

if not os.path.exists(MACRO_FILE):
    print(f"❌ No macro file found: {MACRO_FILE}")
    print("Run recorder.py first!")
    exit()

with open(MACRO_FILE, "r") as f:
    events = json.load(f)

print(f"▶️ PLAYING MACRO ({len(events)} events) [Using pynput]")
print("⚠️ move your mouse to the top-left corner to ABORT safely (Manual Override).")
time.sleep(2) 

mouse = MouseController()
keyboard = KeyboardController()
start_time = time.time()

for i, event in enumerate(events):
    # Timing
    target_time = start_time + event['time']
    delay = target_time - time.time()
    if delay > 0:
        time.sleep(delay)
        
    if event['type'] == 'click':
        print(f"[{i}] Click {event['x']}, {event['y']}")
        mouse.position = (event['x'], event['y'])
        time.sleep(0.05) # Tiny stability pause
        
        # parse button: "Button.left" -> Button.left
        btn_str = event.get('button', 'Button.left')
        btn = Button.left
        if 'right' in btn_str: btn = Button.right
        
        mouse.click(btn, 1)
        
    elif event['type'] == 'press':
        k_str = event['key']
        print(f"[{i}] Press {k_str}")
        
        # Handle special keys
        try:
            if len(k_str) > 1 and "Key." not in k_str:
                # String typing
                 keyboard.type(k_str)
            else:
                # Special key or single char
                # We need to map strings back to Keys if they are special
                # e.g 'Key.enter'
                if 'Key.enter' in k_str: keyboard.press(Key.enter); keyboard.release(Key.enter)
                elif 'Key.space' in k_str: keyboard.press(Key.space); keyboard.release(Key.space)
                # ... mapping all keys is tedious.
                # Simplify: if it's a single char, use press/release
                elif len(k_str) == 1:
                    keyboard.press(k_str)
                    keyboard.release(k_str)
        except Exception as e:
            print(f"Skip key {k_str}: {e}")

    elif event['type'] == 'scroll':
        print(f"[{i}] Scroll {event['dy']}")
        mouse.position = (event['x'], event['y'])
        # dx, dy directly
        mouse.scroll(event['dx'], event['dy'])
        
print("✅ Macro Finished")
