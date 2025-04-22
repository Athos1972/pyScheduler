import pystray
from PIL import Image
import sys
import os

def on_exit(icon, item):
    icon.stop()
    os._exit(0)

def setup_tray_icon():
    image = Image.new('RGB', (64, 64), 'black')
    icon = pystray.Icon(
        "Job Scheduler",
        image,
        menu=pystray.Menu(
            pystray.MenuItem("Beenden", on_exit)
    ))
    icon.run()