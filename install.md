# how to run the script

1. create a venv: `python -m venv .venv`


# how to install mqtt-to-timescale as a script

FINALLY ONCE THE SCRIPT RUNS OK: Create the glow.service and enable it so the script runs on boot up as follows:
Do: CTRL-C to stop the script then - Do: sudo nano /etc/systemd/system/glow.service  and copy & paste in the following (using the chosen script name) ...


```bash
[Unit]
Description=Glow Stick service
After=network.target
After=mosquitto.service
StartLimitIntervalSec=0

[Service]
Environment=EMONCMS_APIKEY="<APIKEYHERE>"
Environment=EMONCMS_SERVER="emoncms.cynexia.net"
Type=simple
Restart=always
RestartSec=1
User=pi
ExecStart=/usr/bin/python3 /home/pi/glow.py

[Install]
WantedBy=multi-user.target
```

Then save & exit and to ensure the glow.service runs on boot up - Do:  sudo systemctl enable glow.service

AS A VERY LAST CHECK - Do: sudo reboot then SSH in again and check the service is active with:  systemctl status glow.service

Finally close the SSH terminal. The script/service will continue to run surviving any future reboots
