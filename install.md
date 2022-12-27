# how to run the script

1. install build dependencies
   - see [Cryptography](https://cryptography.io/en/latest/installation/) for build requirements. 
   `sudo apt-get install-y cmake build-essential libssl-dev libffi-dev \
    python3-dev cargo pkg-config gcc musl-dev git-all`
2. Go to your home folder: `cd ~`
4. clone the folder:
`git clone https://github.com/mnbf9rca/mqtt-to-eventhub.git`
1. change to the follder: `cd mqqt-to-enventhub`
2. copy `.env.example` to `.env` populate it with your values
3. create a venv: `python3.9 -m venv .venv`
4. activate: `source ./.venv/bin/activate`
6. This app uses Poetry as the package manager. However Poetry [doesnt support alternative sources very well](https://github.com/python-poetry/poetry/issues/4854). That means that it ignores https://www.piwheels.org/ and tries to build `uamqp` from scratch - which means building `scipy` from scratch, which takes literally hours. Instead, the package dependencies are exported to `requirements.txt`. It'll still take a while (for some reason it still builds Cryptography) but not nearly as long.

## to use requirements.txt (e.g. on a raspberry pi)

5. install setuptools and wheel:
`pip install wheel setuptools`
1. use pip to install:
`pip install -r requirements.txt`

## to use poetry (e.g. on a VM or something more powerful)

6. Install Poetry using [a script from the Poetry website](https://python-poetry.org/docs/):
`curl -sSL https://install.python-poetry.org | python3 -`
1. Install requirements:
`poetry --no-dev --no-root -v install`

## finally...
1. Run:
`python mqtt-to-eventhub.py`


# how to install mqtt-to-eventhub as a script

FINALLY ONCE THE SCRIPT RUNS OK: Create the glow.service and enable it so the script runs on boot up as follows:
Do: CTRL-C to stop the script then - Do: sudo nano /etc/systemd/system/mqtttoeventhub.service  and copy & paste in the following (using the chosen script name) ...

The script below assumes the user is `pi`. If not, replace `pi` with the username you're using.

```bash
[Unit]
Description=MQTT to EventHub
After=network.target
After=mosquitto.service
StartLimitIntervalSec=0

[Service]
Environment=DOTENV_KEY=""
Type=simple
Restart=always
RestartSec=1
User=pi
ExecStart=/home/pi/mqtt-to-eventhub/.venv/bin/python /home/pi/mqtt-to-eventhub.py

[Install]
WantedBy=multi-user.target
```

Then save & exit and to ensure the glow.service runs on boot up - Do:  sudo systemctl enable glow.service

AS A VERY LAST CHECK - Do: sudo reboot then SSH in again and check the service is active with:  systemctl status glow.service

Finally close the SSH terminal. The script/service will continue to run surviving any future reboots
