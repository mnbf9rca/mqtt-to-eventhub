# PURPOSE: To input Smart Meter data into emoncms (data obtained via a Hildebrand Glow Stick https://www.hildebrand.co.uk/our-products/glow-stick-wifi-cad/ )

# With due acknowledgement to ndfred - a contributor to the Glowmarkt forum
# https://gist.github.com/ndfred/b373eeafc4f5b0870c1b8857041289a9

# Developed and tested on a Raspberry Pi running the Oct 2019 emon image updated to ver 10.2.6

# HOW TO ...

# It's simpler to set up the primary INPUT(tag) first before the script is run. The script can then just refer to this INPUT(tag)

# Start by doing the following in an SSH terminal ...

# Install mosquitto-clients with: sudo apt-get install mosquitto-clients

# Then enter: node="Glow Stick"  # The chosen INPUT(tag) name
# Check by doing: echo $node

# Then enter:  curl --data "node=node&apikey=????????????" "http://127.0.0.1/input/post"    # use appropriate apikey

# Ignore the message about the request containing no data
# The INPUT(tag) (Glow Stick) will have been created but it will not be visible on the Inputs webpage until some input data is subsequently posted

# Then copy this script file to  /home/pi  and make it executable with:  chmod +x /home/pi/glow.py    # using the chosen script name

# Run the script with: /usr/bin/python3 /home/pi/glow.py    # using the chosen script name

# All being well, Smart Meter data will appear in emoncms webpage Inputs and be refreshed every 10 secs - Power Now(W) and Daily & CUM Energy(kWh)

# Create FEEDS using Log to Feed and add UNITS (pencil drop-down) to each

# FINALLY ONCE THE SCRIPT RUNS OK: Create the glow.service and enable it so the script runs on boot up as follows:
# Do: CTRL-C to stop the script then - Do: sudo nano /etc/systemd/system/glow.service  and copy & paste in the following (using the chosen script name) ...

"""

[Unit]
Description=Glow Stick service
After=network.target
After=mosquitto.service
StartLimitIntervalSec=0

[Service]
Type=simple
Restart=always
RestartSec=1
User=pi
ExecStart=/usr/bin/python3 /home/pi/glow.py

[Install]
WantedBy=multi-user.target

"""
# Then save & exit and to ensure the glow.service runs on boot up - Do:  sudo systemctl enable glow.service

# AS A VERY LAST CHECK - Do: sudo reboot then SSH in again and check the service is active with:  systemctl status glow.service

# Finally close the SSH terminal. The script/service will continue to run surviving any future reboots

# ===============================================================================

import importlib
import json
import logging
import os
import time
import typing

import paho.mqtt.client as mqtt  # paho-mqtt is already installed in emon
import requests

# reduce the amount of logging from the requests library
logging.getLogger("requests").setLevel(logging.WARNING)

# load dotenv only if it's available, otherwise assume environment variables are set
dotenv_spec = importlib.util.find_spec("dotenv")
if (dotenv_spec is not None):
    print("loading dotenv")
    from dotenv import load_dotenv
    load_dotenv()

# Glow Stick configuration
GLOW_LOGIN = "your user name"
GLOW_PASSWORD = "your password"
GLOW_DEVICE_ID = "BCDDC2C4ABD0"
GLOW_MQTT_HOST = "192.168.17.4"
GLOW_BASE_TOPIC = "glow/" + GLOW_DEVICE_ID + "/#"


# Emoncms server configuration
emoncms_apikey = os.getenv("EMONCMS_APIKEY")
emoncms_server = os.getenv("EMONCMS_SERVER", "127.0.0.1")
server = f"http://{emoncms_server}"
NODE = "Glow"  # Name of the Input(tag) created to receive the INPUT data

# Name each of the data inputs associated with the newly created Input(tag)
di1 = "Power Now"      # ref E_NOW below
di2 = "Daily Energy"   # ref E_DAY below
di3 = "CUM Energy"     # ref E_METER below

# End of User inputs section ===============


def on_connect(client, _userdata, _flags, result_code):
    if result_code != mqtt.MQTT_ERR_SUCCESS:
        logging.error("Error connecting: %d", result_code)
        return
    result_code, _message_id = client.subscribe(GLOW_BASE_TOPIC)

    if result_code != mqtt.MQTT_ERR_SUCCESS:
        logging.error("Couldn't subscribe: %d", result_code)
        return
    logging.info("Connected and subscribed")


def on_message(_client, _userdata, message):
    payload = json.loads(message.payload)
    response_payload = None
    if "electricitymeter" in payload:
        data = payload["electricitymeter"]
        common_data = extract_common_data(data, "elect")
        # check the units match what we're expecting.
        assert (data["energy"]["import"]["units"] in ["kWh", "Wh"])
        assert (data["power"]["units"] in ["kW", "W"])
        electricity_data = {
            "elect_power": convert_to_watts(data["power"]["value"], data["power"]["units"]),
        }


        response_payload = {**common_data , **electricity_data}

    elif "gasmeter" in payload:
        data = payload["gasmeter"]
        # check the units match what we're expecting.
        # we rely on import.units being the same as import.dayweekmonthvolunits
        assert (data["energy"]["import"]["units"] in ["kWh", "Wh"])
        assert (data["energy"]["import"]["cumulativevolunits"] == "m3")

        common_data = extract_common_data(data, "gas")

        dayweekmonthvolunits = data["energy"]["import"]["dayweekmonthvolunits"]
        conveert_dayweekmonthvolunits = (dayweekmonthvolunits in ["kWh", "Wh"])
        
        gas_data = {
            "gas_cumulativevol": data["energy"]["import"]["cumulativevol"],
        #   on my meter, these are always in kWh but i don't know if that's always the case
        #   as it's 'vol', i assume it might be in m3 on some meters
        #   so i'm conditionally converting to watts if the units are in [k]Wh
            "gas_dayvol": convert_to_watts(data["energy"]["import"]["dayvol"],dayweekmonthvolunits) if conveert_dayweekmonthvolunits else data["energy"]["import"]["dayvol"],
            "gas_weekvol": convert_to_watts(data["energy"]["import"]["weekvol"],dayweekmonthvolunits) if conveert_dayweekmonthvolunits else data["energy"]["import"]["weekvol"],
            "gas_monthvol": convert_to_watts(data["energy"]["import"]["monthvol"],dayweekmonthvolunits) if conveert_dayweekmonthvolunits else data["energy"]["import"]["monthvol"],
        }



        response_payload = {**common_data, **gas_data}

    if (response_payload is not None):
        params_to_send = {
            "apikey": emoncms_apikey,
            "fulljson": json.dumps(response_payload),
        }

        # logging.info("Full payload: %s", json.dumps(
          #   response_payload, indent=2))   # Don't need this info printed

        response = requests.get(f"{server}/input/post/{NODE}", params=params_to_send, timeout=4)
        if (response.status_code != 200) or (response.json() is None) or (not response.json()['success']):
            logging.error("Error sending data to emoncms: %s", response.text)
            raise Exception("Error sending data to emoncms")


def extract_common_data(data: typing.Dict, prefix: str) -> typing.Dict:
    units = data['energy']['import']['units']
    return_value = {
        f"{prefix}_unitrate": data['energy']['import']['price']['unitrate'],
        f"{prefix}_standingcharge": data['energy']['import']['price']['standingcharge'],
        f"{prefix}_day": convert_to_watts(data['energy']['import']['day'], units),
        f"{prefix}_week": convert_to_watts(data['energy']['import']['week'], units),
        f"{prefix}_month": convert_to_watts(data['energy']['import']['month'], units),
        f"{prefix}_cumulative": convert_to_watts(data['energy']['import']['cumulative'], units),
        f"{prefix}_time": round(time.mktime(time.strptime(data["timestamp"], '%Y-%m-%dT%H:%M:%SZ'))),
    }
    return return_value

def convert_to_watts(power: float, units: str) -> int:

    if units in ["kW", "kWh"]:
        return round(power * 1000)
    elif units in ["W", "Wh"]:
        return round(power)
    else:
        raise ValueError(f"Unknown units: {units}")


def loop():
    logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    client = mqtt.Client()
    # client.username_pw_set(GLOW_LOGIN, GLOW_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(GLOW_MQTT_HOST)
    client.loop_forever()

if __name__ == "__main__":
    loop()
