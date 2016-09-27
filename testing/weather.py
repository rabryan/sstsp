import pyowm
import time
import logging as log
import sstsp

owm = pyowm.OWM('95af8f5a066c0337adf73aabf722b5fc')
TEMP_UNITS = 'fahrenheit'
def kelvin_to_f(temp_k):
    return temp_k*9.0/5.0 - 459.67

def update():
    obs = owm.weather_at_place("atlanta, ga")
    if obs is None:
        log.warn("weather update unavailable")
        return

    w = obs.get_weather()

    if w is None:
        log.warn("weather update unavailable")
        return
    
    temp_f = w.get_temperature(TEMP_UNITS)['temp']
    log.info("temperature is {} F".format(temp_f))
    sstsp.send_data("atlanta_temperature", time.time(), temp_f)
    
    wind_speed = w.get_wind()['speed']

    log.info("wind speed is {}".format(wind_speed))
    sstsp.send_data("atlanta_wind_speed", time.time(), wind_speed)

if __name__ == '__main__':
    log.basicConfig(level=log.DEBUG)
    UPDATE_INT = 15
    while True:
        update()
        time.sleep(UPDATE_INT)

