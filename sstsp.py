import requests

USER_ID_LEN = 16
DEFAULT_FREQ = '1s'

URL_BASE = "http://localhost:5000"
KEY="b5f3039a4fe94f1cb7344c10e0fffc22"
USER_ID="73cee64511fbc598"

def send_data(datum, timestamp, value):
    params = {'v':str(value), 'key':KEY, 't':str(timestamp)}
    url = URL_BASE + "/d/" + USER_ID + "/" + str(datum)
    r = requests.post(url, data=params)

    if r.status_code != requests.codes.ok:
        pass #FIXME
