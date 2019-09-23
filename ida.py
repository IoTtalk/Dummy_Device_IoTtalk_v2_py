import random

from iottalkpy.dan import NoData

### The registeration api url, you can use IP or Domain.
api_url = 'http://localhost:9992'  # default
# api_url = 'http://localhost/csm'  # with URL prefix
# api_url = 'http://localhost:9992/csm'  # with URL prefix + port

### [OPTIONAL] If not given or None, server will auto-generate.
# device_name = 'Dummy_Test'

### [OPTIONAL] If not given or None, DAN will register using a random UUID.
### Or you can use following code to use MAC address for device_addr.
# from uuid import getnode
# device_addr = "{:012X}".format(getnode())
# device_addr = "..."

### [OPTIONAL] If the device_addr is set as a fixed value, user can enable
### this option and make the DA register/deregister without rebinding on GUI
# persistent_binding = True

### [OPTIONAL] If not given or None, this device will be used by anyone.
# username = 'myname'

### The Device Model in IoTtalk, please check IoTtalk document.
device_model = 'Dummy_Device'

### The input/output device features, please check IoTtalk document.
idf_list = ['Dummy_Sensor']
odf_list = ['Dummy_Control']

### Set the push interval, default = 1 (sec)
### Or you can set to 0, and control in your feature input function.
push_interval = 10  # global interval
interval = {
    'Dummy_Sensor': 3,  # assign feature interval
}


def register_callback():
    print('register successfully')


def Dummy_Sensor():
    return random.randint(0, 100)

    # Or you want to return nothing.
    # Note that the object `None` is treated as normal data in IoTtalk
    #
    # return NoData


def Dummy_Control(data):  # data is a list
    print(str(data[0]))
