# Dummy_Device_IoTtalk_v2

## [Python venv](https://docs.python.org/3/tutorial/venv.html) (recommend) 
```
python3 -m venv /path/to/venv/dir       # create venv
source /path/to/venv/dir/bin/activate   # start venv
```

## Python module list
- paho-mqtt
- requests
```
pip install paho-mqtt requests
```



## Change IoTtalk Server IP
```
$ vim dai.py

line 4: host = 'IP'
```

## Start
```
python3 dai.py
```