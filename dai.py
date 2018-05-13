#!/usr/bin/env python3
import atexit
import importlib
import re
import signal
import sys
import time

from threading import Thread

from dan import (ApplicationNotFoundError, DeviceFeature,
                 RegistrationError, NoData, log, loop_forever,
                 register, push, deregister)

_flags = {}
_devices = {}
_interval = {}


def push_data(df_name):
    if not _devices[df_name].push_data:
        return
    while _flags[df_name]:
        _data = _devices[df_name].push_data()
        if not isinstance(_data, NoData) and _data is not NoData:
            push(df_name, _data)
        time.sleep(_interval[df_name])


def on_signal(signal, df_list):
    global _flags
    log.info('Receive signal: %s, %s', signal, df_list)
    if 'CONNECT' == signal:
        for df_name in df_list:
            _flags[df_name] = True
            t = Thread(target=push_data, args=(df_name, ))
            t.daemon = True
            t.start()
    elif 'DISCONNECT' == signal:
        for df_name in df_list:
            _flags[df_name] = False
    elif 'SUSPEND' == signal:
        # Not use
        pass
    elif 'RESUME' == signal:
        # Not use
        pass
    return True


def get_df_function_name(df_name):
    return re.sub(r'-O$', '_O', re.sub(r'-I$', '_I', df_name))


def on_data(df_name, data):
    _devices[df_name].on_data(data)
    return True


def main(app):
    global _devices, _interval
    host = app.__dict__.get('host')
    if host is None:
        raise RegistrationError('host not given.')

    port = app.__dict__.get('port')
    if port is None:
        port = 9992

    device_name = app.__dict__.get('device_name')
    if device_name is None:
        pass
        # raise RegistrationError('device_name not given.')

    device_model = app.__dict__.get('device_model')
    if device_model is None:
        raise RegistrationError('device_model not given.')

    from uuid import UUID
    device_addr = app.__dict__.get('device_addr')
    if device_addr:
        try:
            UUID(device_addr)
        except ValueError:
            try:
                device_addr = str(UUID(int=int(device_addr, 16)))
            except ValueError:
                print('Invalid device_addr. Change device_addr to None.')
                device_addr = None

    register_callback = app.__dict__.get('register_callback')

    idfs = app.__dict__.get('idf_list', [])
    odfs = app.__dict__.get('odf_list', [])

    username = app.__dict__.get('username')

    _push_interval = app.__dict__.get('push_interval', 1)
    _interval = app.__dict__.get('interval', {})

    if not idfs and not odfs:
        raise RegistrationError('Neither idf_list nor odf_list is empty.')

    idf_list = []
    for df_profile in idfs:
        if isinstance(df_profile, str):
            _devices[df_profile] = DeviceFeature(df_name=df_profile)
            _devices[df_profile].push_data = app.__dict__.get(get_df_function_name(df_profile))
            idf_list.append(_devices[df_profile].profile())

            # check push data   interval
            if not _interval.get(df_profile):
                _interval[df_profile] = _push_interval
        elif isinstance(df_profile, tuple) and len(df_profile) == 2:
            _devices[df_profile[0]] = DeviceFeature(df_name=df_profile[0],
                                                    df_type=df_profile[1])
            _devices[df_profile[0]].push_data = app.__dict__.get(get_df_function_name(df_profile[0]))
            idf_list.append(_devices[df_profile[0]].profile())

            # check push data interval
            if not _interval.get(df_profile[0]):
                _interval[df_profile[0]] = _push_interval
        else:
            raise RegistrationError('unknown idf_list, usage: [df_name, ...]')

    odf_list = []
    for df_profile in odfs:
        if isinstance(df_profile, str):
            _devices[df_profile] = DeviceFeature(df_name=df_profile)
            _devices[df_profile].on_data = app.__dict__.get(get_df_function_name(df_profile))
            odf_list.append(_devices[df_profile].profile())
        elif isinstance(df_profile, tuple) and len(df_profile) == 2:
            _devices[df_profile[0]] = DeviceFeature(df_name=df_profile[0],
                                                    df_type=df_profile[1])
            _devices[df_profile[0]].on_data = app.__dict__.get(get_df_function_name(df_profile[0]))
            odf_list.append(_devices[df_profile[0]].profile())
        else:
            raise RegistrationError('unknown odf_list, usage: [df_name, ...]')

    context = register(
        'http://{}:{}'.format(host, port),
        on_signal=on_signal,
        on_data=on_data,
        accept_protos=['mqtt'],
        id_=device_addr,
        idf_list=idf_list,
        odf_list=odf_list,
        name=device_name,
        profile={
            'model': device_model,
            'u_name': username,
        },
        register_callback=register_callback,
    )

    atexit.register(deregister)

    #signal.pause()
    while 1:
        time.sleep(0.2)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        ida_filename = 'ida'
    elif len(sys.argv) == 2:
        ida_filename = sys.argv[1]
    app = importlib.import_module(ida_filename)
    main(app)
