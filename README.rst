Dummy Device for  IoTtalk v2
===============================================================================

`Python Virtual Environment <https://docs.python.org/3/tutorial/venv.html>`_ (recommend)
----------------------------------------------------------------------

::

    python3 -m venv /path/to/venv/dir       # create venv
    source /path/to/venv/dir/bin/activate   # activate venv


Install dependencies
----------------------------------------------------------------------

::

    pip install iottalk-py==2.3.1


Config IoTtalk Server URL
----------------------------------------------------------------------

Please set the variable ``api_url`` in ``sa.py``.


Start
----------------------------------------------------------------------

::


    python -m iottalkpy.dai sa.py
