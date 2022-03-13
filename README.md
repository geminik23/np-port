# np-port

numpy object를 원격 전송을 목적으로 구현. 기본 Backend로 RabbitMQ를 사용.

## Requirements.

```
pika
msgpack
msgpack_numpy
```


## Installation

```bash
python3 -m pip install --user --upgrade setuptools wheel

python3 setup.py install
```


## Get started

```python3

from np_port.port import create_port

def initialize():
	URL = ''
	SELF_ID = 'DummyID'
	GROUP_NAME = 'DummyGroup'
    port = create_port(SELF_ID, GROUP_NAME, URL)

```


## TODO

- [] solve RabbitMQ 128MB size limit
