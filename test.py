import time
import threading
from np_port.port import create_port, Port
import os
import numpy as np


def dummy_server():
    conn_url = os.getenv('RABBITMQ_URL')
    print(conn_url)
    name = "DummyServer"
    port = create_port(name, 'server', conn_url)
    time.sleep(1.5)


    ## PUB 1 to group 'client'
    port.publish('client', 'PUB_TEST', None)
    ## PUB 2 to All
    port.publish(None, 'PUB_TEST_TO_ALL', {'to':'All'})

    ## Rep 1
    msg = port.receive() #1
    print('SERVER data received : ', msg)
    time.sleep(0.1)
    ## Rep 2
    msg = port.receive() #2
    print('SERVER data received : ', msg)

    time.sleep(0.1)
    ## SUB PUB 2 Message
    msg = port.receive() #2
    print('SERVER data received : ', msg)
    
def dummy_worker():
    conn_url = os.getenv('RABBITMQ_URL')
    name = "DummyWorker"
    port = create_port(name, 'client', conn_url)
    time.sleep(1)

    ## Req 1
    port.request('DummyServer', 'TEST', {'hello':'df'})
    ## Req 2
    port.request('DummyServer', 'TEST2', np.array([1,2,3]))

    time.sleep(1)

    ## Sub from group 'client'
    msg = port.receive() #2
    print('WORKER data received : ', msg)
    msg = port.receive() #2
    print('WORKER data received : ', msg)


if __name__ == '__main__':
    tc = threading.Thread(target=dummy_server, daemon=True)
    tc.start()
    tw = threading.Thread(target=dummy_worker, daemon=True)
    tw.start()

    tc.join()
    tw.join()


    pass
