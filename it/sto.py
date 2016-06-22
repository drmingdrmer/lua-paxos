import shutil
import os

sto_base = "/tmp/paxos_test"

def init_sto():

    try:
        shutil.rmtree(sto_base)
    except Exception as e:
        print repr(e)

    os.mkdir(sto_base)
