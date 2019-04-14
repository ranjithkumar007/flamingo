from multiprocessing import Process
from multiprocessing.managers import SyncManager
from queue import PriorityQueue

class MyManager(SyncManager):
    pass
MyManager.register("PriorityQueue", PriorityQueue)  # Register a shared PriorityQueue

def Manager():
    m = MyManager()
    m.start()
    return m
