import time
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

def worker(queue):
    print(queue)
    for i in range(100):
        queue.put(i)
    print("worker", queue.qsize())


m = Manager()
pr_queue = m.PriorityQueue()  # This is process-safe
worker_process = Process(target = worker, args = (pr_queue,))
worker_process.start()

time.sleep(5)    # nope, race condition, you shall not pass (probably)
print("main", pr_queue.qsize())
