import threading


class CountDownLatch(object):
    def __init__(self, count=1):
        self.count = count
        self.lock = threading.Condition()
        self.done = False
        self.t = None

    def count_down(self):
        self.lock.acquire()
        self.count -= 1

        if self.count <= 0:
            self.done = True
            self.lock.notifyAll()

        self.t.cancel()
        self.lock.release()

    def _release(self):
        self.lock.acquire()
        self.count = 0
        self.lock.notifyAll()
        self.lock.release()

    def await(self, timeout=5):
        self.lock.acquire()

        self.t = threading.Timer(timeout, self._release)
        self.t.start()

        while self.count > 0:
            self.lock.wait()

        self.t.cancel()
        self.lock.release()


latch = CountDownLatch()

print("pre")
latch.await(5)
print("after")
