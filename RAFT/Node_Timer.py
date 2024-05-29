from threading import Timer
from random import uniform # gives a random floating point number N by give a and b where a <= N <= b
from concurrent.futures import ThreadPoolExecutor

class Node_Timer:
    
    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.timer = None

    def create(self, func, /, *args, **kwargs):
        self.stop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            self.timer = Timer(uniform(self.a, self.b), lambda: executor.submit(func, args, kwargs))

    def start(self):
        self.timer.start()

    def stop(self):
        if self.timer != None:
            self.timer.cancel()

    def reset(self):
        self.stop()
        self.create()
        self.start()

    def changeBounds(self, a, b):
        self.a = a
        self.b = b
