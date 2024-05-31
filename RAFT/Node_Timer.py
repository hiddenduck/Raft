from threading import Timer
from random import uniform # gives a random floating point number N by give a and b where a <= N <= b
from concurrent.futures import ThreadPoolExecutor

class Node_Timer:
    
    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.timer = None
        self.started = False
        self.func = None
        self.args = None

    def create(self, func=None, /, *args):
        self.stop()
        if func != None:
            self.func = func
        else:
            func = self.func
        if args:
            self.args = args
        else:
            args = self.args

        self.timer = Timer(uniform(self.a, self.b), lambda *args: func(*args), args)

    def start(self):
        self.timer.start()
        self.started = True

    def stop(self):
        if self.timer != None and self.started:
            self.started = False
            self.timer.cancel()

    def reset(self):
        self.stop()
        self.create()
        self.start()

    def changeBounds(self, a, b):
        self.a = a
        self.b = b
