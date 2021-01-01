import threading


class Timer(threading.Timer):

    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
