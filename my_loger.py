from datetime import datetime

class My_loger():
    def __init__(self, logfile = False):
        self.logfile = logfile
        
    def log(self, mesage):
        if not self.logfile:
            return
        with open(self.logfile, 'at') as f:
            f.write(f'{datetime.now():%H:%M:%S} {mesage}\n')