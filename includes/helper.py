import os


class WorkingPath:
    def __init__(self):
        self.parent = os.path.abspath('../')
        self.external = 'external'
        self.preparation = 'preparation'
        self.proceed = 'proceed'
        self.wrangling = 'wrangling'
    
    def external_path(self):
        return os.path.join(self.parent, self.external)
    
    def preparation_path(self):
        return os.path.join(self.parent, self.preparation)
    
    def wrangling_path(self):
        return os.path.join(self.parent, self.wrangling)
    
    def proceed_path(self):
        return os.path.join(self.parent, self.proceed)

