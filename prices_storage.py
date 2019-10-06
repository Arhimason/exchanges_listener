class PricesStorage(dict):
    def __setitem__(self, key, value):
        if value == 0:
            if key in self:
                self.pop(key)
        else:
            super(PricesStorage, self).__setitem__(key, value)
