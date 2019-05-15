class AttrDict(dict):

    def __init__(self, dictionary):
        super(AttrDict, self).__init__(**dictionary)
        self.__dict__ = self

        self.keys_values = self
        self.values_keys = {v: k for k, v in self.keys_values.items()}

    def to_dict(self):
        return self
