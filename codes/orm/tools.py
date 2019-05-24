import json



class _AttrDict(dict):

    def __init__(self, dictionary):
        super(_AttrDict, self).__init__(**dictionary)
        self.__dict__ = self



class AttrDict(dict):

    def __init__(self, dictionary):
        super(AttrDict, self).__init__(**dictionary)
        self.__dict__ = self._parse(dictionary)


    @classmethod
    def _parse(cls, dictionary):
        for k, v in dictionary.items():
            if isinstance(v, dict):
                dictionary[k] = cls._parse(v)

        return _AttrDict(dictionary)


    def dump(self, fn = 'attr_dict.json'):
        with open(fn, 'wt') as f:
            json.dump(self, f)


    @classmethod
    def load(cls, fn = 'attr_dict.json'):
        with open(fn, 'rt') as f:
            return cls(json.load(f))
