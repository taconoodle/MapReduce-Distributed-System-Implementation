import cloudpickle

def map_fn(data):
    for k, v in data.items():
        yield k, 1

def reduce_fn(key, values):
    return key, sum(values)


with open('../../map_fn.pkl', 'wb') as f:
    cloudpickle.dump(map_fn, f)

with open('../../reduce_fn.pkl', 'wb') as f:
    cloudpickle.dump(reduce_fn, f)