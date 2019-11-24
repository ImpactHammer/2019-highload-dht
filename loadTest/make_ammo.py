import random
import numpy as np

ENCODING = 'ascii'
SEED = 0
VALUE_LEN = 256

import os
def kv_generator(n, value_len, population_size=None, offset=0):
    n = int(n)
    value_len = int(value_len)
    if not population_size:
        population_size = n
    
    random.seed(SEED)
    samples = random.sample(range(offset, offset + int(population_size)), int(n))
    for i in samples:
        yield i, os.urandom(value_len)

def make_put(key, val):
    request = 'PUT /v0/entity?id={} HTTP/1.1\r\n'.format(key)
    request += 'Content-Length: {}\r\n'.format(str(len(val)))
    request += '\r\n'
    request = bytes(request, ENCODING)
    
    request += val
    request += bytes('\r\n', ENCODING)
    request = bytes(str(len(request)), ENCODING) + bytes(' put\n', ENCODING) + request
    
    return request

def make_get(key):
    request = 'GET /v0/entity?id={} HTTP/1.1\r\n'.format(key)
    request += '\r\n'
    request = bytes(request, ENCODING)

    request += bytes('\r\n', ENCODING)
    request = bytes(str(len(request)), ENCODING) + bytes(' get\n', ENCODING) + request
    
    return request

def put_shots(n_shots, value_len, fname):
    gen = kv_generator(n_shots, value_len)

    h = open(fname, 'wb')
    for k, v in gen:
        h.write(make_put(str(k), v))
    h.close()
    
def get_shots(n_shots, population_size, fname):
    n_shots = int(n_shots)
    population_size = int(population_size)
    
    h = open(fname, 'wb')
    for i_shot in range(n_shots):
        k = random.randint(0, population_size - 1)
        h.write(make_get(str(k)))
    h.close()

def mixed_shots(n_shots, value_len, current_db_size, fname):
    n_puts = n_shots / 2
    gen_put = kv_generator(n_puts, value_len, offset=current_db_size)
    h = open(fname, 'wb')
    for i_shot in range(n_shots):
        is_get = bool(random.randint(0, 1))
        if is_get:
            k = random.randint(0, current_db_size - 1)
            h.write(make_get(str(k)))
        else:
            try:
                k, v = next(gen_put)
                h.write(make_put(str(k), v))
            except:
                k = random.randint(0, current_db_size - 1)
                h.write(make_get(str(k)))
    h.close()
