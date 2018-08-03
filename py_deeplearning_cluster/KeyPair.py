
def local_private_key(path):
    with open(path, 'r') as private_key_f:
        private_key_str = private_key_f.read()
    return private_key_str
