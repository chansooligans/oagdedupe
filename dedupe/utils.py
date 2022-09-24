def recordlinkage(f):
    def wrapper(*args, **kwargs):
        self = args[0]
        if self.settings.other.dedupe == False:
            kwargs["rl"] = "_link"
            return f(*args, **kwargs)
        else:
            return f(*args, **kwargs)
    return wrapper

def recordlinkage_repeat(f):
    def wrapper(*args, **kwargs):
        self = args[0]
        f(*args, **kwargs)
        if self.settings.other.dedupe == False:
            kwargs["rl"] = "_link"
            f(*args, **kwargs)
    return wrapper