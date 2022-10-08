def recordlinkage(f):
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self.settings.model.dedupe:
            kwargs["rl"] = "_link"
            return f(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return wrapper


def recordlinkage_both(f):
    def wrapper(*args, **kwargs):
        self = args[0]
        out1 = f(*args, **kwargs)
        if not self.settings.model.dedupe:
            kwargs["rl"] = "_link"
            out2 = f(*args, **kwargs)
            return out1, out2
        return out1

    return wrapper


def recordlinkage_repeat(f):
    def wrapper(*args, **kwargs):
        self = args[0]
        f(*args, **kwargs)
        if not self.settings.model.dedupe:
            kwargs["rl"] = "_link"
            f(*args, **kwargs)

    return wrapper
