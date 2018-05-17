import os


class FilePaths(object):
    """ Paths to files that frequently get passed around to many tasks.

        All paths given relative to the root path but are converted to absolute
        paths on creation.

    Parameters
    ----------
    root: str
        Root directory, usually the mount point for RHINO

    Notes
    -----
    All keyword arguments are converted into absolute paths relative to the
    given root directory

    """

    def __init__(self, root, **kwargs):
        self.root = os.path.expanduser(root)

        self._keys = []
        for key, val in kwargs.items():
            stripped_val = val.lstrip('/').rstrip('/')
            abs_path = os.path.join(self.root, stripped_val)
            setattr(self, key, abs_path)
            self._keys.append(key)

    def keys(self):
        """ List of file path keys that have been defined """
        return self._keys

