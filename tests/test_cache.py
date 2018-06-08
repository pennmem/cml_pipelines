import os
from cml_pipelines.cache import memory


def test_cachedir(tmpdir):
    memory.cachedir = str(tmpdir)

    @memory.cache
    def doit(p):
        return p

    doit(1)
    assert os.path.exists(str(tmpdir.join("test_cache").join("doit")))

    # change to a new location
    memory.cachedir = str(tmpdir.join("other"))

    @memory.cache
    def doit2(p):
        return p

    doit2(1)
    newpath = str(tmpdir.join("other").join("test_cache").join("doit2"))
    assert os.path.exists(newpath)
