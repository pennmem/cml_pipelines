import os
from cml_pipelines.cache import memory


def test_cachedir(tmpdir):
    memory.cachedir = tmpdir

    @memory.cache
    def doit(p):
        return p

    doit(1)

    assert os.path.exists(tmpdir.join("test_cache").join("doit"))
