import os
import pytest
from cml_pipelines.paths import FilePaths


class TestFilePaths:
    @classmethod
    def setup_class(cls):
        cls.test_paths = FilePaths("/mnt/mount_point/", dir1="/base/",
                                   dir2="dir2/")

    @pytest.mark.parametrize("basedir", ["basedir", "/basedir", "/basedir/"])
    @pytest.mark.parametrize("root", ["/", "~", "mount_point/", "/mount_point/"])
    def test_initialize(self, root, basedir):
        paths = FilePaths(root, base=basedir)

        basedir = basedir.lstrip("/").rstrip("/")

        assert paths.root == os.path.expanduser(root)
        assert paths.base == os.path.join(os.path.expanduser(root), basedir)
        return

    def test_keys(self):
        keys = self.test_paths.keys()
        assert len(keys) == 2
        assert "dir1" in keys
        assert "dir2" in keys
