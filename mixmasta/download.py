import requests
import shutil
import zipfile
import os
import sys
from urllib.request import urlretrieve
from tqdm import tqdm

# This is used to show progress when downloading.
# see here: https://github.com/tqdm/tqdm#hooks-and-callbacks
class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""
    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)  # will also set self.n = b * bsize

def download_file(url, fname):
    with requests.get(url, stream=True) as r:
        with open(fname, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
    return fname

def download_progress(url, fname):
    """Download a file and show a progress bar."""
    with TqdmUpTo(unit='B', unit_scale=True, miniters=1,
              desc=url.split('/')[-1]) as t:  # all optional kwargs
        urlretrieve(url, filename=fname, reporthook=t.update_to, data=None)
        t.total = t.n
    return fname

def download_and_clean(version=1, 
                       url="https://jataware-world-modelers.s3.amazonaws.com/mixmasta.zip", 
                       dirname='mixmasta_data'):
    """Download mixmasta and prep the GADM directory.
    This downloads the zip file from the source, extracts it, renames the
    resulting directory, and removes large files not used at runtime.  
    """
    cdir = os.path.expanduser("~")
    print(cdir)
    fname = os.path.join(cdir, 'mixmasta.zip')
    print("Downloading mixmasta v{}...".format(version), file=sys.stderr)
    download_progress(url, fname)
    print("Finished download.")

    outdir = os.path.join(cdir, dirname)
    with zipfile.ZipFile(fname, 'r') as zf:
        zf.extractall(outdir)
    os.rename(f"{outdir}/data", f"{outdir}/gadm")
    os.remove(fname)

    # save a version file so we can tell what it is
    vpath = os.path.join(outdir, 'version')
    with open(vpath, 'w') as vfile:
        vfile.write('mixmasta-{}'.format(version))

    print("Downloaded mixmasta v{} to {}".format(version, outdir), file=sys.stderr)