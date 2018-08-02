#!/usr/bin/env python3
"""
Uploads python wheels to archiva

Usage:
    upload-wheels.py artifacts/*.whl
"""

from __future__ import print_function
import hashlib
import os
import subprocess
import sys
import urllib

try:
    # python 2.x
    import urllib2
except ImportError:
    # python 3.x. This isn't the entirety
    # of urllib2, but it's enough for us.
    import urllib.request as urllib2


DRY_RUN = False
REPO = 'https://archiva.wikimedia.org/repository/python/'
GROUP_ID = 'python'


def make_url(artifact, version):
    return '%s%s/%s/%s/%s-%s.whl' % (
        REPO, GROUP_ID.replace('.', '/'), artifact, version, artifact, version)


def url_exists(url):
    request = urllib2.Request(url)
    request.get_method = lambda: 'HEAD'
    try:
        response = urllib2.urlopen(request)
        return response.code == 200
    except urllib2.HTTPError:
        # 404, others?
        return False


def fetch_url(url):
    res = urllib2.urlopen(url)
    return res.read()


def calc_sha1(path):
    sha1 = hashlib.sha1()
    with open(path, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


def mvn_deploy_file(**kwargs):
    cmd = ['mvn', 'deploy:deploy-file'] + ['-D%s=%s' % x for x in kwargs.items()]
    print(cmd)
    if not DRY_RUN:
        subprocess.check_call(cmd)


if __name__ == "__main__":
    args = sys.argv[1:]
    if not len(args) or args[0] in ("-h", "--help"):
        print(__doc__ + "\n")
        sys.exit(1)

    for path in sys.argv[1:]:
        fname = os.path.basename(path)
        artifact, version = os.path.splitext(fname)[0].split('-', 1)
        url = make_url(artifact, version)

        # Git-fat stores the sha1sum of a package. Sadly wheel creation is not
        # bit for bit reproducable, so it's possible the same version is
        # already on archiva but with a different sha1. In that case download
        # the remote version rather than failing to replace the file already
        # there.
        #
        # The wheel file creation itself is deterministic in wheel >= 0.26 when
        # SOURCE_DATE_EPOCH is set, but any C compilation is non-deterministic.
        if url_exists(url):
            # TODO: Archiva only provides sha1 and md5, which are both unsafe.
            # We could always download but that might be a few hundred MB.
            repo_sha1 = fetch_url(url + '.sha1').decode('ascii').split(' ')[0]
            local_sha1 = calc_sha1(path)
            if repo_sha1 == local_sha1:
                print("Already deployed to repo: %s" % (path))
            else:
                print("Remote wheel does not match local: %s (remote) != %s (local)" % (repo_sha1, local_sha1))
                print("Downloading repo wheel from: %s" % (url))
                if not DRY_RUN:
                    urllib.urlretrieve(url, path)
        else:
            mvn_deploy_file(
                repositoryId='wikimedia.python', url=REPO, file=path,
                groupId=GROUP_ID, artifactId=artifact, version=version,
                generatePom=False, packaging="whl")
