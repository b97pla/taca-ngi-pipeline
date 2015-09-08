__author__ = 'Pontus'

from glob import iglob
from logging import getLogger
from os import path, walk
from taca.utils.misc import hashfile

logger = getLogger(__name__)


class FileNotFoundException(Exception):
    pass


class PatternNotMatchedException(Exception):
    pass


def gather_files(patterns, no_checksum=False, hash_algorithm="md5"):
    """ This method will locate files matching the patterns specified in
        the config and compute the checksum and construct the staging path
        according to the config.

        The config should contain the key 'files_to_deliver', which should
        be a list of tuples with source path patterns and destination path
        patterns. The source path can be a file glob and can refer to a
        folder or file. File globs will be expanded and folders will be
        traversed to include everything beneath.

        :returns: A generator of tuples with source path,
            destination path and the checksum of the source file
            (or None if source is a folder)
    """
    def _get_digest(sourcepath, destpath, no_digest_cache=False, no_digest=False):
        digest = None
        # skip the digest if either the global or the per-file setting is to skip
        if not any([no_checksum, no_digest]):
            checksumpath = "{}.{}".format(sourcepath, hash_algorithm)
            try:
                with open(checksumpath, 'r') as fh:
                    digest = fh.next()
            except IOError:
                digest = hashfile(sourcepath, hasher=hash_algorithm)
                if not no_digest_cache:
                    try:
                        with open(checksumpath, 'w') as fh:
                            fh.write(digest)
                    except IOError as we:
                        logger.warning("could not write checksum {} to file {}: {}".format(digest, checksumpath, we))
        return sourcepath, destpath, digest

    def _walk_files(currpath, destpath):
        # if current path is a folder, return all files below it
        if path.isdir(currpath):
            parent = path.dirname(currpath)
            for parentdir, _, dirfiles in walk(currpath, followlinks=True):
                for currfile in dirfiles:
                    fullpath = path.join(parentdir, currfile)
                    # the relative path will be used in the destination path
                    relpath = path.relpath(fullpath, parent)
                    yield (fullpath, path.join(destpath, relpath))
        else:
            yield (currpath,
                   path.join(
                       destpath,
                       path.basename(currpath)))

    if patterns is None:
        patterns = []
    for pattern in patterns:
        sfile, dfile = pattern[0:2]
        try:
            extra = pattern[2]
        except IndexError:
            extra = {}
        matches = 0
        for f in iglob(sfile):
            for spath, dpath in _walk_files(f, dfile):
                # ignore checksum files
                if not spath.endswith(".{}".format(hash_algorithm)):
                    matches += 1
                    # skip and warn if a path does not exist, this includes broken symlinks
                    if path.exists(spath):
                        yield _get_digest(
                            spath,
                            dpath,
                            no_digest_cache=extra.get('no_digest_cache', False),
                            no_digest=extra.get('no_digest', False))
                    else:
                        # if the file pattern requires a match, throw an error. otherwise warn
                        msg = "path {} does not exist, possibly because of a broken symlink".format(spath)
                        if extra.get('required', False):
                            logger.error(msg)
                            raise FileNotFoundException(msg)
                        logger.warning(msg)
        if matches == 0:
            msg = "no files matching search expression '{}' found ".format(sfile)
            if extra.get('required', False):
                logger.error(msg)
                raise PatternNotMatchedException(msg)
            logger.warning(msg)

