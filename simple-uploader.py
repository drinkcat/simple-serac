#!/bin/python

import dataclasses
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

def humanSize(size):
    suffix = ['', 'k', 'M', 'G']
    i = 0
    while size >= 1024 and i < len(suffix)-1:
        size = size // 1024
        i += 1
    return f"{size}{suffix[i]}"

class DatabaseEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__    

DBVERSION=1

@dataclasses.dataclass
class FileEntry:
    name: str
    size: int
    modified: str
    sha: str = ""

    def sha256sum(file):
        with open(file, 'rb') as f:
            return hashlib.file_digest(f, 'sha256').hexdigest()

    def gen(indir, file):
        absfile = os.path.join(indir, file)

        stat = os.stat(absfile, follow_symlinks=False)
        size = stat.st_size
        modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
        if os.path.islink(absfile):
            sha = hashlib.sha256(os.readlink(absfile))
        elif os.path.isfile(absfile):
            sha = FileEntry.sha256sum(absfile)
        else:
            raise SystemError(f"Found a file {file} that's not a file or a link.")

        return FileEntry(file, size, modified, sha)

@dataclasses.dataclass
class Database:
    data: list[FileEntry] = dataclasses.field(default_factory=list)
    count: int = 0
    version: int = DBVERSION

    def add(self, file: FileEntry):
        self.data.append(file)
        self.count += 1

def createTar(tardb, size):
    global tarfileindex
    print(f"Creating tar with {tardb.count} files {size}.")

    basefilepath = f"now-{tarfileindex}"
    tarfileindex = tarfileindex + 1

    # TODO: Actually tar the files

    with open(basefilepath + ".txt", "w") as dbfile:
        json.dump(tardb, dbfile, cls=DatabaseEncoder, indent=4)

# 128 MB chunks is a good sweet spot pricing-wise
# Note that chunks can be larger as we only fit in full files.
MINSIZE=128*1024*1024

# TODO: Better location for database cache
db = "/home/drinkcat/.cache/s3-uploader/one/"
tarfileindex = 0

if len(sys.argv) != 2:
    print("Usage: python s3-uploader.py indir")
    exit()

indir = os.path.abspath(sys.argv[1])

if not os.path.isdir(db):
    os.makedirs(db)

# List all files
inlist = []
for folder, subs, files in os.walk(indir, followlinks=False):
    # Trim prefix (this could be done with os.path functions?)
    if not folder.startswith(indir):
        raise SystemError(f"Weird folder {folder} does not start with {indir}")
    folder = folder[len(indir)+1:]

    inlist += map(lambda f: os.path.join(folder, f), files)

# Sort for consistency
inlist.sort()

# Database for the current tar
tardb = Database()
size = 0

for file in inlist:
    fileentry = FileEntry.gen(indir, file)

    # TODO: Find in database and skip if needed

    size += fileentry.size
    tardb.add(fileentry)
    if size > MINSIZE:
        createTar(tardb, humanSize(size))
        tardb = Database()
        size = 0
