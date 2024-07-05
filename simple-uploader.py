#!/bin/python

import dataclasses
import hashlib
import json
import os
import sys
import tarfile
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
        if isinstance(o, FileEntry):
            dict = o.__dict__
            del(dict["db"])
            return dict
        elif isinstance(o, Database):
            return o.__dict__

DBVERSION=1

@dataclasses.dataclass
class FileEntry:
    name: str
    size: int
    modified: str
    sha: str
    db: str = ""

    def sha256sum(file):
        if os.path.islink(file):
            return hashlib.sha256(os.readlink(file).encode("utf-8")).hexdigest()
        elif os.path.isfile(file):
            with open(file, 'rb') as f:
                return hashlib.file_digest(f, 'sha256').hexdigest()
        else:
            raise SystemError(f"Found a file {file} that's not a file or a link.")

    def gen(indir, file):
        absfile = os.path.join(indir, file)

        stat = os.stat(absfile, follow_symlinks=False)
        size = stat.st_size
        modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
        sha = FileEntry.sha256sum(absfile)

        return FileEntry(file, size, modified, sha)

    def from_dict(d, db):
        return FileEntry(d["name"], d["size"], d["modified"], d["sha"], db)

@dataclasses.dataclass
class Database:
    data: list[FileEntry] = dataclasses.field(default_factory=list)
    version: int = DBVERSION

    def add(self, file: FileEntry):
        self.data.append(file)
    
    def writeJson(self, outfile):
        with open(outfile, "w") as dbfile:
            json.dump(self, dbfile, cls=DatabaseEncoder, indent=4)

def createTar(tardb, size):
    global tarfileindex

    basefilepath = f"{tarfileprefix}-{tarfileindex:06}"
    tarfileindex = tarfileindex + 1

    print(f"Creating tar {basefilepath} with {len(tardb.data)} files ({size}).")

    # TODO: Use tmp directory
    with tarfile.open(os.path.join(dbdir, basefilepath + ".tar"), "w") as tar:
        for fileentry in tardb.data:
            tar.add(os.path.join(indir, fileentry.name),
                arcname=fileentry.name, recursive="False")
    # TODO: Upload tar and delete

    tardb.writeJson(os.path.join(dbdir, basefilepath + ".json"))
    # TODO: Upload json

# 128 MB chunks is a good sweet spot pricing-wise
# Note that chunks can be larger as we only fit in full files.
MINSIZE=128*1024*1024

if len(sys.argv) != 2:
    print("Usage: python s3-uploader.py indir")
    exit()

# TODO: Better location for database cache
dbdir = "/home/drinkcat/.cache/s3-uploader/one/"

tarfileprefix = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
tarfileindex = 0

indir = os.path.abspath(sys.argv[1])

if not os.path.isdir(dbdir):
    os.makedirs(dbdir)

# TODO: Sync database with remote

# Read existing database
db = {}
dbfiles = [f for f in os.listdir(dbdir) if f.endswith(".json")]
dbfiles.sort()
for j in dbfiles:
    with open(os.path.join(dbdir, j), "r") as read_content:
        try:
            jsondata = json.load(read_content)
        except Exception as e:
            raise ValueError(f"Database error in {j}.") from e
        if jsondata["version"] != DBVERSION:
            raise SystemError(f"Database version error in {j}: {jsondata["version"]}")
        for file in jsondata["data"]:
            fe = FileEntry.from_dict(file, j)
            db[fe.name] = fe

print(f"Read database: {len(db)} files.")

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

print(f"Found {len(inlist)} files.")

# Database for the current output tar
tardb = Database()
size = 0
totalskip = 0
lastskip = 0
totalwritten = 0

for file in inlist:
    fileentry = FileEntry.gen(indir, file)

    # TODO: Find in database and skip if needed
    dbfile = db.get(file)
    if dbfile and dbfile.sha == fileentry.sha:
        totalskip += 1
        if totalskip >= lastskip+1000:
            print(f"Skipped {totalskip} files so far.")
            lastskip = totalskip
        continue

    totalwritten += 1

    size += fileentry.size
    tardb.add(fileentry)
    if size > MINSIZE:
        if totalskip > lastskip:
            print(f"Skipped {totalskip} files so far.")
            lastskip = totalskip

        createTar(tardb, humanSize(size))
        tardb = Database()
        size = 0

# Create the last archive
if len(tardb.data) > 0:
    createTar(tardb, humanSize(size))

print(f"Done! Wrote {totalwritten} files. Skipped {totalskip} files.")
