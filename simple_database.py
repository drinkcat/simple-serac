#!/bin/python

import csv
import dataclasses
import hashlib
import json
import os
import tarfile
import tempfile
from datetime import datetime, timezone
from simple_utils import human_size

### Basic utils

def flat_date():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

### Database related functions

class DatabaseFileEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, FileEntry):
            dict = o.__dict__
            del(dict["db"])
            del(dict["alt"])
            return dict
        elif isinstance(o, DatabaseFile):
            return o.__dict__
        else:
            raise SystemError(f"No idea what to do with {o}.")

@dataclasses.dataclass
class FileEntry:
    name: str
    size: int
    modified: str
    sha: str
    db: str = ""
    alt: list('FileEntry') = dataclasses.field(default_factory=list)

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
class DatabaseFile:
    DBVERSION = 1

    data: list[FileEntry] = dataclasses.field(default_factory=list)
    version: int = DBVERSION

    def add(self, file: FileEntry):
        self.data.append(file)
    
    def writeJson(self, outfile):
        with open(outfile, "w") as dbfile:
            json.dump(self, dbfile, cls=DatabaseFileEncoder, indent=4)

class BackupDatabase:
    # 256 MB chunk is a good sweet spot pricing-wise
    # Note that chunks can be larger as we only fit in full files.
    DEFAULT_MINSIZE=256*1024*1024

    # Whole database: map from filename to FileEntry
    db = None

    def __init__(self, dbcachedir, s3, minsize=DEFAULT_MINSIZE):
        self.dbcachedir = dbcachedir
        self.s3 = s3
        # Minimum archive size
        self.minsize = minsize

    def read_database(self):
        # Read existing database
        self.db = {}
        dbfiles = [f for f in os.listdir(self.dbcachedir) if f.endswith(".json")]
        dbfiles.sort()
        for j in dbfiles:
            with open(os.path.join(self.dbcachedir, j), "r") as read_content:
                try:
                    jsondata = json.load(read_content)
                except Exception as e:
                    raise ValueError(f"Database error in {j}.") from e
                if jsondata["version"] != DatabaseFile.DBVERSION:
                    raise SystemError(f"Database version error in {j}: {jsondata["version"]}")
                for file in jsondata["data"]:
                    fe = FileEntry.from_dict(file, j)
                    # Save other copies in alt
                    existing = self.db.get(fe.name)
                    if existing:
                        fe.alt = [ existing ] + existing.alt
                        existing.alt = []
                    self.db[fe.name] = fe
        print(f"Read database: {len(self.db)} files.")

    def create_tar(self, tardb, indir, basefilepath, size, storageclass):
        print(f"Creating tar {basefilepath} with {len(tardb.data)} files ({human_size(size)}).")

        with tempfile.NamedTemporaryFile() as outtarobj:
            with tarfile.open(fileobj=outtarobj, mode="w") as tar:
                for fileentry in tardb.data:
                    tar.add(os.path.join(indir, fileentry.name),
                        arcname=fileentry.name, recursive="False")
            self.s3.upload_file(outtarobj.name, "data", basefilepath + ".tar", storageclass=storageclass)

        outjson = os.path.join(self.dbcachedir, basefilepath + ".json")
        tardb.writeJson(outjson)
        self.s3.upload_file(outjson, "db", storageclass="STANDARD")

    def create_tars(self, indir, inlist, storageclass):
        class FileNameGen:
            def __init__(self, prefix):
                self.prefix = prefix
                self.index = 0

            def next(self):
                filename = f"{self.prefix}-{self.index:06}"
                self.index += 1
                return filename
    
        filenamegen = FileNameGen(flat_date())

        # Database file for the current output tar
        tardb = DatabaseFile()
        size = 0
        totalskip = 0
        lastskip = 0
        totalwritten = 0

        for file in inlist:
            fileentry = FileEntry.gen(indir, file)

            dbfile = self.db.get(file)
            if dbfile and dbfile.sha == fileentry.sha:
                totalskip += 1
                if totalskip >= lastskip+1000:
                    print(f"Skipped {totalskip} files so far.")
                    lastskip = totalskip
                continue

            totalwritten += 1

            size += fileentry.size
            tardb.add(fileentry)
            if size > self.minsize:
                if totalskip > lastskip:
                    print(f"Skipped {totalskip} files so far.")
                    lastskip = totalskip

                self.create_tar(tardb, indir, filenamegen.next(), size, storageclass)
                tardb = DatabaseFile()
                size = 0

        # Create the last archive
        if len(tardb.data) > 0:
            self.create_tar(tardb, indir, filenamegen.next(), size, storageclass)

        print(f"Done! Wrote {totalwritten} files. Skipped {totalskip} files.")
        return (totalwritten, totalskip)

    def generate_CSV(self):
        with tempfile.NamedTemporaryFile(mode='w+') as outcsvobj:
            csvwriter = csv.writer(outcsvobj, quoting=csv.QUOTE_NONNUMERIC)
            csvwriter.writerow(["tar File", "Filename", "Size", "Modified", "SHA"])
            for file in self.db:
                def file_to_array(fe):
                    tarname = fe.db.removesuffix(".json") + ".tar"
                    return (tarname, file, fe.size, fe.modified, fe.sha)

                csvwriter.writerow(file_to_array(self.db[file]))
                for altfile in self.db[file].alt:
                    csvwriter.writerow(file_to_array(altfile))

            self.s3.upload_file(outcsvobj.name, "report", flat_date() + ".csv")
