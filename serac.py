#!/bin/python

import argparse
import os
import pathlib

from simple_s3 import SimpleS3
from simple_database import BackupDatabase
from simple_utils import list_files

### Remote storage related functions

def remote_check(s3):
    files = s3.list_files()
    # Sanity check of the files
    jsonfiles = [file for file in files if file.startswith("db/") and file.endswith(".json")]
    tarfiles = [file for file in files if file.startswith("data/") and file.endswith(".tar")]

    for jsonfile in jsonfiles:
        base = jsonfile.removeprefix("db/").removesuffix(".json")
        if "data/" + base + ".tar" not in tarfiles:
            raise SystemError(f"Error on remote: {base}.json without the corresponding tar.")

    for tar in tarfiles:
        base = tar.removeprefix("data/").removesuffix(".tar")
        if "db/" + base + ".json" not in jsonfiles:
            print(f"WARNING: Remote {base}.tar without the corresponding json.")
        if files[tar].storageclass != "DEEP_ARCHIVE":
            print(f"WARNING: Remote {tar} in incorrect storage class {files[tar].storageclass}.")

    for file in files:
        if file.startswith("report/") and file.endswith(".csv"):
            continue
        if file in jsonfiles or file in tarfiles:
            continue
        print(f"WARNING: Remote {file} not supposed to be in bucket.")

def gen_cache_directory(outurl):
    # TODO: Not sure if this is cross-platform
    saneurl = "".join([ c if c.isalnum() else "_" for c in outurl ])
    return pathlib.Path.home() / ".cache" / "simple-uploader" / saneurl

### Main

def main():
    # TODO: Add minimum tar size as parameter
    parser = argparse.ArgumentParser(
                    description='Backup to S3 Glacier',
                    epilog='https://github.com/drinkcat/simple-serac')
    parser.add_argument('-n', '--dry-run', action='store_true', help="Do not actually upload anything")
    parser.add_argument('indir', help="Input directory")
    parser.add_argument('s3url', help="S3 URL, i.e. s3://bucket/directory")
    args = parser.parse_args()

    indir = os.path.abspath(args.indir)
    if not os.path.isdir(indir):
        raise SystemError(f"Input directory {indir} does not exist.")
    outurl = args.s3url

    # Storage class for the tarballs (json always in "STANDARD")
    storageclass = "DEEP_ARCHIVE"

    # Create Database cache directory
    dbcachedir = gen_cache_directory(outurl)
    if not os.path.isdir(dbcachedir):
        os.makedirs(dbcachedir)

    # Sync and load database
    print("Syncing database...")
    s3 = SimpleS3(outurl, dry_run=args.dry_run)
    remote_check(s3)
    s3.download_dir(dbcachedir, "db")

    backupdb = BackupDatabase(dbcachedir, s3)
    backupdb.read_database()

    # List local files and back them up
    inlist = list_files(indir)
    (totalwritten, totalskip) = backupdb.create_tars(indir, inlist, storageclass)

    if totalwritten > 0:
        print("Generating csv report...")
        # Re-read the database, generate csv
        backupdb.read_database()
        backupdb.generate_CSV()
        print("Done!")

if __name__ == "__main__":
    main()