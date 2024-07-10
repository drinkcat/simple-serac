#!/bin/python

import argparse
import os
import pathlib
import pprint

from simple_s3 import SimpleS3
from simple_database import BackupDatabase
from simple_utils import list_files

### Remote storage related functions

def remote_check_bucket_configuration_lifecycle(s3, config):
    (warnings, errors) = (0, 0)

    noncurrentexpiration = False
    abortmultipart = False
    for rule in config["lifecycle"]["Rules"]:
        if rule["Status"] != "Enabled":
            print(f"INFO: Rule ${rule["ID"]} is disabled, ignoring.")
            continue
        # Only allow blank filter, or with a Prefix shorter than URL's prefix
        if rule["Filter"] != {}:
            keys = list(rule["Filter"].keys())
            if len(keys) > 1 or keys[0] != "Prefix":
                print(f"INFO: Rule {rule["ID"]} filter too elaborate, assuming it does not apply.")
                warnings += 1
                continue
            if not s3.prefix.startswith(rule["Filter"]["Prefix"]):
                print(f"INFO: Rule {rule["ID"]} prefix doesn't cover provided URL.")
                continue
        if nve := rule.get("NoncurrentVersionExpiration"):
            # Not a good rule if noncurrent versions are kept
            if nve.get("NewerNoncurrentVersions"):
                print(f"INFO: Rule {rule["ID"]} keeps noncurrent versions, ignoring.")
            elif nve.get("NoncurrentDays"):
                noncurrentexpiration = True
        if aimu := rule.get("AbortIncompleteMultipartUpload"):
            if aimu.get("DaysAfterInitiation"):
                abortmultipart = True

    if not noncurrentexpiration:
        print("ERROR: Found no rule that would expire noncurrent objects.")
        errors += 1

    if not abortmultipart:
        print("ERROR: Found no rule that would abort incomplete multipart uploads.")
        errors += 1

    return (warnings, errors)

def remote_check_bucket_configuration(s3):
    (warnings, errors) = (0, 0)

    print(f"Checking bucket {s3.bucket} configuration:")
    config = s3.get_bucket_configuration()
    pprint.PrettyPrinter().pprint(config)
    print("----")

    if config["versioning"]["Status"] == "Enabled":
        (w,e) = remote_check_bucket_configuration_lifecycle(s3, config)
        warnings += w
        errors += e
    else:
        print("Versioning not enabled, no need to check for lifecycle rules.")

    if pabc := config['public_access_block'].get('PublicAccessBlockConfiguration'):
        for key in pabc:
            if not pabc[key]:
                print(f"ERROR: PublicAccessBlockConfiguration {key} not True.")
                errors += 1
    else:
        print("ERROR: No PublicAccessBlockConfiguration.")
        errors += 1

    return (warnings, errors)
    #

def remote_check(s3, verify, tarstorageclass):
    (warnings, errors) = (0, 0)

    if verify:
        (files, outdated) = s3.list_versions()
    else:
        files = s3.list_files()
        outdated = {}
    # Sanity check of the files
    jsonfiles = [file for file in files if file.startswith("db/") and file.endswith(".json")]
    tarfiles = [file for file in files if file.startswith("data/") and file.endswith(".tar")]

    for jsonfile in jsonfiles:
        base = jsonfile.removeprefix("db/").removesuffix(".json")
        if "data/" + base + ".tar" not in tarfiles:
            print(f"ERROR on remote: {base}.json without the corresponding tar.")
            errors += 1

    for tar in tarfiles:
        base = tar.removeprefix("data/").removesuffix(".tar")
        if "db/" + base + ".json" not in jsonfiles:
            print(f"WARNING: Remote {base}.tar without the corresponding json.")
            warnings += 1
        if files[tar].storageclass != tarstorageclass:
            print(f"WARNING: Remote {tar} in incorrect storage class {files[tar].storageclass} is not expected {tarstorageclass}.")
            warnings += 1

    for file in files:
        if file.startswith("report/") and file.endswith(".csv"):
            continue
        if file in jsonfiles or file in tarfiles:
            continue
        print(f"WARNING: Remote {file} not supposed to be in bucket.")
        warnings += 1

    for file in outdated:
        prefix = ""
        print(f"WARNING: Remote {file} has one of more outdated copies (noncurrent), that probably should be expired.")
        warnings += 1

    if verify:
        (w,e) = remote_check_bucket_configuration(s3)
        warnings += w
        errors += e

    return (warnings, errors)

def gen_cache_directory(outurl):
    # TODO: Not sure if this is cross-platform
    saneurl = "".join([ c if c.isalnum() else "_" for c in outurl ])
    return pathlib.Path.home() / ".cache" / "simple-uploader" / saneurl

### Main

def main():
    # TODO: Add minimum tar size as parameter
    parser = argparse.ArgumentParser(
                    description='Backup to S3 Glacier',
                    epilog='https://github.com/drinkcat/simple-serac',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-n', '--dry-run', action='store_true', help="Do not actually upload anything")
    parser.add_argument('s3url', help="S3 URL, i.e. s3://bucket/directory")
    parser.add_argument('-v', '--verify', action='store_true', help="Verify remote bucket configuration and state.")
    parser.add_argument('-i', '--input', action='store', dest='indir', type=str, help="Input directory")
    parser.add_argument('-c', '--class', action='store', dest='storageclass', type=str, default="DEEP_ARCHIVE", help="upload class (e.g. STANDARD or DEEP_ARCHIVE)")
    args = parser.parse_args()

    indir = args.indir
    if indir is not None:
        indir = os.path.abspath(args.indir)
        if not os.path.isdir(indir):
            raise SystemError(f"Input directory {indir} does not exist.")

    outurl = args.s3url

    # Storage class for the tarballs (json always in "STANDARD")
    storageclass = args.storageclass

    # Create Database cache directory
    dbcachedir = gen_cache_directory(outurl)
    if not os.path.isdir(dbcachedir):
        os.makedirs(dbcachedir)

    # Sync and load database
    print("Syncing database...")
    s3 = SimpleS3(outurl, dry_run=args.dry_run)
    (warnings, errors) = remote_check(s3, args.verify, storageclass)
    print(f"{errors} errors and {warnings} warnings while checking remote.")
    if errors > 0:
        print(f"Errors found, please fix issues manually")

    s3.download_dir(dbcachedir, "db")

    backupdb = BackupDatabase(dbcachedir, s3)
    backupdb.read_database()

    if indir is None:
        print("No input directory given, bailing out!")
        return

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