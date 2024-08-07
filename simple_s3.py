#!/bin/python

import argparse
import boto3
import dataclasses
import hashlib
import os
import pprint
import sys
import threading
import time
import urllib.parse
from simple_utils import human_size_2, human_size_f, list_files

@dataclasses.dataclass
class S3File:
    name: str
    size: int
    storageclass: str
    md5: str
    # TODO: Do we want to store sha as well?

    def gen_from_s3(content, prefix):
        name = content["Key"]
        # Trim prefix (this could be done with os.path functions?)
        if not name.startswith(prefix):
            raise SystemError(f"Weird name {name} does not start with {prefix}")
        name = name[len(prefix):]
        size = content["Size"]
        storageclass = content["StorageClass"]
        # TODO: We also stored a SHA-256 but that requires one more operation.
        md5 = content["ETag"].strip('"')
        # TODO: Hack, for now, add restore status to the class string
        if "RestoreStatus" in content:
            if content["RestoreStatus"]["IsRestoreInProgress"]:
                storageclass += " (restoring)"
            else:
                storageclass += f" (restored until {content["RestoreStatus"]["RestoreExpiryDate"]})"
        return S3File(name, size, storageclass, md5)

def addslash(s):
    s = s.removeprefix("/")
    if s != "" and not s.endswith("/"):
        return s + "/"
    return s

class SimpleS3:
    # TODO: Make this a parameter, chunk size for multipart upload
    DEFAULT_CHUNKSIZE=32*1024*1024

    def __init__(self, url, dry_run=False):
        self.dry_run = dry_run

        self.s3_client = boto3.client('s3')
        self.files = None
        parse = urllib.parse.urlparse(url)
        if parse.scheme != "s3":
            raise SystemError(f"Bad URL {url} does not start with s3.")
        self.bucket = parse.netloc
        self.prefix = addslash(parse.path)

        self.transferconfig = boto3.s3.transfer.TransferConfig(
            multipart_threshold=self.DEFAULT_CHUNKSIZE, multipart_chunksize=self.DEFAULT_CHUNKSIZE)

        print(f"Bucket: {self.bucket}, Prefix: '{self.prefix}'{' (DRY RUN)' if self.dry_run else ''}")

    def list_files(self):
        self.files = {}
        start_after = ""
        is_truncated = True
        while is_truncated:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix,
                StartAfter=start_after, OptionalObjectAttributes=['RestoreStatus'])
            is_truncated = response["IsTruncated"]
            if is_truncated:
                start_after = response["Contents"][-1]["Key"]
            for content in response.get("Contents", {}):
                s3file = S3File.gen_from_s3(content, self.prefix)
                self.files[s3file.name] = s3file
        print(f"Got {len(self.files)} files in bucket folder.")
        return self.files

    def list_versions(self):
        latest = {}
        outdated = {}
        next_key_marker = ""
        is_truncated = True
        while is_truncated:
            response = self.s3_client.list_object_versions(Bucket=self.bucket, Prefix=self.prefix,
                    KeyMarker=next_key_marker, OptionalObjectAttributes=['RestoreStatus'])
            is_truncated = response["IsTruncated"]
            if is_truncated:
                next_key_marker = response["NextKeyMarker"]
            for content in response.get("Versions", ()):
                s3file = S3File.gen_from_s3(content, self.prefix)
                if content["IsLatest"]:
                    latest[s3file.name] = s3file
                else:
                    outdated[s3file.name] = outdated.get(s3file.name, []) + [ s3file ]
        print(f"Got {len(latest)} latest files in bucket folder, and {len(outdated)} outdated files.")
        return (latest, outdated)

    # TODO: Return stats?
    def download_dir(self, localdir, subdir=""):
        subdir = addslash(subdir)

        if self.files is None:
            self.list_files()

        localfiles = os.listdir(localdir)
        localfiles.sort()
        goodfiles = []

        for file in self.files:
            if not file.startswith(subdir):
                continue
            localfilebase = file[len(subdir):]
            localfile = os.path.join(localdir, localfilebase)

            pull = False
            if localfilebase in localfiles:
                pull = False
                stat = os.stat(localfile, follow_symlinks=False)

                bad = None
                if os.path.islink(localfile):
                    bad = "link"
                elif self.files[file].size != stat.st_size:
                    bad = "size"
                else:
                    # TODO: This will never match if the file is multipart
                    # We store the SHA-256, but MD5 is readily available.
                    with open(localfile, 'rb') as f:
                        md5 = hashlib.file_digest(f, 'md5').hexdigest()
                    if self.files[file].md5 != md5:
                        print(f"{self.files[file].md5} != {md5}")
                        bad = "hash"

                if bad is None:
                    # print(f"Local file {localfilebase} already good.")
                    # Good local copy
                    goodfiles.append(localfilebase)
                else:
                    print(f"Local file {localfilebase} incorrect ({bad}), moving away.")
                    os.rename(localfile, localfile + "~")
                    pull = True
            else:
                pull = True

            if not pull:
                continue
            
            print(f"Downloading {localfilebase}...")
            objectname = self.prefix + file
            self.s3_client.download_file(self.bucket, objectname, localfile)
            goodfiles.append(localfilebase)

        # Check for leftovers
        for localfilebase in localfiles:
            if localfilebase in goodfiles:
                continue
            # Already a backup
            if localfilebase.endswith("~"):
                continue
            
            # Make a backup
            print(f"Found leftover file {localfilebase} in local database, moving away.")
            localfile = os.path.join(localdir, localfilebase)
            os.rename(localfile, localfile + "~")

    def upload_file(self, filename, subdir="", targetname=None, storageclass="STANDARD"):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        """

        if self.files is None:
            self.list_files()

        if targetname is None:
            targetname = os.path.basename(filename)

        subdir = addslash(subdir)
        objectname = self.prefix + subdir + targetname

        if targetname in self.files:
            raise FileExistsError(f"Refusing to override existing file {targetname} in bucket.")

        if self.dry_run:
            print(f"DRY RUN: Would have uploaded {targetname} to s3://{self.bucket}/{self.prefix}{subdir} ({storageclass}).")
            return

        sys.stdout.write(f"Uploading {targetname}...")
        sys.stdout.flush()

        # Upload the file
        size = os.path.getsize(filename)
        start = time.monotonic()
        self.s3_client.upload_file(filename, self.bucket, objectname,
            ExtraArgs={"ChecksumAlgorithm": "SHA256", "StorageClass": storageclass},
            Callback=ProgressPercentage(targetname, size),
            Config=self.transferconfig
            )
        interval = time.monotonic() - start
        if interval != 0:
            speed = float(size) / interval
        else:
            speed = 0
        sys.stdout.write(f"\rUploaded {targetname} to s3://{self.bucket}/{self.prefix}{subdir} ({human_size_f(speed)}/s, {storageclass}).\n")

        # Make sure we don't accidentally upload a second time
        self.files[targetname] = "Uploaded"

    # TODO: Fully implement this
    def get_file_attributes(self, name):
        response = self.s3_client.get_object_attributes(Bucket=self.bucket, Key=self.prefix + name,
            ObjectAttributes=['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize'])
        return response

    # Dump bucket configuration (at least what we care about)
    def get_bucket_configuration(self):
        config = {}

        config["acl"] = self.s3_client.get_bucket_acl(Bucket=self.bucket)
        config["encryption"] = self.s3_client.get_bucket_encryption(Bucket=self.bucket)
        config["lifecycle"] = self.s3_client.get_bucket_lifecycle_configuration(Bucket=self.bucket)
        config["notification"] = self.s3_client.get_bucket_notification_configuration(Bucket=self.bucket)
        config["public_access_block"] = self.s3_client.get_public_access_block(Bucket=self.bucket)
        config["versioning"] = self.s3_client.get_bucket_versioning(Bucket=self.bucket)

        # Remove ResponseMetadata
        for key in config:
            del config[key]['ResponseMetadata']

        return config

class ProgressPercentage(object):
    def __init__(self, filename, size):
        self._filename = filename
        self._size = size
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                f"\rUploading {self._filename} {human_size_2(self._seen_so_far, self._size)} ({percentage:.2f}%)")
            sys.stdout.flush()

if __name__ == "__main__":
    # TODO: Add minimum tar size as parameter
    parser = argparse.ArgumentParser(
                    description='Operations to S3',
                    epilog='https://github.com/drinkcat/simple-serac',
                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-n', '--dry-run', action='store_true', help="do not actually upload anything")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('-l', '--list', action='store_true', help="list files in bucket")
    action.add_argument('-v', '--versions', action='store_true', help="list file versions in bucket")
    action.add_argument('-d', '--dump', action='store_true', help="dump bucket configuration")
    action.add_argument('-u', '--upload', action='store', type=str, metavar="INDIR", help="upload directory to s3")
    action.add_argument('-f', '--file', action='store', type=str, help="dump file attributes")
    parser.add_argument('-c', '--class', action='store', dest='storageclass', type=str, default="STANDARD", help="upload class (e.g. STANDARD or DEEP_ARCHIVE)")
    parser.add_argument('s3url', help="S3 URL, i.e. s3://bucket/directory")
    args = parser.parse_args()

    outurl = args.s3url

    s3 = SimpleS3(outurl, dry_run=args.dry_run)

    if args.dump:
        config = s3.get_bucket_configuration()
        pprint.PrettyPrinter().pprint(config)
    elif args.file:
        fileattr = s3.get_file_attributes(args.file)
        pprint.PrettyPrinter().pprint(fileattr)
    elif args.list:
        s3.list_files()
        for file in s3.files:
            s3file = s3.files[file]
            print(f"{file} ({s3file.size}, {s3file.storageclass})")
    elif args.upload:
        indir = os.path.abspath(args.upload)
        if not os.path.isdir(indir):
            raise SystemError(f"Input directory {indir} does not exist.")

        s3.list_files()

        inlist = list_files(indir)
        for file in inlist:
            try:
                s3.upload_file(os.path.join(indir, file), targetname=file, storageclass=args.storageclass)
            except FileExistsError:
                print(f"WARNING: Skippping existing file {file}.")
    elif args.versions:
        (latest, outdated) = s3.list_versions()
        for file in latest:
            s3file = latest[file]
            print(f"{file} ({s3file.size}, {s3file.storageclass}, LATEST)")
            if outdated.get(file):
                for s3file in outdated[file]:
                    print(f"  {file} ({s3file.size}, {s3file.storageclass}, OUTDATED)")
                del(outdated[file])
        # Leftover outdated files
        print("Deleted files:")
        for file in outdated:
            prefix = ""
            for s3file in outdated[file]:
                print(f"  {prefix}{file} ({s3file.size}, {s3file.storageclass}, OUTDATED)")
                prefix = "  "
