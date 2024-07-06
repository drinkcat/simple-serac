#!/bin/python

import boto3
import dataclasses
import os
import sys
import threading
import time
import urllib.parse
from simple_utils import human_size, human_size_2

@dataclasses.dataclass
class S3File:
    name: str
    size: int
    storageclass: str
    # TODO: Do we want to store sha as well?

def addslash(s):
    s = s.removeprefix("/")
    if s != "" and not s.endswith("/"):
        return s + "/"
    return s

class SimpleS3:
    def __init__(self, url, dry_run=False):
        self.dry_run = dry_run

        self.s3_client = boto3.client('s3')
        self.files = None
        parse = urllib.parse.urlparse(url)
        if parse.scheme != "s3":
            raise SystemError(f"Bad URL {url} does not start with s3.")
        self.bucket = parse.netloc
        self.prefix = addslash(parse.path)

        print(f"Bucket: {self.bucket}, Prefix: '{self.prefix}'{' (DRY RUN)' if self.dry_run else ''}")

    def list_files(self):
        self.files = {}
        start_after = ""
        is_truncated = True
        while is_truncated:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix, StartAfter=start_after)
            is_truncated = response["IsTruncated"]
            if is_truncated:
                start_after = response["Contents"][-1]["Key"]
            for content in response.get("Contents", ()):
                name = content["Key"]
                # Trim prefix (this could be done with os.path functions?)
                if not name.startswith(self.prefix):
                    raise SystemError(f"Weird name {name} does not start with {self.prefix}")
                name = name[len(self.prefix):]
                size = content["Size"]
                storageclass = content["StorageClass"]
                self.files[name] = S3File(name, size, storageclass)
        print(f"Got {len(self.files)} files in bucket folder.")
        return self.files

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

                # TODO: Use hash too?
                if self.files[file].size == stat.st_size and not os.path.islink(localfile):
                    # print(f"Local file {localfilebase} already good.")
                    # Good local copy
                    goodfiles.append(localfilebase)
                else:
                    print(f"Local file {localfilebase} incorrect, moving away.")
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

    def upload_file(self, filename, subdir="", targetname=None):
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

        if self.dry_run:
            print(f"DRY RUN: Would have uploaded {targetname} to s3://{self.bucket}/{self.prefix}{subdir}.")
            return

        sys.stdout.write(f"Uploading {targetname}...")
        sys.stdout.flush()
        if filename in self.files:
            raise SystemError(f"Refusing to override existing file {filename} in bucket.")

        # TODO: set storage class
        # Upload the file
        size = os.path.getsize(filename)
        start = time.monotonic()
        self.s3_client.upload_file(filename, self.bucket, objectname, Callback=ProgressPercentage(targetname, size))
        interval = time.monotonic() - start
        if interval != 0:
            speed = size/interval
        else:
            speed = 0
        sys.stdout.write(f"\rUploaded {targetname} to s3://{self.bucket}/{self.prefix}{subdir} ({human_size(speed)}/s).\n")

        # Make sure we don't accidentally upload a second time
        self.files[filename] = "Uploaded"

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
    #s3 = SimpleS3(sys.argv[1])
    #s3.list_files()
    dbfiles = [f for f in os.listdir(".") if f.endswith(".json")]
    dbfiles.sort()
    #for j in dbfiles:
    #    s3.upload_file(j, "db")
    #s3.download_dir("./db", "db")
    #print(human_size_2(1024, 104857))
