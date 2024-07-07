import os

SIZE_SUFFIXES = ['B', 'KiB', 'MiB', 'GiB']

def human_size(size):
    i = 0
    while size >= 1024 and i < len(SIZE_SUFFIXES)-1:
        size = size // 1024
        i += 1
    return f"{size} {SIZE_SUFFIXES[i]}"

def human_size_2(size, total):
    i = 0
    while total >= 1024 and i < len(SIZE_SUFFIXES)-1:
        total = total // 1024
        size = size // 1024
        i += 1
    stotal = str(total)
    ssize = str(size).rjust(len(stotal), " ")
    return f"{ssize} / {stotal} {SIZE_SUFFIXES[i]}"

def list_files(indir):
    # List all files in input directory
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
    return inlist

if __name__ == "__main__":
    print(human_size(1024))
    print(human_size_2(1024, 104857))
