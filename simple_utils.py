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

if __name__ == "__main__":
    print(human_size(1024))
    print(human_size_2(1024, 104857))
