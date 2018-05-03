#!/usr/bin/env python3


import os.path
import re
import argparse

extpatt = re.compile('^\.(c|cc|cpp|cxx|c\+\+|h|hh|hpp|hxx|h\+\+)$')
pragma_omp = re.compile('^\s*#pragma omp')
omp_func = re.compile('omp_')


def remove_pragma_fp(fp):
    '''Return whether the file contains OMP function calls.'''
    print(fp)
    lines = []
    omp_continue = False  # is inside an pragma block?
    found_omp = False
    for line in open(fp).readlines():
        match_func = omp_func.search(line)
        if match_func is not None:
            print(line.rstrip(), "\t***skipped***")
            return True
        match_pragma = pragma_omp.search(line)
        if not omp_continue and match_pragma is None:
            lines.append(line)
            continue
        found_omp = True
        print(line, end='')
        line = line.rstrip()
        if line.endswith("\\"):
            omp_continue = True
        else:
            omp_continue = False
    if found_omp:
        tmpfp = fp + ".tmp"
        tmpf = open(tmpfp, 'w')
        tmpf.writelines(lines)
        tmpf.close()
        os.rename(tmpfp, fp)
    return False


def remove_pragma(dir_path):
    '''Return list of files which contain OMP function calls.'''
    omp_files = []
    dir_path = os.path.abspath(dir_path)
    if(os.path.isfile(dir_path)):
        omp = remove_pragma_fp(dir_path)
        if omp:
            omp_files.append(dir_path)
        return omp_files
    for root, dirs, files in os.walk(dir_path, followlinks=True):
        # print(root)
        for fn in files:
            # print(fn)
            ext = os.path.splitext(fn)[1]
            if extpatt.match(ext) is None:
                continue
            fp = os.path.join(root, fn)
            omp = remove_pragma_fp(fp)
            if omp:
                omp_files.append(fp)
    return omp_files


def main():
    parser = argparse.ArgumentParser(
        description='Remove "#pragma omp" lines of all C/C++ files under given directory. Files contain OMP function calls will be skipped.')
    parser.add_argument(
        'root', nargs='*', default=['.'], metavar='root', help='root of the working tree')
    args = parser.parse_args()
    roots = sorted(args.root)
    # print(roots)
    omp_files = []
    for root in roots:
        omp_files2 = remove_pragma(root)
        omp_files += omp_files2
    if 0 != len(omp_files):
        print('*'*120)
        print('WARNING: following files are skipped since they contain OMP function calls:')
        for fp in sorted(omp_files):
            print(fp)


if __name__ == '__main__':
    main()
