#!/usr/bin/env python3


import os.path
import re
import argparse

extpatt = re.compile('^\.(c|cc|cpp|cxx|c\+\+|h|hh|hpp|hxx|h\+\+)$')
pragma_omp = re.compile('^\s*#pragma omp')


def remove_pragma_fp(fp):
    '''Return whether the file contains OMP pragma.'''
    # print(fp)
    lines = []
    omp_continue = False  # is inside an pragma block?
    found_omp = False
    for line in open(fp).readlines():
        match_pragma = pragma_omp.search(line)
        if not omp_continue and match_pragma is None:
            lines.append(line)
            continue
        found_omp = True
        # print(line, end='')
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
    return found_omp


def remove_pragma(dir_path):
    '''Return list of files which contain OMP pragma'''
    omp_files = []
    dir_path = os.path.abspath(dir_path)
    if(os.path.isfile(dir_path)):
        omp = remove_pragma_fp(dir_path)
        if omp:
            omp_files.append(dir_path)
        return omp_files
    for root, dirs, files in os.walk(dir_path):
        # print(root)
        for fn in files:
            # print(fp)
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
    for root in roots:
        omp_files = remove_pragma(root)
        if(omp_files):
            print('modified following files in place:' + '*'*30)
            print('\n'.join(omp_files))


if __name__ == '__main__':
    main()
