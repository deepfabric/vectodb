/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <sys/time.h>

#include <iostream>
#include <string>

#include "AutoTune.h"
#include "index_io.h"

using namespace std;

/**
 * To run this demo, please download the ANN_SIFT1M dataset from
 *
 *   http://corpus-texmex.irisa.fr/
 *
 * and unzip it to the sudirectory sift1M.
 * 
 * This demo checks if train(or query) vectors are inside the database.
 **/

/*****************************************************
 * I/O functions for fvecs and ivecs
 *****************************************************/

float*
fvecs_read(const char* fname, size_t* d_out, size_t* n_out)
{
    FILE* f = fopen(fname, "r");
    if (!f) {
        fprintf(stderr, "could not open %s\n", fname);
        perror("");
        abort();
    }
    int d;
    fread(&d, 1, sizeof(int), f);
    assert((d > 0 && d < 1000000) || !"unreasonable dimension");
    fseek(f, 0, SEEK_SET);
    struct stat st;
    fstat(fileno(f), &st);
    size_t sz = st.st_size;
    assert(sz % ((d + 1) * 4) == 0 || !"weird file size");
    size_t n = sz / ((d + 1) * 4);

    *d_out = d;
    *n_out = n;
    float* x = new float[n * (d + 1)];
    size_t nr = fread(x, sizeof(float), n * (d + 1), f);
    assert(nr == n * (d + 1) || !"could not read whole file");

    // shift array to remove row headers
    for (size_t i = 0; i < n; i++)
        memmove(x + i * d, x + 1 + i * (d + 1), d * sizeof(*x));

    fclose(f);
    return x;
}

// not very clean, but works as long as sizeof(int) == sizeof(float)
int* ivecs_read(const char* fname, size_t* d_out, size_t* n_out)
{
    return (int*)fvecs_read(fname, d_out, n_out);
}

double
elapsed()
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

// train phase, input: index_key database train_set, output: index
int main(int argc, char** argv)
{
    const string usage("vectodb_validate database [train_set] [query]");
    if (argc < 3) {
        cerr << usage << endl;
        exit(-1);
    }
    char* database = argv[1];
    size_t nb, d;
    float* xb = fvecs_read(database, &d, &nb);

    double t0 = elapsed();

    for (int c = 2; c < argc; c++) {
        size_t nq, d2;
        float* xq = fvecs_read(argv[c], &d2, &nq);
        assert(d == d2 || !"dataset does not have same dimension as train set");

        size_t found = 0;
#pragma omp parallel for
        for (size_t i = 0; i < nq; i++) {
            for (size_t j = 0; j < nb; j++) {
                if (0 == memcmp(xq + i * d, xb + j * d, d * sizeof(float))) {
                    found++;
                    break;
                }
            }
        }
        if (found != nq) {
            printf("[%.3f s] %s nq %ld, found %ld\n", elapsed() - t0, argv[c], nq, found);
        }
        delete[] xq;
    }
    delete[] xb;
    printf("[%.3f s] done\n", elapsed() - t0);
    return 0;
}
