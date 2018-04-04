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
#include <omp.h>
#include <string>

#include "AutoTune.h"
#include "IndexFlat.h"
#include "IndexIVFFlat.h"
#include "index_io.h"

using namespace std;

/**
 * To run this demo, please download the ANN_SIFT1M dataset from
 *
 *   http://corpus-texmex.irisa.fr/
 *
 * and unzip it to the sudirectory sift1M.
 * 
 * This demo does kNN search for the given index, database and query.
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
    //Sets the number of threads in subsequent parallel regions.
    omp_set_num_threads(1);

    const string usage("vectodb_search index database query groundtruth");
    if (argc != 5) {
        cerr << usage << endl;
        exit(-1);
    }
    char* fname_index = argv[1];
    char* database = argv[2];
    char* query = argv[3];
    char* groundtruth = argv[4];

    double t0 = elapsed();

    printf("[%.3f s] Loading index\n", elapsed() - t0);
    faiss::Index* index = faiss::read_index(fname_index);

    printf("[%.3f s] Loading database\n", elapsed() - t0);
    size_t nb, d;
    float* xb = fvecs_read(database, &d, &nb);

    size_t nq, d2;
    float* xq;

    {
        printf("[%.3f s] Loading queries\n", elapsed() - t0);
        xq = fvecs_read(query, &d2, &nq);
        assert(d == d2 || !"query does not have same dimension as database");
    }

    size_t k; // nb of results per query in the GT
    faiss::Index::idx_t* gt; // nq * k matrix of ground-truth nearest-neighbors

    {
        printf("[%.3f s] Loading ground truth for %ld queries\n", elapsed() - t0, nq);
        // load ground-truth and convert int to long
        size_t nq2;
        int* gt_int = ivecs_read(groundtruth, &k, &nq2);
        assert(nq2 == nq || !"incorrect nb of ground truth entries");
        gt = new faiss::Index::idx_t[k * nq];
        for (int i = 0; i < k * nq; i++) {
            gt[i] = gt_int[i];
        }
        delete[] gt_int;
    }

    { // Perform a search
        printf("[%.3f s] Perform a search on %ld queries\n",
            elapsed() - t0, nq);

        // output buffers
        faiss::Index::idx_t* I = new faiss::Index::idx_t[nq * k];
        float* D = new float[nq * k];

        index->search(nq, xq, k, D, I);

        printf("[%.3f s] Compute recalls\n", elapsed() - t0);

        // evaluate result by hand.
        int n_1 = 0, n_10 = 0, n_100 = 0;
        for (int i = 0; i < nq; i++) {
            int gt_nn = gt[i * k];
            for (int j = 0; j < k; j++) {
                if (I[i * k + j] == gt_nn) {
                    if (j < 1)
                        n_1++;
                    if (j < 10)
                        n_10++;
                    if (j < 100)
                        n_100++;
                }
            }
        }
        printf("R@1 = %.4f\n", n_1 / float(nq));
        printf("R@10 = %.4f\n", n_10 / float(nq));
        printf("R@100 = %.4f\n", n_100 / float(nq));

        if (dynamic_cast<faiss::IndexFlat*>(index) == nullptr && dynamic_cast<faiss::IndexIVFFlat*>(index) == nullptr) {
            printf("[%.3f s] refining result\n", elapsed() - t0);
            n_1 = 0;
            n_10 = 0;
            n_100 = 0;
            for (int i = 0; i < nq; i++) {
                int gt_nn = gt[i * k];
                faiss::Index* index2 = faiss::index_factory(d2, "Flat");
                float* xb2 = new float[d2 * k];
                float* D2 = new float[k];
                faiss::Index::idx_t* I2 = new faiss::Index::idx_t[k];
                for (int j = 0; j < k; j++)
                    memcpy(xb2 + j * d, xb + I[i * k + j] * d, sizeof(float) * d);
                index2->add(k, xb2);
                index2->search(1, xq + i * d, k, D2, I2);
                for (int j = 0; j < k; j++) {
                    if (I[i * k + I2[j]] == gt_nn) {
                        if (j < 1 || D2[j] == D2[0])
                            n_1++;
                        if (j < 10 || D2[j] == D2[9])
                            n_10++;
                        if (j < 100 || D2[j] == D2[99])
                            n_100++;
                    }
                }
                delete[] xb2;
                delete[] D2;
                delete[] I2;
            }
            printf("R@1 = %.4f\n", n_1 / float(nq));
            printf("R@10 = %.4f\n", n_10 / float(nq));
            printf("R@100 = %.4f\n", n_100 / float(nq));
        }
    }

    delete[] xq;
    delete[] gt;
    delete[] xb;
    delete index;
    printf("[%.3f s] done\n", elapsed() - t0);

    return 0;
}
