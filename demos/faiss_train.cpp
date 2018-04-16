/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <omp.h>
#include <string>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "AutoTune.h"
#include "index_io.h"

using namespace std;

static const long train_ratio = 10; //typical value is 5~10

/**
 * To run this demo, please download the ANN_SIFT1M dataset from
 *
 *   http://corpus-texmex.irisa.fr/
 *
 * and unzip it to the sudirectory sift1M.
 * 
 * This demo trains an index for the given database.
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
    omp_set_num_threads(2);

    const string usage("faiss_train index_key metric_type database (output)index");
    if (argc != 5) {
        cerr << usage << endl;
        exit(-1);
    }
    char* index_key = argv[1];
    char* metric_type = argv[2];
    char* database = argv[3];
    char* fname_index = argv[4];
    const char* supported_index_keys[9] = {
        "IVF4096,Flat",
        "Flat",
        "PQ32",
        "PCA80,Flat",
        "IVF4096,PQ8+16",
        "IVF4096,PQ32",
        "IMI2x8,PQ32",
        "IMI2x8,PQ8+16",
        "OPQ16_64,IMI2x8,PQ8+16"
    };
    const char* selected_paramss[9] = {
        "nprobe=256",
        "",
        "ht=118",
        "",
        "nprobe=2048,ht=64,k_factor=64",
        "nprobe=256,ht=256",
        "nprobe=4096,ht=256,max_codes=inf",
        "nprobe=4096,ht=64,max_codes=32768,k_factor=16",
        "nprobe=4096,ht=64,max_codes=inf,k_factor=64"
    };

    const char* selected_params = nullptr;
    bool supported = false;
    for (int i = 0; i < sizeof(supported_index_keys) / sizeof(const char*); i++) {
        if (0 == strcmp(index_key, supported_index_keys[i])) {
            supported = true;
            selected_params = selected_paramss[i];
            break;
        }
    }
    if (!supported) {
        cerr << "index_key " << index_key << " is not supported!" << endl;
        cerr << "supported index_key are:";
        for (int i = 0; i < sizeof(supported_index_keys) / sizeof(const char*); i++) {
            cerr << "/" << supported_index_keys[i];
        }
        cerr << endl;
        cerr << "Note that only Flat is exact kNN search, others are approximate. And only Flat doesn't need train phase." << endl;
        exit(-1);
    }

    faiss::MetricType metric = faiss::METRIC_L2;
    if (0 == strcmp(metric_type, "L2")) {
        metric = faiss::METRIC_L2;
    } else if (0 == strcmp(metric_type, "IP")) {
        metric = faiss::METRIC_INNER_PRODUCT;
    } else {
        cerr << "metric_type " << metric_type << " is not supported!" << endl;
        cerr << "supported metric_type are: L2, IP" << endl;
        cerr << "Note that SIFT1M descriptors are not perfectly normalized, therefore neighbors for inner product and L2 distances are not strictly equivalent. The SIFT1M ground-truth is for L2, not inner product. " << endl;
        exit(-1);
    }

    double t0 = elapsed();

    printf("[%.3f s] Loading database\n", elapsed() - t0);
    size_t nb, d2;
    float* xb = fvecs_read(database, &d2, &nb);

    printf("[%.3f s] Preparing index \"%s\" d=%ld\n", elapsed() - t0, index_key, d2);
    faiss::Index* index = faiss::index_factory(d2, index_key, metric);

    if (strcmp(index_key, "Flat")) {
        printf("[%.3f s] Generating train set\n", elapsed() - t0);
        /*size_t nt = nb / train_ratio;
        float* xt = new float[nt * d2];
        for (size_t i = 0; i < nt; i += 1) {
            memcpy(xt + i * d2, xb + i * train_ratio * d2, sizeof(float) * d2);
        }

        printf("[%.3f s] Training on %ld vectors\n", elapsed() - t0, nt);
        index->train(nt, xt);
        delete[] xt;
        */
        long nt = std::min(long(nb), std::max(long(nb / 10), 100000L));
        index->train(nt, xb);

        // selected_params is cached auto-tuning result.
        faiss::ParameterSpace params;
        params.initialize(index);
        params.set_index_parameters(index, selected_params);
    }

    printf("[%.3f s] Indexing database, size %ld*%ld\n", elapsed() - t0, nb, d2);
    index->add(nb, xb);

    printf("[%.3f s] Writing %s\n", elapsed() - t0, fname_index);
    faiss::write_index(index, fname_index);

    delete[] xb;
    delete index;
    printf("[%.3f s] done\n", elapsed() - t0);
    return 0;
}
