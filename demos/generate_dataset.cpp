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

#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <random>
#include <sys/time.h>

#include "faiss/AutoTune.h"
#include "faiss/IndexFlat.h"

using namespace std;
namespace fs = boost::filesystem;

double t0;

/**
 *   http://corpus-texmex.irisa.fr/
 * .bvecs, .fvecs and .ivecs vector file formats:

 *****************************************************
 * I/O functions for fvecs and ivecs
 *****************************************************/

float* fvecs_read(const char* fname,
    size_t* d_out, size_t* n_out)
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

double elapsed()
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec + tv.tv_usec * 1e-6;
}

void normalize(size_t dim, float* vec)
{
    double l = 0;
    for (size_t i = 0; i < dim; i++) {
        l += double(vec[i]) * double(vec[i]);
    }
    l = sqrt(l);
    for (size_t i = 0; i < dim; i++) {
        vec[i] = (float)(((double)vec[i]) / l);
    }
    return;
}

// https://zh.cppreference.com/w/cpp/numeric/random
// 生成围绕平均值的正态分布
class Random {
private:
    std::random_device r;
    std::seed_seq seed2;
    std::mt19937 e2;
    double mean;
    double var;
    std::normal_distribution<> normal_dist;

public:
    Random(double mean_in, double var_in)
        : seed2({ r(), r(), r(), r(), r(), r(), r(), r() })
        , e2(seed2)
        , mean(mean_in)
        , var(var_in)
        , normal_dist(mean, var)
    {
    }
    double get()
    {
        return normal_dist(e2);
    }
};

void expand(size_t dim, float* vec, int ratio, Random& randGen, std::vector<char>& outbuf, int& outlen)
{
    outlen = 4 + 4 * dim * ratio;
    outbuf.resize(outlen);
    *(int*)(&outbuf[0]) = dim * ratio;
    for (int i = 0; i < ratio; i++) {
        int offset = 4 + 4 * dim * i;
        for (size_t j = 0; j < dim; j++) {
            int offset2 = offset + 4 * j;
            float var = vec[j];
            double rand = randGen.get();
            var = (float)(rand * (double)var);
            *(float*)(&outbuf[offset2]) = var;
        }
    }
    normalize(dim * ratio, (float*)(&outbuf[4]));
}

void expand_fvecs(string fp, string outdir, int repeats)
{
    fs::create_directories(outdir);

    size_t d, nb;
    float* xb = fvecs_read(fp.c_str(), &d, &nb);

    //https://stackoverflow.com/questions/31483349/how-can-i-open-a-file-for-reading-writing-creating-it-if-it-does-not-exist-w
    string fp_base;
    std::fstream fs_base;
    fs_base.exceptions(std::ios::failbit | std::ios::badbit);

    Random randGen(4, 2);
    std::vector<char> outbuf;
    int outlen;

    fs::path ph(fp);
    string fn = ph.filename().string();
    for (int r = 0; r < repeats; r++) {
        std::ostringstream oss;
        oss << outdir << "/" << fn;
        if (repeats > 1)
            oss << "." << r;
        fp_base = oss.str();

        fs_base.open(fp_base, std::fstream::out | std::fstream::binary | std::fstream::trunc);

        for (size_t i = 0; i < nb; i++) {
            expand(d, &xb[d * i], 4, randGen, outbuf, outlen);
            fs_base.write(&outbuf[0], outlen);
        }
        fs_base.close();
        printf("[%.3f s] done %s\n", elapsed() - t0, fp_base.c_str());
    }
    delete[] xb;
}

void generate_groundtruth(string fp_base, string fp_query, string outdir, int seq)
{
    size_t d, nb;
    float* xb = fvecs_read(fp_base.c_str(), &d, &nb);
    size_t d2, nq;
    float* xq = fvecs_read(fp_query.c_str(), &d2, &nq);

    faiss::Index* flat = new faiss::IndexFlat(d, faiss::METRIC_INNER_PRODUCT);
    flat->add(nb, xb);

    long k = 5;
    faiss::Index::idx_t* I = new faiss::Index::idx_t[nq * k];
    float* D = new float[nq * k];
    flat->search(nq, xq, k, &D[0], &I[0]);

    std::ostringstream oss;
    oss << outdir << "/sift_groundtruth." << seq;
    string fp_ground = oss.str();
    std::fstream fs_ground;
    fs_ground.exceptions(std::ios::failbit | std::ios::badbit);
    fs_ground.open(fp_ground, std::fstream::out | std::fstream::binary | std::fstream::trunc);

    //fs_ground format: long nq, long k, float D[nq*k], long I[nq*k]
    fs_ground.write((const char*)&nq, sizeof(nq));
    fs_ground.write((const char*)&k, sizeof(k));
    fs_ground.write((const char*)D, sizeof(float) * nq * k);
    fs_ground.write((const char*)I, sizeof(long) * nq * k);
    fs_ground.close();

    delete flat;
    delete[] I;
    delete[] D;
    delete[] xb;
    delete[] xq;

    printf("[%.3f s] done %s\n", elapsed() - t0, fp_ground.c_str());
}

int main(int argc, char** argv)
{
    const string usage("generate_dataset [base|query|ground] [repeats]");
    if (argc < 2 || argc > 3) {
        cerr << usage << endl;
        exit(-1);
    }
    string outdir("sift100M");
    t0 = elapsed();

    if (strcmp(argv[1], "base") == 0) {
        int repeats = atoi(argv[2]);
        expand_fvecs("sift1M/sift_base.fvecs", outdir, repeats);
    } else if (strcmp(argv[1], "query") == 0) {
        expand_fvecs("sift1M/sift_query.fvecs", outdir, 1);
    } else {
        int repeats = atoi(argv[2]);
        for (int i = 0; i < repeats; i++) {
            std::ostringstream oss;
            oss << outdir << "/sift_base.fvecs." << i;
            string fp_base = oss.str();

            std::ostringstream oss2;
            oss2 << outdir << "/sift_query.fvecs";
            string fp_query = oss2.str();

            generate_groundtruth(fp_base, fp_query, outdir, i);
        }
    }

    return 0;
}
