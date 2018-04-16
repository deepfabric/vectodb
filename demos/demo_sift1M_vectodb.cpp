#include "vectodb.h"

#include <glog/logging.h>

#include <iostream>
#include <memory>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>

using namespace std;

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

// train phase, input: index_key database train_set, output: index
int main(int argc, char* argv[])
{
    FLAGS_stderrthreshold = 0;
    FLAGS_log_dir = ".";
    google::InitGoogleLogging(argv[0]);

    LOG(INFO) << "Loading database";
    const char* work_dir = "/tmp";
    VectoDB::ClearWorkDir(work_dir);
    //auto vdb{ std::make_unique<VectoDB>("/tmp", 128, 1) };
    const long sift_dim = 128L;
    VectoDB vdb(work_dir, sift_dim, 1);
    size_t nb, d;
    float* xb = fvecs_read("sift1M/sift_base.fvecs", &d, &nb);
    long* xids = new long[nb];
    for (long i = 0; i < (long)nb; i++) {
        xids[i] = i;
    }

    const bool incremental = true;
    if (incremental) {
        const long batch_size = 200000L;
        const long batch_num = nb / batch_size;
        assert(nb % batch_size == 0);
        for (long i = 0; i < batch_num; i++) {
            vdb.AddWithIds(batch_size, xb + i * batch_size * sift_dim, xids + i * batch_size);
            faiss::Index* index;
            long ntrain;
            vdb.BuildIndex(index, ntrain);
            vdb.ActivateIndex(index, ntrain);
        }
    } else {
        vdb.AddWithIds(nb, xb, xids);

        faiss::Index* index;
        long ntrain;
        vdb.BuildIndex(index, ntrain);
        vdb.ActivateIndex(index, ntrain);
    }
    delete[] xb;
    delete[] xids;

    LOG(INFO) << "Searching index";
    size_t nq;
    size_t d2;
    float* xq = fvecs_read("sift1M/sift_query.fvecs", &d2, &nq);
    float* D = new float[nq];
    long* I = new long[nq];
    vdb.Search(nq, xq, D, I);

    size_t k; // nb of results per query in the GT
    long* gt; // nq * k matrix of ground-truth nearest-neighbors
    {
        LOG(INFO) << "Loading ground truth for " << nq << " queries";

        // load ground-truth and convert int to long
        size_t nq2;
        int* gt_int = ivecs_read("sift1M/sift_groundtruth.ivecs", &k, &nq2);
        assert(nq2 == nq || !"incorrect nb of ground truth entries");

        gt = new long[k * nq];
        for (long i = 0; i < (long)(k * nq); i++) {
            gt[i] = gt_int[i];
        }
        delete[] gt_int;
    }

    LOG(INFO) << "Compute recalls";
    // evaluate result by hand.
    int n_1 = 0;
    for (long i = 0; i < (long)nq; i++) {
        long gt_nn = gt[i * k];
        if (I[i] == gt_nn) {
            n_1++;
        }
    }
    LOG(INFO) << "R@1 = " << n_1 / float(nq);

    delete[] D;
    delete[] I;
}
