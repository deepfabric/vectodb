#include "vectodb.hpp"

#include <glog/logging.h>

#include <iostream>
#include <memory>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <thread>
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
int main(int /*argc*/, char** argv)
{
    FLAGS_stderrthreshold = 0;
    FLAGS_log_dir = ".";
    google::InitGoogleLogging(argv[0]);

    LOG(INFO) << "Loading database";
    const long sift_dim = 128L;
    const char* work_dir1 = "/tmp/demo_sift1M_vectodb_cpp1";
    const char* work_dir2 = "/tmp/demo_sift1M_vectodb_cpp2";

    //ClearDir(work_dir1);
    //ClearDir(work_dir2);
    //VectoDB vdb(work_dir, sift_dim);
    VectoDB vdb1(work_dir1, sift_dim, "IVF4096,PQ32", "nprobe=256,ht=256");
    //VectoDB vdb1(work_dir, sift_dim, "IVF16384_HNSW32,Flat", "nprobe=384");
    VectoDB vdb2(work_dir2, sift_dim, "Flat", "");

    size_t nb, d;
    float* xb = fvecs_read("sift1M/sift_base.fvecs", &d, &nb);
    long* xids = new long[nb];
    for (long i = 0; i < (long)nb; i++) {
        xids[i] = i;
        for(long j = 0; j<(long)d; j++) {
            xb[i*d+j] = (float)drand48();
        }
        NormVec(xb+i*d, d);
    }

    if(vdb1.GetTotal()==0 && vdb2.GetTotal()==0) {
        const bool incremental = false;
        if (incremental) {
            const long batch_size = std::min(100000L, (long)nb);
            const long batch_num = nb / batch_size;
            assert(nb % batch_size == 0);
            for (long i = 0; i < batch_num; i++) {
                LOG(INFO) << "Calling vdb1.AddWithIds " << nb;
                vdb1.AddWithIds(batch_size, xb + i * batch_size * sift_dim, xids + i * batch_size);
                LOG(INFO) << "Calling vdb1.SyncIndex";
                vdb1.SyncIndex();
            }
        } else {
            LOG(INFO) << "Calling vdb1.AddWithIds " << nb;
            vdb1.AddWithIds(nb, xb, xids);
            LOG(INFO) << "Calling vdb1.SyncIndex";
            vdb1.SyncIndex();
        }
        LOG(INFO) << "Calling vdb2.AddWithIds " << nb;
        vdb2.AddWithIds(nb, xb, xids);
        LOG(INFO) << "Calling vdb2.SyncIndex";
        vdb2.SyncIndex();
    }

    LOG(INFO) << "Searching index";
    const long nq = 1000;
    const long k = 10;
    const float* xq = xb;
    float* D = new float[nq*k];
    long* I = new long[nq*k];
    float* D2 = new float[nq*k];
    long* I2 = new long[nq*k];

    LOG(INFO) << "Executing " << nq << " queries in single batch";
    vdb1.Search(nq, k, xq, nullptr, D, I);

    const long num_threads = 0;
    if (num_threads >= 2) {
        LOG(INFO) << "Executing " << nq << " queries in multiple threads";
        const long batch_size = (long)nq / num_threads;
        vector<thread> workers;
        for (long i = 0; i < num_threads; i++) {
            std::thread worker{ [&vdb1, batch_size, i, &xq, &D, &I]() {
                LOG(INFO) << "thread " << i << " begins";
                vdb1.Search(batch_size, k, xq + i * batch_size * sift_dim, nullptr, D + i * batch_size * k, I + i * batch_size * k);
                LOG(INFO) << "thread " << i << " ends";
            } };
            workers.push_back(std::move(worker));
        }
        for (long i = 0; i < num_threads; i++) {
            workers[i].join();
        }
    }

    const bool one_by_one = false;
    if (one_by_one) {
        LOG(INFO) << "Executing " << nq << " queries one by one";
        for (long i = 0; i < (long)nq; i++) {
            vdb1.Search(1, k, xq + i * sift_dim, nullptr, D + i*k, I + i*k);
        }
    }

    LOG(INFO) << "Generating ground truth";
    vdb2.Search(nq, k, xq, nullptr, D2, I2);

    LOG(INFO) << "Compute recalls";
    // evaluate result by hand.
    int total=0, hit = 0;
    for (long q = 0; q < (long)nq; q++) {
        for(int i=0; i<1; i++) {
            if(I2[q*k+i]!=-1L){
                total++;
                for(int j=0; j<k; j++){
                    if(I2[q*k+i]==I[q*k+j])
                        hit++;
                }
            }
        }
    }
    LOG(INFO) << "R@"<< k << "=" <<  float(hit)/total;

    delete[] D;
    delete[] I;
    delete[] D2;
    delete[] I2;
    delete[] xb;
    delete[] xids;
}
