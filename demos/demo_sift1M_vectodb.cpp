#include "vectodb.hpp"

#include "faiss/IndexFlat.h"
#include "vectodb.h"

#include <glog/logging.h>
#include <iostream>
#include <memory>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>

#include <cassert>

using namespace std;

const long sift_dim = 128L;
const char* work_dir = "/tmp/demo_sift1M_vectodb_cpp";

const int small_set_size = 32;

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

int check_indexflat(faiss::IndexFlat *flat, const string& work_dir, long vecs_per_user)
{
    string fp = work_dir + "/flatdisk.index";
    int fd = open(fp.c_str(), O_RDONLY);

    struct stat buf;
    fstat(fd, &buf);
    long totsize = buf.st_size;
    uint8_t *ptr = (uint8_t*)mmap(nullptr, totsize, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    size_t capacity = *(size_t *)(ptr + flat->header_size());
    float *xb = (float *)(ptr + flat->header_size() + sizeof(capacity));
    long *ids = (long *)(ptr + flat->header_size() + sizeof(capacity) + sizeof(float)* (flat->d) *capacity);
    int rc = memcmp(flat->xb.data(), xb, flat->ntotal * (flat->d) * sizeof(float));
    if(rc!=0){
        LOG(ERROR) << "IndexFlatDisk xb is corrupted!";
        return rc;
    }
    int i = 0;
    for(; (i<flat->ntotal) && ids[i]==GetXid(i/vecs_per_user, i); i++);
    if(i<flat->ntotal){
        LOG(ERROR) << "IndexFlatDisk xid is corrupted!";
        return rc;
    }
    munmap(ptr, totsize);
    return 0;
}

int demo_search_recall(size_t d, size_t nb, float* xb)
{
    LOG(INFO) << "Loading database";

    //Search performance(10000 queries):
    //"IVF1,Flat", "nprobe=1":      458s
    //"Flat":                       51s
    //"IVF4096,PQ32", "nprobe=256"  26s
    //"IVF16384_HNSW32,Flat", "nprobe=384"  23s

    ClearDir(work_dir);
    VectoDB vdb(work_dir, d);
    //vdb.Reset();
    faiss::IndexFlat flat(d, faiss::METRIC_INNER_PRODUCT);

    long* xids = new long[nb];
    const static long vecs_per_user = 100L;
    for (long i = 0; i < (long)nb; i++) {
        xids[i] = GetXid(i/vecs_per_user, i);
    }

    const bool incremental = false;
    if (incremental) {
        const long batch_size = std::min(100000L, (long)nb);
        const long batch_num = nb / batch_size;
        assert(nb % batch_size == 0);
        for (long i = 0; i < batch_num; i++) {
            LOG(INFO) << "Calling vdb.AddWithIds " << nb;
            vdb.AddWithIds(batch_size, xb + i * batch_size * sift_dim, xids + i * batch_size);
        }
    } else {
        LOG(INFO) << "Calling vdb.AddWithIds " << nb;
        vdb.AddWithIds(nb, xb, xids);
    }
    LOG(INFO) << "Calling flat.add " << nb;
    flat.add(nb, xb);

    //check IndexFlatDisk file content
    LOG(INFO) << "Checking IndexFlatDisk file";
    if(flat.ntotal != vdb.GetTotal()) {
        LOG(ERROR) << "vdb is corrupted! flat.ntotal " << flat.ntotal << ", vdb.GetTotal() " << vdb.GetTotal();
        return -1;
    }
    int rc = check_indexflat(&flat, work_dir, vecs_per_user);
    if(rc!=0){
        LOG(ERROR) << "IndexFlatDisk file is corrupted!";
        return rc;
    }

    LOG(INFO) << "Searching index";
    //const long nq = 10000;
    const long nq = 1000;
    const long k = 400;
    const float* xq = xb;
    float* D = new float[nq*k];
    long* I = new long[nq*k];
    float* D2 = new float[nq*k];
    long* I2 = new long[nq*k];

    LOG(INFO) << "Executing " << nq << " queries in single batch";
    vdb.Search(nq, xq, k, true, nullptr, D, I);

    const long num_threads = 0;
    if (num_threads >= 2) {
        LOG(INFO) << "Executing " << nq << " queries in multiple threads";
        const long batch_size = (long)nq / num_threads;
        vector<thread> workers;
        for (long i = 0; i < num_threads; i++) {
            std::thread worker{ [&vdb, batch_size, i, &xq, &D, &I]() {
                LOG(INFO) << "thread " << i << " begins";
                vdb.Search(batch_size, xq + i * batch_size * sift_dim, k, true, nullptr, D + i * batch_size * k, I + i * batch_size * k);
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
            vdb.Search(1, xq + i * sift_dim, k, true, nullptr, D + i*k, I + i*k);
        }
    }

    LOG(INFO) << "Generating ground truth";
    flat.search(nq, xq, k, D2, I2);

    LOG(INFO) << "Compute recalls";
    // Another metric is mAP(https://zhuanlan.zhihu.com/p/35983818).
    vector<int> total(k), hit(k);
    for (int i=0; i<k; i++) {
        total[k] = hit[k] = 0;
    }
    for (long q = 0; q < (long)nq; q++) {
        for(int i=0; i<k; i++) {
            if(I2[q*k+i]!=-1L){
                total[i]++;
                for(int j=0; j<k; j++){
                    if(GetXid(I2[q*k+i]/vecs_per_user, I2[q*k+i])==I[q*k+j]){
                        hit[i]++;
                        break;
                    }
                }
            }
        }
    }
    int sum_total=0, sum_hit=0;
    ostringstream oss;
    for (int i=0; i<k; i++) {
        sum_total += total[i];
        sum_hit += hit[i];
        oss << "\t" << (float)sum_hit/(float)sum_total;
    }
    LOG(INFO) << oss.str();

    delete[] D;
    delete[] I;
    delete[] D2;
    delete[] I2;
    delete[] xids;
    return 0;
}

int demo_search_bitmap(size_t d, size_t nb, float* xb, int vecs_per_user, int nq, int k, bool top_vectors, int bm_card)
{
    ClearDir(work_dir);
    VectoDB vdb(work_dir, d);
    //vdb.Reset();

    long* xids = new long[nb];
    for (long i = 0; i < (long)nb; i++) {
        xids[i] = GetXid(i/vecs_per_user, i);
    }
    LOG(INFO) << "Calling vdb.AddWithIds " << nb;
    vdb.AddWithIds(nb, xb, xids);

    const float* xq = xb;
    vector<float> D(nq*k);
    vector<long> I(nq*k);
    vector<roaring_bitmap_t *> rbs(nq);
    vector<char *> uids(nq);

    if (bm_card >= 0) {
        for(int i=0; i<nq; i++){
            int uid = i/vecs_per_user;
            rbs[i] = roaring_bitmap_create();
            roaring_bitmap_add_range(rbs[i], uid, uid+bm_card);
            int size;
            ChBitmapSerialize(rbs[i], uids[i], size);
        }
    } else {
        for(int i=0; i<nq; i++){
            rbs[i] = nullptr;
        }
    }

    LOG(INFO) << "Executing " << nq << " queries in single batch";
    vdb.Search(nq, xq, k, top_vectors, (long *)(&uids[0]), &D[0], &I[0]);

    LOG(INFO) << "Checking result";
    bool printed_error = false;
    if (bm_card >= 0) {
        for(int i=0; i<nq; i++){
            for(int j=0; j<k; j++){
                long xid = I[i*k+j];
                if(xid==-1L)
                    break;
                uint64_t uid = GetUid(xid);
                uint64_t pid = GetPid(xid);
                bool c = roaring_bitmap_contains(rbs[i], uint32_t(uid));
                if(!c && !printed_error){
                    LOG(ERROR) << "Bitmap filter bug, i " << i << ", xid " << xid << ", uid " << uid << ", pid " << pid;
                    printed_error = true;
                }
            }
        }
    }

    for(int i=0; i<nq; i++){
        if(rbs[i]!=nullptr)
            roaring_bitmap_free(rbs[i]);
        delete[] uids[i];
    }
    delete[] xids;
    return 0;
}

int demo_bitmap_codec()
{
    int nums[] = {0, 1, small_set_size-1, small_set_size, small_set_size+1, 100, 10000};
    for(int i=0; i<sizeof(nums)/sizeof(int); i++) {
        int num = nums[i];
        roaring_bitmap_t *rb1, *rb2;
        rb1 = roaring_bitmap_create();
        roaring_bitmap_add_range(rb1, 0, num);

        char *buf;
        int size;
        ChBitmapSerialize(rb1, buf, size);

        rb2 = ChBitmapDeserialize(buf);
        if(rb2==nullptr){
            LOG(ERROR) << "ChBitmapSerialize/ChBitmapDeserialize bug, failed to deserialize, num " << num;
            return -1;
        }
        roaring_bitmap_t *rb3 = roaring_bitmap_xor(rb1, rb2);
        int can = roaring_bitmap_get_cardinality(rb3);
        if(can != 0) {
            LOG(ERROR) << "ChBitmapSerialize/ChBitmapDeserialize bug, num " << num << ", xor size " << can;
            roaring_bitmap_free(rb3);
            return -1;
        }
        delete[] buf;
        roaring_bitmap_free(rb1);
        roaring_bitmap_free(rb2);
    }
    return 0;
}

int main(int /*argc*/, char** argv)
{
    FLAGS_stderrthreshold = 0;
    FLAGS_log_dir = ".";
    google::InitGoogleLogging(argv[0]);

    size_t nb, d;
    float* xb = fvecs_read("sift1M/sift_base.fvecs", &d, &nb);
    for (long i = 0; i < (long)nb; i++) {
        /*
        //Randomlizing causes far less recall. Don't do that.
        for(long j = 0; j<(long)d; j++)
            xb[i*d+j] = float(2 * random() - RAND_MAX);
        */
        NormVec(xb+i*d, d);
    }

    int rc = demo_bitmap_codec();
    if(rc<0)
        return rc;
    demo_search_recall(d, nb, xb);

	LOG(INFO) << "demo_search_bitmap(dim, nb, xb, 1, 1000, 400, true, -1)";
	demo_search_bitmap(d, nb, xb, 1, 1000, 400, true, -1);

	LOG(INFO) << "demo_search_bitmap(dim, nb, xb, 1, 1000, 400, false, -1)";
	demo_search_bitmap(d, nb, xb, 1, 1000, 400, false, -1);

	LOG(INFO) << "demo_search_bitmap(dim, nb, xb, 100, 1000, 400, false, -1)";
	demo_search_bitmap(d, nb, xb, 100, 1000, 400, false, -1);

	LOG(INFO) << "demo_search_bitmap(dim, nb, xb, 1, 1000, 400, false, 10)";
	demo_search_bitmap(d, nb, xb, 1, 1000, 400, false, 10);

	LOG(INFO) << "demo_search_bitmap(dim, nb, xb, 1, 1000, 400, false, 100000000)";
	demo_search_bitmap(d, nb, xb, 1, 1000, 400, false, 100000000);

	LOG(INFO) << "demo_search_bitmap(dim, nb, xb, 100, 1000, 400, false, 100000000)";
	demo_search_bitmap(d, nb, xb, 100, 1000, 400, false, 100000000);

    delete[] xb;
}
