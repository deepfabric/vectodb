
#include "index_flat_wrapper.h"
#include "faiss/IndexFlat.h"
#include <shared_mutex>
#include <mutex>
#include <pthread.h>
#include <sstream>
#include <string>
#include <unordered_map>

using namespace std;
using mtxlock = unique_lock<mutex>;
using rlock = unique_lock<shared_mutex>;
using wlock = shared_lock<shared_mutex>;

struct IndexFlatWrapper {
    shared_mutex rw_flat;
    faiss::IndexFlat* flat;
    unordered_map<uint64_t, uint64_t> xid2num;
    vector<uint64_t> xids; //vector of xid of all vectors
};

void* IndexFlatNew(long dim)
{
    IndexFlatWrapper* ifw = new IndexFlatWrapper();
    ifw->flat = new faiss::IndexFlat(dim, faiss::METRIC_INNER_PRODUCT);
    return ifw;
}

void IndexFlatDelete(void* ifwIn)
{
    IndexFlatWrapper* ifw = static_cast<IndexFlatWrapper*>(ifwIn);
    delete ifw->flat;
    delete ifw;
}

void IndexFlatAddWithIds(void* ifwIn, long nb, float* xb, unsigned long* xids)
{
    IndexFlatWrapper* ifw = static_cast<IndexFlatWrapper*>(ifwIn);
    wlock w{ ifw->rw_flat };
    long ntotal = ifw->flat->ntotal;
    ifw->flat->add(nb, xb);
    for (long i = 0; i < nb; i++) {
        ifw->xid2num[xids[i]] = ntotal + i;
        ifw->xids.push_back(xids[i]);
    }
}

void IndexFlatSearch(void* ifwIn, long nq, float* xq, float* distances, unsigned long* xids)
{
    static const long k = 1;
    IndexFlatWrapper* ifw = static_cast<IndexFlatWrapper*>(ifwIn);
    {
        rlock r{ ifw->rw_flat };
        ifw->flat->search(nq, xq, k, distances, (long*)xids);
    }
    for (int i = 0; i < nq; i++) {
        xids[i] = ifw->xids[xids[i]];
    }
}
