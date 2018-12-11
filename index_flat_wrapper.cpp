
#include "index_flat_wrapper.h"
#include "faiss/IndexFlat.h"
#include <boost/thread/shared_mutex.hpp>
#include <mutex>
#include <pthread.h>
#include <sstream>
#include <string>
#include <unordered_map>

using namespace std;
using mtxlock = unique_lock<mutex>;
using rlock = unique_lock<boost::shared_mutex>;
using wlock = boost::shared_lock<boost::shared_mutex>;

struct IndexFlatWrapper {
    float dist_threshold;
    boost::shared_mutex rw_flat;
    faiss::IndexFlat* flat;
    unordered_map<long, long> xid2num;
    vector<long> xids; //vector of xid of all vectors
};

void* IndexFlatNew(long dim, float dist_threshold)
{
    IndexFlatWrapper* ifw = new IndexFlatWrapper();
    ifw->dist_threshold = dist_threshold;
    ifw->flat = new faiss::IndexFlat(dim, faiss::METRIC_INNER_PRODUCT);
    return ifw;
}

void IndexFlatDelete(void* ifwIn)
{
    IndexFlatWrapper* ifw = static_cast<IndexFlatWrapper*>(ifwIn);
    delete ifw->flat;
    delete ifw;
}

void IndexFlatAddWithIds(void* ifwIn, long nb, float* xb, long* xids)
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

void IndexFlatSearch(void* ifwIn, long nq, float* xq, float* distances, long* xids)
{
    static const long k = 1;
    IndexFlatWrapper* ifw = static_cast<IndexFlatWrapper*>(ifwIn);
    {
        rlock r{ ifw->rw_flat };
        ifw->flat->search(nq, xq, k, distances, xids);
    }
    for (int i = 0; i < nq; i++) {
        if (distances[i] < ifw->dist_threshold) {
            xids[i] = long(-1);
        } else {
            xids[i] = ifw->xids[xids[i]];
        }
    }
}
