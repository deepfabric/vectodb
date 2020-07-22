/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/IndexFlat.h>

#include <cstring>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/extra_distances.h>
#include <faiss/utils/utils.h>
#include <faiss/utils/Heap.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>

using namespace std;

namespace faiss {

IndexFlat::IndexFlat (idx_t d, MetricType metric):
            Index(d, metric)
{
}



void IndexFlat::add (idx_t n, const float *x) {
    xb.insert(xb.end(), x, x + n * d);
    ntotal += n;
}


void IndexFlat::reset() {
    xb.clear();
    ntotal = 0;
}


void IndexFlat::search (idx_t n, const float *x, idx_t k,
                               float *distances, idx_t *labels) const
{
    search(n, x, k, true, nullptr, distances, labels);
}


void IndexFlat::search (idx_t n, const float *x, idx_t k, bool top_vectors, roaring_bitmap_t ** rbs,
                               float *distances, idx_t *labels) const
{
    // we see the distances and labels as heaps

    if (metric_type == METRIC_INNER_PRODUCT) {
        float_minheap_array_t res = {
            size_t(n), size_t(k), labels, distances};
        knn_inner_product (x, xb.data(), nullptr, d, n, ntotal, top_vectors, rbs, &res);
    } else if (metric_type == METRIC_L2) {
        float_maxheap_array_t res = {
            size_t(n), size_t(k), labels, distances};
        knn_L2sqr (x, xb.data(), nullptr, d, n, ntotal, top_vectors, rbs, &res);
    } else {
        float_maxheap_array_t res = {
            size_t(n), size_t(k), labels, distances};
        knn_extra_metrics (x, xb.data(), nullptr, d, n, ntotal,
                           metric_type, metric_arg, top_vectors, rbs, &res);
    }
}


void IndexFlat::range_search (idx_t n, const float *x, float radius,
                              RangeSearchResult *result) const
{
    switch (metric_type) {
    case METRIC_INNER_PRODUCT:
        range_search_inner_product (x, xb.data(), nullptr, d, n, ntotal,
                                    radius, result);
        break;
    case METRIC_L2:
        range_search_L2sqr (x, xb.data(), nullptr, d, n, ntotal, radius, result);
        break;
    default:
        FAISS_THROW_MSG("metric type not supported");
    }
}


void IndexFlat::compute_distance_subset (
            idx_t n,
            const float *x,
            idx_t k,
            float *distances,
            const idx_t *labels) const
{
    switch (metric_type) {
        case METRIC_INNER_PRODUCT:
            fvec_inner_products_by_idx (
                 distances,
                 x, xb.data(), labels, d, n, k);
            break;
        case METRIC_L2:
            fvec_L2sqr_by_idx (
                 distances,
                 x, xb.data(), labels, d, n, k);
            break;
        default:
            FAISS_THROW_MSG("metric type not supported");
    }

}

size_t IndexFlat::remove_ids (const IDSelector & sel)
{
    idx_t j = 0;
    for (idx_t i = 0; i < ntotal; i++) {
        if (sel.is_member (i)) {
            // should be removed
        } else {
            if (i > j) {
                memmove (&xb[d * j], &xb[d * i], sizeof(xb[0]) * d);
            }
            j++;
        }
    }
    size_t nremove = ntotal - j;
    if (nremove > 0) {
        ntotal = j;
        xb.resize (ntotal * d);
    }
    return nremove;
}


namespace {


struct FlatL2Dis : DistanceComputer {
    size_t d;
    Index::idx_t nb;
    const float *q;
    const float *b;
    size_t ndis;

    float operator () (idx_t i) override {
        ndis++;
        return fvec_L2sqr(q, b + i * d, d);
    }

    float symmetric_dis(idx_t i, idx_t j) override {
        return fvec_L2sqr(b + j * d, b + i * d, d);
    }

    explicit FlatL2Dis(const IndexFlat& storage, const float *q = nullptr)
        : d(storage.d),
          nb(storage.ntotal),
          q(q),
          b(storage.xb.data()),
          ndis(0) {}

    void set_query(const float *x) override {
        q = x;
    }
};

struct FlatIPDis : DistanceComputer {
    size_t d;
    Index::idx_t nb;
    const float *q;
    const float *b;
    size_t ndis;

    float operator () (idx_t i) override {
        ndis++;
        return fvec_inner_product (q, b + i * d, d);
    }

    float symmetric_dis(idx_t i, idx_t j) override {
        return fvec_inner_product (b + j * d, b + i * d, d);
    }

    explicit FlatIPDis(const IndexFlat& storage, const float *q = nullptr)
        : d(storage.d),
          nb(storage.ntotal),
          q(q),
          b(storage.xb.data()),
          ndis(0) {}

    void set_query(const float *x) override {
        q = x;
    }
};




}  // namespace


DistanceComputer * IndexFlat::get_distance_computer() const {
    if (metric_type == METRIC_L2) {
        return new FlatL2Dis(*this);
    } else if (metric_type == METRIC_INNER_PRODUCT) {
        return new FlatIPDis(*this);
    } else {
        return get_extra_distance_computer (d, metric_type, metric_arg,
                                            ntotal, xb.data());
    }
}


void IndexFlat::reconstruct (idx_t key, float * recons) const
{
    memcpy (recons, &(xb[key * d]), sizeof(*recons) * d);
}


/* The standalone codec interface */
size_t IndexFlat::sa_code_size () const
{
    return sizeof(float) * d;
}

void IndexFlat::sa_encode (idx_t n, const float *x, uint8_t *bytes) const
{
    memcpy (bytes, x, sizeof(float) * d * n);
}

void IndexFlat::sa_decode (idx_t n, const uint8_t *bytes, float *x) const
{
    memcpy (x, bytes, sizeof(float) * d * n);
}




/***************************************************
 * IndexFlatL2BaseShift
 ***************************************************/

IndexFlatL2BaseShift::IndexFlatL2BaseShift (idx_t d, size_t nshift, const float *shift):
    IndexFlatL2 (d), shift (nshift)
{
    memcpy (this->shift.data(), shift, sizeof(float) * nshift);
}

void IndexFlatL2BaseShift::search (
            idx_t n,
            const float *x,
            idx_t k,
            float *distances,
            idx_t *labels) const
{
    FAISS_THROW_IF_NOT (shift.size() == ntotal);

    float_maxheap_array_t res = {
        size_t(n), size_t(k), labels, distances};
    knn_L2sqr_base_shift (x, xb.data(), d, n, ntotal, &res, shift.data());
}



/***************************************************
 * IndexRefineFlat
 ***************************************************/

IndexRefineFlat::IndexRefineFlat (Index *base_index):
    Index (base_index->d, base_index->metric_type),
    refine_index (base_index->d, base_index->metric_type),
    base_index (base_index), own_fields (false),
    k_factor (1)
{
    is_trained = base_index->is_trained;
    FAISS_THROW_IF_NOT_MSG (base_index->ntotal == 0,
                      "base_index should be empty in the beginning");
}

IndexRefineFlat::IndexRefineFlat () {
    base_index = nullptr;
    own_fields = false;
    k_factor = 1;
}


void IndexRefineFlat::train (idx_t n, const float *x)
{
    base_index->train (n, x);
    is_trained = true;
}

void IndexRefineFlat::add (idx_t n, const float *x) {
    FAISS_THROW_IF_NOT (is_trained);
    base_index->add (n, x);
    refine_index.add (n, x);
    ntotal = refine_index.ntotal;
}

void IndexRefineFlat::reset ()
{
    base_index->reset ();
    refine_index.reset ();
    ntotal = 0;
}

namespace {
typedef faiss::Index::idx_t idx_t;

template<class C>
static void reorder_2_heaps (
      idx_t n,
      idx_t k, idx_t *labels, float *distances,
      idx_t k_base, const idx_t *base_labels, const float *base_distances)
{
#pragma omp parallel for
    for (idx_t i = 0; i < n; i++) {
        idx_t *idxo = labels + i * k;
        float *diso = distances + i * k;
        const idx_t *idxi = base_labels + i * k_base;
        const float *disi = base_distances + i * k_base;

        heap_heapify<C> (k, diso, idxo, disi, idxi, k);
        if (k_base != k) { // add remaining elements
            heap_addn<C> (k, diso, idxo, disi + k, idxi + k, k_base - k);
        }
        heap_reorder<C> (k, diso, idxo);
    }
}


}


void IndexRefineFlat::search (
              idx_t n, const float *x, idx_t k,
              float *distances, idx_t *labels) const
{
    FAISS_THROW_IF_NOT (is_trained);
    idx_t k_base = idx_t (k * k_factor);
    idx_t * base_labels = labels;
    float * base_distances = distances;
    ScopeDeleter<idx_t> del1;
    ScopeDeleter<float> del2;


    if (k != k_base) {
        base_labels = new idx_t [n * k_base];
        del1.set (base_labels);
        base_distances = new float [n * k_base];
        del2.set (base_distances);
    }

    base_index->search (n, x, k_base, base_distances, base_labels);

    for (int i = 0; i < n * k_base; i++)
        assert (base_labels[i] >= -1 &&
                base_labels[i] < ntotal);

    // compute refined distances
    refine_index.compute_distance_subset (
        n, x, k_base, base_distances, base_labels);

    // sort and store result
    if (metric_type == METRIC_L2) {
        typedef CMax <float, idx_t> C;
        reorder_2_heaps<C> (
            n, k, labels, distances,
            k_base, base_labels, base_distances);

    } else if (metric_type == METRIC_INNER_PRODUCT) {
        typedef CMin <float, idx_t> C;
        reorder_2_heaps<C> (
            n, k, labels, distances,
            k_base, base_labels, base_distances);
    } else {
        FAISS_THROW_MSG("Metric type not supported");
    }

}



IndexRefineFlat::~IndexRefineFlat ()
{
    if (own_fields) delete base_index;
}

/***************************************************
 * IndexFlat1D
 ***************************************************/


IndexFlat1D::IndexFlat1D (bool continuous_update):
    IndexFlatL2 (1),
    continuous_update (continuous_update)
{
}

/// if not continuous_update, call this between the last add and
/// the first search
void IndexFlat1D::update_permutation ()
{
    perm.resize (ntotal);
    if (ntotal < 1000000) {
        fvec_argsort (ntotal, xb.data(), (size_t*)perm.data());
    } else {
        fvec_argsort_parallel (ntotal, xb.data(), (size_t*)perm.data());
    }
}

void IndexFlat1D::add (idx_t n, const float *x)
{
    IndexFlatL2::add (n, x);
    if (continuous_update)
        update_permutation();
}

void IndexFlat1D::reset()
{
    IndexFlatL2::reset();
    perm.clear();
}

void IndexFlat1D::search (
            idx_t n,
            const float *x,
            idx_t k,
            float *distances,
            idx_t *labels) const
{
    FAISS_THROW_IF_NOT_MSG (perm.size() == ntotal,
                    "Call update_permutation before search");

#pragma omp parallel for
    for (idx_t i = 0; i < n; i++) {

        float q = x[i]; // query
        float *D = distances + i * k;
        idx_t *I = labels + i * k;

        // binary search
        idx_t i0 = 0, i1 = ntotal;
        idx_t wp = 0;

        if (xb[perm[i0]] > q) {
            i1 = 0;
            goto finish_right;
        }

        if (xb[perm[i1 - 1]] <= q) {
            i0 = i1 - 1;
            goto finish_left;
        }

        while (i0 + 1 < i1) {
            idx_t imed = (i0 + i1) / 2;
            if (xb[perm[imed]] <= q) i0 = imed;
            else                    i1 = imed;
        }

        // query is between xb[perm[i0]] and xb[perm[i1]]
        // expand to nearest neighs

        while (wp < k) {
            float xleft = xb[perm[i0]];
            float xright = xb[perm[i1]];

            if (q - xleft < xright - q) {
                D[wp] = q - xleft;
                I[wp] = perm[i0];
                i0--; wp++;
                if (i0 < 0) { goto finish_right; }
            } else {
                D[wp] = xright - q;
                I[wp] = perm[i1];
                i1++; wp++;
                if (i1 >= ntotal) { goto finish_left; }
            }
        }
        goto done;

    finish_right:
        // grow to the right from i1
        while (wp < k) {
            if (i1 < ntotal) {
                D[wp] = xb[perm[i1]] - q;
                I[wp] = perm[i1];
                i1++;
            } else {
                D[wp] = std::numeric_limits<float>::infinity();
                I[wp] = -1;
            }
            wp++;
        }
        goto done;

    finish_left:
        // grow to the left from i0
        while (wp < k) {
            if (i0 >= 0) {
                D[wp] = q - xb[perm[i0]];
                I[wp] = perm[i0];
                i0--;
            } else {
                D[wp] = std::numeric_limits<float>::infinity();
                I[wp] = -1;
            }
            wp++;
        }
    done:  ;
    }

}

// keep sync with read_index_header, write_index_header
const static int off_header_d = sizeof(uint32_t);
const static int off_header_ntotal = off_header_d + sizeof(int);
const static int off_header_is_trained = off_header_ntotal + 3 * sizeof(idx_t);
const static int off_header_metric_type = off_header_is_trained + sizeof(int);
const static int off_header_metric_arg = off_header_metric_type + sizeof(int);

IndexFlatDisk::IndexFlatDisk (const std::string& filename_in, idx_t d, MetricType metric)
    : Index(d, metric)
    , xb(nullptr)
    , ids(nullptr)
    , filename(filename_in)
    , ptr(nullptr)
    , p_ntotal(nullptr)
    , totsize(0)
    , capacity(1000000L)
{
    pthread_rwlock_init(&rwlock, NULL);
    int fd = open(filename.c_str(), O_RDWR | O_APPEND);
    if (fd < 0) {
        fd = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        FAISS_THROW_IF_NOT_FMT(fd >= 0, "could not create or truncate %s: %s", filename.c_str(), strerror(errno));
        close(fd);
        totsize = header_size() + sizeof(capacity) + sizeof(float)*d*capacity + sizeof(idx_t)*capacity;
        int rc = truncate(filename.c_str(), totsize);
        FAISS_THROW_IF_NOT_FMT(rc == 0, "could not truncate %s: %s", filename.c_str(), strerror(errno));
        fd = open(filename.c_str(), O_RDWR);
        FAISS_THROW_IF_NOT_FMT(fd >= 0, "could not open %s: %s", filename.c_str(), strerror(errno));
        ptr = (uint8_t*)mmap(nullptr, totsize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
        FAISS_THROW_IF_NOT_FMT(ptr != MAP_FAILED, "could not mmap %s: %s", filename.c_str(), strerror(errno));
        close(fd);
        memcpy(ptr, "IxFD", 4);
        *(int *)(ptr + off_header_d) = d;
        *(idx_t *)(ptr + off_header_ntotal) = ntotal;
        *(int *)(ptr + off_header_is_trained) = 1; //is_trained
        *(int *)(ptr + off_header_metric_type) = metric_type;
        if (metric_type > 1)
            *(float *)(ptr + off_header_metric_arg) = metric_arg;
        *(size_t *)(ptr + header_size()) = capacity;
        msync(ptr, totsize, MS_SYNC);
        xb = (float *)(ptr + header_size() + sizeof(capacity));
        ids = (idx_t *)(ptr + header_size() + sizeof(capacity) + sizeof(float)*d*capacity);
    } else {
        struct stat buf;
        fstat(fd, &buf);
        totsize = buf.st_size;
        ptr = (uint8_t*)mmap(nullptr, totsize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
        FAISS_THROW_IF_NOT_FMT(ptr != MAP_FAILED, "could not mmap %s: %s", filename.c_str(), strerror(errno));
        close(fd);
        int rc = strncmp((char *)ptr, "IxFD", 4);
        FAISS_THROW_IF_NOT_MSG(rc==0, "index type is not IxFD");
        d = *(int *)(ptr + off_header_d);
        ntotal = *(idx_t *)(ptr + off_header_ntotal);
        is_trained = 1;
        metric_type = (MetricType)*(int *)(ptr + off_header_metric_type);
        if (metric_type > 1)
            metric_arg = *(float *)(ptr + off_header_metric_arg);
        capacity = *(size_t *)(ptr + header_size());
    }
    p_ntotal = (idx_t *)(ptr + off_header_ntotal);
    xb = (float *)(ptr + header_size() + sizeof(capacity));
    ids = (idx_t *)(ptr + header_size() + sizeof(capacity) + sizeof(float)*d*capacity);
}


void IndexFlatDisk::add (idx_t /*n*/, const float */*x*/) {
    FAISS_THROW_MSG ("add not implemented for this type of index");
}

void IndexFlatDisk::add_with_ids (idx_t n, const float * x, const idx_t *xids) {
    reserve(n);
    pthread_rwlock_wrlock(&rwlock);
    memcpy((uint8_t *)xb+sizeof(float)*d*ntotal, x, sizeof(float)*d*n);
    memcpy((uint8_t *)ids+sizeof(idx_t)*ntotal, xids, sizeof(idx_t)*n);
    ntotal += n;
    *p_ntotal = ntotal;
    msync(ptr, totsize, MS_SYNC);
    pthread_rwlock_unlock(&rwlock);
}


void IndexFlatDisk::reset() {
    pthread_rwlock_wrlock(&rwlock);
    if (ntotal != 0) {
        ntotal = 0;
        *p_ntotal = ntotal;
        msync(p_ntotal, sizeof(idx_t), MS_SYNC);
    }
    pthread_rwlock_unlock(&rwlock);
}


void IndexFlatDisk::reserve(size_t n) {
    pthread_rwlock_wrlock(&rwlock);
    FAISS_THROW_IF_NOT_FMT(ptr != nullptr, "inconsistent state, ptr nullptr, ntotal %ld, filename %s", ntotal, filename.c_str());
    if (ntotal + n > capacity) {
        munmap(ptr, totsize);
        size_t xb_off = header_size() + sizeof(capacity);
        size_t xb_cap = sizeof(float)*d*capacity;
        size_t xb_ids_cap = sizeof(float)*d*capacity + sizeof(idx_t)*capacity;
        totsize = xb_off + 2 * xb_ids_cap;
        int rc = truncate(filename.c_str(), totsize);
        FAISS_THROW_IF_NOT_FMT(rc == 0, "could not truncate %s: %s", filename.c_str(), strerror(errno));
        int fd = open(filename.c_str(), O_RDWR);
        FAISS_THROW_IF_NOT_FMT(fd >= 0, "could not open %s: %s", filename.c_str(), strerror(errno));
        ptr = (uint8_t*)mmap (nullptr, totsize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
        FAISS_THROW_IF_NOT_FMT(ptr != MAP_FAILED, "could not mmap %s: %s", filename.c_str(), strerror(errno));
        close(fd);
        memcpy(ptr + xb_off + 2 * xb_cap, ptr + xb_off + xb_cap, sizeof(idx_t)*ntotal);
        capacity *= 2;
        *(size_t *)(ptr + header_size()) = capacity;
        msync(ptr, totsize, MS_SYNC);
        p_ntotal = (idx_t *)(ptr + 4 + sizeof(d));
        xb = (float *)(ptr + header_size() + sizeof(capacity));
        ids = (idx_t *)(ptr + header_size() + sizeof(capacity) + sizeof(float)*d*capacity);
    }
    pthread_rwlock_unlock(&rwlock);
}


void IndexFlatDisk::search (idx_t n, const float *x, idx_t k,
                               float *distances, idx_t *labels) const
{
    search(n, x, k, true, nullptr, distances, labels);
}


void IndexFlatDisk::search (idx_t n, const float *x, idx_t k, bool top_vectors, roaring_bitmap_t ** rbs,
                               float *distances, idx_t *labels) const
{
    // we see the distances and labels as heaps
    pthread_rwlock_rdlock(&rwlock);
    if (metric_type == METRIC_INNER_PRODUCT) {
        float_minheap_array_t res = {
            size_t(n), size_t(k), labels, distances};
        knn_inner_product (x, xb, (int64_t *)ids, d, n, ntotal, top_vectors, rbs, &res);
    } else if (metric_type == METRIC_L2) {
        float_maxheap_array_t res = {
            size_t(n), size_t(k), labels, distances};
        knn_L2sqr (x, xb, (int64_t *)ids, d, n, ntotal, top_vectors, rbs, &res);
    } else {
        float_maxheap_array_t res = {
            size_t(n), size_t(k), labels, distances};
        knn_extra_metrics (x, xb, (int64_t *)ids, d, n, ntotal,
                           metric_type, metric_arg, top_vectors, rbs,
                           &res);
    }
    pthread_rwlock_unlock(&rwlock);
}


void IndexFlatDisk::range_search (idx_t n, const float *x, float radius,
                              RangeSearchResult *result) const
{
    pthread_rwlock_rdlock(&rwlock);
    switch (metric_type) {
    case METRIC_INNER_PRODUCT:
        range_search_inner_product (x, xb, (int64_t *)ids, d, n, ntotal,
                                    radius, result);
        break;
    case METRIC_L2:
        range_search_L2sqr (x, xb, (int64_t *)ids, d, n, ntotal, radius, result);
        break;
    default:
        FAISS_THROW_MSG("metric type not supported");
    }
    pthread_rwlock_unlock(&rwlock);
}


void IndexFlatDisk::compute_distance_subset (
            idx_t n,
            const float *x,
            idx_t k,
            float *distances,
            const idx_t *labels) const
{
    pthread_rwlock_rdlock(&rwlock);
    switch (metric_type) {
        case METRIC_INNER_PRODUCT:
            fvec_inner_products_by_idx (
                 distances,
                 x, xb, labels, d, n, k);
            break;
        case METRIC_L2:
            fvec_L2sqr_by_idx (
                 distances,
                 x, xb, labels, d, n, k);
            break;
        default:
            FAISS_THROW_MSG("metric type not supported");
    }
    pthread_rwlock_unlock(&rwlock);
}


size_t IndexFlatDisk::remove_ids (const IDSelector & sel)
{
    pthread_rwlock_wrlock(&rwlock);
    idx_t j = 0;
    for (idx_t i = 0; i < ntotal; i++) {
        if (!sel.is_member(ids[i])) {
            if (i > j) {
                memmove (&xb[d * j], &xb[d * i], sizeof(xb[0]) * d);
                ids[j] = ids[i];
            }
            j++;
        }
    }
    size_t nremove = ntotal - j;
    ntotal -= j;
    *p_ntotal = ntotal;
    msync(ptr, totsize, MS_SYNC);
    pthread_rwlock_unlock(&rwlock);
    return nremove;
}


void IndexFlatDisk::reconstruct (idx_t /*key*/, float * /*recons*/) const
{
    FAISS_THROW_MSG ("reconstruct not implemented for this type of index");
}


DistanceComputer * IndexFlatDisk::get_distance_computer() const {
    FAISS_THROW_MSG ("get_distance_computer() not implemented");
    return nullptr;
}


/* The standalone codec interface */
size_t IndexFlatDisk::sa_code_size () const
{
    return sizeof(float) * d;
}


void IndexFlatDisk::sa_encode (idx_t n, const float *x, uint8_t *bytes) const
{
    memcpy (bytes, x, sizeof(float) * d * n);
}


void IndexFlatDisk::sa_decode (idx_t n, const uint8_t *bytes, float *x) const
{
    memcpy (x, bytes, sizeof(float) * d * n);
}

IndexFlatDisk::~IndexFlatDisk ()
{
    if (ptr != nullptr) {
        munmap(ptr, totsize);
        ptr = nullptr;
        xb = nullptr;
        ids = nullptr;
    }
}

} // namespace faiss
