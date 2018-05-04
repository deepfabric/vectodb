#include "vectodb.hpp"
#include "vectodb.h"

#include "faiss/AutoTune.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexHNSW.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/index_io.h"

#include <boost/filesystem.hpp>
#include <boost/system/system_error.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <pthread.h>
#include <sstream>
#include <stdio.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <system_error>
#include <unordered_map>
#include <vector>

using namespace std;
namespace fs = boost::filesystem;
using mtxlock = unique_lock<mutex>;
using rlock = unique_lock<boost::shared_mutex>;
using wlock = boost::shared_lock<boost::shared_mutex>;

const long MIN_NTRAIN = 10000L;
const long MAX_NTRAIN = 160000L; //the number of training points which IVF4096 needs for 1M dataset

struct DbState {
    DbState()
        : data(nullptr)
        , len_data(0)
        , ntrain(0L)
        , index(nullptr)
        , flat(nullptr)
        , total(0)
    {
    }
    ~DbState()
    {
        delete index;
        fs_base.close();
    }

    // Main activities in decreasing priority: insert, search, build and activate index.
    // Normally index is large, the read-lock time is long(~26s for 1M sift vectors),
    // the write-lock just protects a pointer assignment.
    // So it's better to use an Go rwlock as an coarse method-level lock.
    uint8_t* data; //mapped (readonly) base file. remap after activating an index
    long len_data; //length of mapped file
    long ntrain; // the number of training points of the index
    faiss::Index* index;

    // Normally flat is small, the read-lock time is short(40ms for 1K sift vectors),
    // the write-lock is also short(insertion speed is ~1M sift vectors/second).
    // So it's better to use C++ rwlock.
    boost::shared_mutex rw_flat;
    faiss::Index* flat;

    mutex m_base;
    std::fstream fs_base;
    vector<float> base; //vectors not in index + flat

    boost::shared_mutex rw_xids;
    unordered_map<long, long> xid2num;
    vector<long> xids;

    atomic<long> total;
};

VectoDB::VectoDB(const char* work_dir_in, long dim_in, int metric_type_in, const char* index_key_in, const char* query_params_in)
    : work_dir(work_dir_in)
    , dim(dim_in)
    , len_line(sizeof(long) + dim * sizeof(float))
    , metric_type(metric_type_in)
    , index_key(index_key_in)
    , query_params(query_params_in)
{
    static_assert(sizeof(float) == 4, "sizeof(float) must be 4");
    static_assert(sizeof(long) > 4, "sizeof(long) must be larger than 4");

    fs::path dir{ fs::absolute(work_dir_in) };
    work_dir = dir.string().c_str();

    auto st{ std::make_unique<DbState>() }; //Make DbState be exception safe
    state = std::move(st); // equivalent to state.reset(st.release());
    fs::create_directories(dir);
    //filename spec: base.fvecs, <index_key>.<ntrain>.index
    //line spec of base.fvecs: <xid> {<dim>}<float>
    const string fp_base = getBaseFp();
    //Loading database
    //https://stackoverflow.com/questions/31483349/how-can-i-open-a-file-for-reading-writing-creating-it-if-it-does-not-exist-w
    state->fs_base.exceptions(std::ios::failbit | std::ios::badbit);
    state->fs_base.open(fp_base, std::fstream::out | std::fstream::app); //create file if not exist, otherwise do nothing
    state->fs_base.close();
    state->fs_base.open(fp_base, std::fstream::in | std::fstream::out | std::fstream::binary);
    state->fs_base.seekp(0, ios_base::end); //a particular libstdc++ implementation may use a single pointer for both seekg and seekp.

    long ntrain = getIndexFpNtrain();
    if (ntrain > 0) {
        //Loading index
        const string& fp_index = getIndexFp(ntrain);
        LOG(INFO) << "Loading index " << fp_index;
        state->index = faiss::read_index(fp_index.c_str());
        state->ntrain = ntrain;
    }
    mmapFile(getBaseFp(), state->data, state->len_data);
    buildFlat();

    vector<long> xids;
    readXids(state->data, state->len_data, 0, xids);
    for (long i = 0; i < (long)xids.size(); i++) {
        state->xid2num[xids[i]] = i;
    }
    state->xids = std::move(xids);

    state->total = state->len_data / len_line;
    google::FlushLogFiles(google::INFO);
}

VectoDB::~VectoDB()
{
    munmapFile(getBaseFp(), state->data, state->len_data);
}

void VectoDB::BuildIndex(long cur_ntrain, long cur_nsize, faiss::Index*& index_out, long& ntrain) const
{
    index_out = nullptr;
    ntrain = 0;
    if (0 == index_key.compare("Flat")) {
        return;
    }

    uint8_t* data = nullptr;
    long len_data = 0;
    mmapFile(getBaseFp(), data, len_data);
    long nb = len_data / len_line;
    faiss::Index* index = nullptr;
    long nt = 0;

    // Prepareing index
    LOG(INFO) << "BuildIndex " << work_dir << ". dim=" << dim << ", index_key=\"" << index_key << "\", metric=" << metric_type << ", nb=" << nb;
    if (nb < MIN_NTRAIN)
        goto quit;

    nt = std::min(nb, std::max(nb / 10, MAX_NTRAIN));
    if (nt == cur_ntrain) {
        long& index_size = cur_nsize;
        if (nb == index_size) {
            LOG(INFO) << "Nothing to do since ntrain " << nt << " and index_size " << index_size << " are unchanged";
            index_out = nullptr;
        } else {
            LOG(INFO) << "Reuse current index since ntrain " << nt << " is unchanged. index_size will increase from " << index_size << " to " << nb;
            index = faiss::read_index(getIndexFp(nt).c_str());
            vector<float> base2;
            readBase(data, len_data, index_size, base2);
            index->add(nb - index_size, &base2[0]);
            index_out = index;
        }
    } else {
        LOG(INFO) << "Training on " << nt << " vectors. cur_ntrain is " << cur_ntrain;
        index = faiss::index_factory(dim, index_key.c_str(), metric_type == 0 ? faiss::METRIC_INNER_PRODUCT : faiss::METRIC_L2);
        // according to faiss/benchs/bench_hnsw.py, ivf_hnsw_quantizer.
        auto index_ivf = dynamic_cast<faiss::IndexIVFFlat*>(index);
        if (index_ivf != nullptr) {
            index_ivf->cp.min_points_per_centroid = 5; //quiet warning
            index_ivf->quantizer_trains_alone = 2;
        }
        // Training
        vector<float> base;
        readBase(data, len_data, 0, base);
        assert((long)base.size() >= nt * dim);
        index->train(nt, &base[0]);

        // selected_params is cached auto-tuning result.
        faiss::ParameterSpace params;
        params.initialize(index);
        params.set_index_parameters(index, query_params.c_str());

        // Indexing database
        LOG(INFO) << "Indexing " << nb << " vectors";
        index->add(nb, &base[0]);
        index_out = index;
    }
quit:
    ntrain = nt;
    munmapFile(getBaseFp(), data, len_data);
    LOG(INFO) << "BuildIndex " << work_dir << " done";
    google::FlushLogFiles(google::INFO);
}

// Needs Go write-lock
void VectoDB::ActivateIndex(faiss::Index* index, long ntrain)
{
    if (index != nullptr) {
        if (state->ntrain != 0)
            fs::remove(getIndexFp(state->ntrain));
        // Output index
        faiss::write_index(index, getIndexFp(ntrain).c_str());
        delete state->index;
        state->ntrain = ntrain;
        state->index = index;
    }
    mmapFile(getBaseFp(), state->data, state->len_data);
    buildFlat();
}

// Needs Go write-lock
void VectoDB::buildFlat()
{
    faiss::Index* flat = new faiss::IndexFlat(dim, metric_type == 0 ? faiss::METRIC_INNER_PRODUCT : faiss::METRIC_L2);
    vector<float> base;
    long index_size = (state->index == nullptr) ? 0 : state->index->ntotal;
    readBase(state->data, state->len_data, index_size, base);
    flat->add(base.size() / dim, &base[0]);

    wlock l{ state->rw_flat };
    delete state->flat;
    state->flat = flat;
}

// Needs Go read-lock
void VectoDB::GetIndexSize(long& ntrain, long& nsize) const
{
    ntrain = state->ntrain;
    nsize = (state->index == nullptr) ? 0 : state->index->ntotal;
}

long VectoDB::GetTotal()
{
    return state->total;
}

long VectoDB::GetFlatSize()
{
    mergeToFlat();
    rlock l{ state->rw_flat };
    long nflat = state->flat->ntotal;
    return nflat;
}

void VectoDB::AddWithIds(long nb, const float* xb, const long* xids)
{
    long len_buf = nb * len_line;
    std::vector<char> buf(len_buf);
    for (long i = 0; i < nb; i++) {
        memcpy(&buf[i * len_line], &xids[i], sizeof(long));
        memcpy(&buf[i * len_line + sizeof(long)], &xb[i * dim], dim * sizeof(float));
    }
    mtxlock m{ state->m_base };
    // deduplicate xids
    {
        rlock r{ state->rw_xids };
        if (state->xid2num.count(xids[0]) > 0)
            return;
    }
    long ntotal = state->total.fetch_add(nb);
    state->fs_base.write(&buf[0], len_buf);
    for (long i = 0; i < nb * dim; i++) {
        state->base.push_back(xb[i]);
    }
    wlock w{ state->rw_xids };
    for (long i = 0; i < nb; i++) {
        state->xids.push_back(xids[i]);
        state->xid2num[xids[i]] = ntotal + i;
    }
}

void VectoDB::UpdateWithIds(long nb, const float* xb, const long* xids)
{
    mtxlock m{ state->m_base };
    {
        rlock r{ state->rw_xids };
        auto end = state->xid2num.end();
        long len_vec = len_line - sizeof(long);
        for (long i = 0; i < nb; i++) {
            auto it = state->xid2num.find(xids[i]);
            if (it == end)
                continue;
            long line_num = it->second;
            // The experiment indicates that the readers of mmaped file are not ware to following changes untill they be flushed.
            // TODO: Is it possible that readers get the middle state of a change?
            state->fs_base.seekp(line_num * len_line + sizeof(long), ios_base::beg);
            state->fs_base.write((const char*)&xb[i * dim], len_vec);
        }
    }
    state->fs_base.seekp(0, ios_base::end);
}

void VectoDB::mergeToFlat()
{
    mtxlock m{ state->m_base };
    long memSize = state->base.size() / dim;
    if (memSize != 0) {
        wlock l{ state->rw_flat };
        state->flat->add(memSize, &state->base[0]);
        state->base.clear();
    }
}

// Needs Go read-lock
long VectoDB::Search(long nq, const float* xq, float* distances, long* xids)
{
    long total = state->total;
    if (total <= 0)
        return total;
    // output buffers
    const long k = 100;
    vector<float> D(nq * k);
    vector<faiss::Index::idx_t> I(nq * k);

    float xb2[dim * k];
    float D2[k];
    faiss::Index::idx_t I2[k];

    if (state->index) {
        // Perform a search
        state->index->search(nq, xq, k, &D[0], &I[0]);

        // Refine result
        faiss::Index* index2 = new faiss::IndexFlat(dim, metric_type == 0 ? faiss::METRIC_INNER_PRODUCT : faiss::METRIC_L2);
        for (int i = 0; i < nq; i++) {
            for (int j = 0; j < k; j++) {
                long line_num = I[i * k + j];
                memcpy(xb2 + j * dim, &state->data[len_line * line_num + sizeof(long)], sizeof(float) * dim);
            }
            index2->add(k, xb2);
            index2->search(1, xq + i * dim, k, D2, I2);
            index2->reset();
            distances[i] = D2[0];
            xids[i] = I[i * k + I2[0]];
        }
        delete index2;
    }
    long index_size = (state->index == nullptr) ? 0 : state->index->ntotal;

    mergeToFlat();

    {
        rlock r{ state->rw_flat };
        if (state->flat->ntotal != 0) {
            state->flat->search(nq, xq, k, &D[0], &I[0]);
            for (int i = 0; i < nq; i++) {
                if (0 == index_size || distances[i] > D[i * k]) {
                    distances[i] = D[i * k];
                    xids[i] = I[i * k];
                }
            }
        }
    }

    {
        rlock r{ state->rw_xids };
        for (int i = 0; i < nq; i++) {
            xids[i] = state->xids[xids[i]];
        }
    }
    return total;
}

std::string VectoDB::getBaseFp() const
{
    ostringstream oss;
    oss << work_dir << "/base.fvecs";
    return oss.str();
}

std::string VectoDB::getIndexFp(long ntrain) const
{
    ostringstream oss;
    oss << work_dir << "/" << index_key << "." << ntrain << ".index";
    return oss.str();
}

long VectoDB::getIndexFpNtrain() const
{
    long max_ntrain = 0;
    fs::path fp_index;
    string prefix(index_key);
    prefix.append(".");
    const string suffix(".index");
    for (auto ent = fs::directory_iterator(work_dir); ent != fs::directory_iterator(); ent++) {
        const fs::path& p = ent->path();
        if (fs::is_regular_file(p)) {
            const string fn = p.filename().string();
            if (fn.length() >= suffix.length()
                && 0 == fn.compare(fn.length() - suffix.length(), suffix.length(), suffix)
                && 0 == fn.compare(0, prefix.length(), prefix)) {
                long ntrain = std::stol(fn.substr(prefix.length(), fn.length() - prefix.length() - suffix.length()));
                if (ntrain > max_ntrain) {
                    max_ntrain = ntrain;
                    fp_index = p;
                }
            }
        }
    }
    return max_ntrain;
}

void VectoDB::readBase(const uint8_t* data, long len_data, long start_num, vector<float>& base) const
{
    if (data == nullptr)
        return;
    assert(len_data % len_line == 0);
    long num_line = len_data / len_line;
    if (num_line <= start_num)
        return;
    long nb = num_line - start_num;
    base.resize(nb * dim);
    for (long i = 0; i < nb; i++) {
        const uint8_t* start_pos = data + (i + start_num) * len_line;
        memcpy(&base[i * dim], (float*)(start_pos + sizeof(long)), sizeof(float) * dim);
    }
}

void VectoDB::readXids(const uint8_t* data, long len_data, long start_num, vector<long>& xids) const
{
    if (data == nullptr)
        return;
    assert(len_data % len_line == 0);
    long num_line = len_data / len_line;
    if (num_line <= start_num)
        return;
    long nb = num_line - start_num;
    xids.resize(nb);
    for (long i = 0; i < nb; i++) {
        const uint8_t* start_pos = data + (i + start_num) * len_line;
        xids[i] = *(long*)start_pos;
    }
}

void VectoDB::ClearWorkDir(const char* work_dir)
{
    ostringstream oss;
    oss << work_dir << "/base.fvecs";
    fs::remove(oss.str());

    const string suffix(".index");
    for (auto ent = fs::directory_iterator(work_dir); ent != fs::directory_iterator(); ent++) {
        const fs::path& p = ent->path();
        if (fs::is_regular_file(p)) {
            const string fn = p.filename().string();
            if (fn.length() >= suffix.length()
                && 0 == fn.compare(fn.length() - suffix.length(), suffix.length(), suffix)) {
                fs::remove(p);
            }
        }
    }
}

void VectoDB::mmapFile(const string& fp, uint8_t*& data, long& len_data)
{
    munmapFile(fp, data, len_data);
    long len_f = fs::file_size(fp); //equivalent to "fs_base.seekp(0, ios_base::end); long len_f = fs_base.tellp();"
    if (len_f == 0)
        return;
    int f = open(fp.c_str(), O_RDONLY);
    void* tmpd = mmap(NULL, len_f, PROT_READ, MAP_SHARED, f, 0);
    if (tmpd == MAP_FAILED)
        throw fs::filesystem_error(fp, boost::system::error_code(errno, boost::system::generic_category()));
    close(f);
    data = (uint8_t*)tmpd;
    len_data = len_f;
}

void VectoDB::munmapFile(const string& fp, uint8_t*& data, long& len_data)
{
    if (data != nullptr) {
        int rc = munmap(data, len_data);
        if (rc < 0)
            throw fs::filesystem_error(fp, boost::system::error_code(errno, boost::system::generic_category()));
        data = nullptr;
        len_data = 0;
    }
}

/**
 * C wrappers.
 */

void* VectodbNew(char* work_dir, long dim, int metric_type, char* index_key, char* query_params)
{
    VectoDB* vdb = new VectoDB(work_dir, dim, metric_type, index_key, query_params);
    return vdb;
}

void VectodbDelete(void* vdb)
{
    delete static_cast<VectoDB*>(vdb);
}

void* VectodbBuildIndex(void* vdb, long cur_ntrain, long cur_nsize, long* ntrain)
{
    faiss::Index* index = nullptr;
    static_cast<VectoDB*>(vdb)->BuildIndex(cur_ntrain, cur_nsize, index, *ntrain);
    return index;
}

void VectodbAddWithIds(void* vdb, long nb, float* xb, long* xids)
{
    static_cast<VectoDB*>(vdb)->AddWithIds(nb, xb, xids);
}

void VectodbUpdateWithIds(void* vdb, long nb, float* xb, long* xids)
{
    static_cast<VectoDB*>(vdb)->UpdateWithIds(nb, xb, xids);
}

long VectodbGetTotal(void* vdb)
{
    return static_cast<VectoDB*>(vdb)->GetTotal();
}

long VectodbGetFlatSize(void* vdb)
{
    return static_cast<VectoDB*>(vdb)->GetFlatSize();
}

void VectodbActivateIndex(void* vdb, void* index, long ntrain)
{
    static_cast<VectoDB*>(vdb)->ActivateIndex(static_cast<faiss::Index*>(index), ntrain);
}

void VectodbGetIndexSize(void* vdb, long* ntrain, long* ntotal)
{
    static_cast<VectoDB*>(vdb)->GetIndexSize(*ntrain, *ntotal);
}

long VectodbSearch(void* vdb, long nq, float* xq, float* distances, long* xids)
{
    return static_cast<VectoDB*>(vdb)->Search(nq, xq, distances, xids);
}

void VectodbClearWorkDir(char* work_dir)
{
    VectoDB::ClearWorkDir(work_dir);
}
