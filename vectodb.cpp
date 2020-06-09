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
#include <math.h>
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
        , total(0)
        , ntrain(0L)
        , index(nullptr)
        , flat(nullptr)
        , flat_start_num(0)
    {
    }
    ~DbState()
    {
    }

    mutex m_base;
    std::fstream fs_base; //for append of base.fvecs

    boost::shared_mutex rw_data;
    uint8_t* data; //mapped (readonly) base file. remap after activating an index
    long len_data; //length of mapped file, be equivlant to index.ntotal*len_base_line
    atomic<long> total;

    // Main activities in decreasing priority: insert, search, build and activate index.
    // Normally index is large, the read-lock (search) time is long(~26s for 10K searchs of sift),
    // the write-lock (activate index) just protects a pointer assignment.
    boost::shared_mutex rw_index;
    long ntrain; // the number of training points of the index
    faiss::Index* index;

    // Normally flat is small, the read-lock time is short(40ms for 1K sift vectors),
    // the write-lock is also short(insertion speed is ~1M sift vectors/second).
    // So it's better to use C++ rwlock.
    boost::shared_mutex rw_flat;
    faiss::Index* flat;
    long flat_start_num; //the line num of the first vecrot of flat. It's index->ntotal normally.

    boost::shared_mutex rw_xids;
    vector<long> xids; //vector of xid of all vectors

    mutex m_update;
    std::fstream fs_update; //for append, sequential read and truncate of update.fvecs

    mutex m_base2;
    std::fstream fs_base2; //for random write of base.fvecs
};

struct VecExt {
    long count;
    vector<float> vec;
};

VectoDB::VectoDB(const char* work_dir_in, long dim_in, int metric_type_in, const char* index_key_in, const char* query_params_in, float dist_threshold_in)
    : work_dir(work_dir_in)
    , dim(dim_in)
    , len_vec(dim * sizeof(float))
    , len_base_line(2 * sizeof(long) + len_vec)
    , len_upd_line(sizeof(long) + len_vec)
    , metric_type(metric_type_in)
    , dist_threshold(dist_threshold_in)
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
    //line spec of base.fvecs: <xid> <count> {<dim>}<float>
    //line spec of update.fvecs: <line_num_at_base> {<dim>}<float>
    const string& fp_base = getBaseFp();
    //Loading database
    //https://stackoverflow.com/questions/31483349/how-can-i-open-a-file-for-reading-writing-creating-it-if-it-does-not-exist-w
    state->fs_base.exceptions(std::ios::failbit | std::ios::badbit);
    state->fs_base.open(fp_base, std::fstream::out | std::fstream::app); //create file if not exist, otherwise do nothing
    state->fs_base.close();
    state->fs_base.open(fp_base, std::fstream::in | std::fstream::out | std::fstream::binary);
    state->fs_base.seekp(0, ios_base::end); //a particular libstdc++ implementation may use a single pointer for both seekg and seekp.

    long ntrain = getIndexFpNtrain();
    faiss::Index* index = nullptr;
    if (ntrain > 0) {
        //Loading index
        const string& fp_index = getIndexFp(ntrain);
        LOG(INFO) << "Loading index " << fp_index;
        index = faiss::read_index(fp_index.c_str());
    }
    ActivateIndex(index, ntrain);

    vector<long> xids;
    readXids(state->data, state->total, 0, xids);
    state->xids = std::move(xids);

    state->fs_base2.exceptions(std::ios::failbit | std::ios::badbit);
    state->fs_base2.open(fp_base, std::fstream::in | std::fstream::out | std::fstream::binary);

    google::FlushLogFiles(google::INFO);
}

VectoDB::~VectoDB()
{
    // There's no lock protection since I assume the object is idle.
    // Up layer could protect it with rwlock.
    if (state.get() != nullptr) {
        munmapFile(getBaseFp(), state->data, state->len_data);
        delete state->index;
        delete state->flat;
    }
}

void VectoDB::BuildIndex(long cur_ntrain, long cur_nsize, faiss::Index*& index_out, long& ntrain) const
{
    index_out = nullptr;
    ntrain = 0;
    if (0 == index_key.compare("Flat")) {
        return;
    }

    const string& fp_base = getBaseFp();
    uint8_t* data = nullptr;
    long len_data = 0;
    mmapFile(fp_base, data, len_data); // this may occur in the middle of wirting to fp_base.
    long nb = getNumLines(len_data, len_base_line);
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
            readBase(data, nb, index_size, base2);
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
        readBase(data, nb, 0, base);
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
    munmapFile(fp_base, data, len_data);
    LOG(INFO) << "BuildIndex " << work_dir << " done";
    google::FlushLogFiles(google::INFO);
}

void VectoDB::ActivateIndex(faiss::Index* index, long ntrain)
{
    const string& fp_base = getBaseFp();
    mtxlock m{ state->m_base };
    state->fs_base.flush();
    {
        wlock w{ state->rw_data };
        mmapFile(fp_base, state->data, state->len_data);
    }
    long nb = getNumLines(state->len_data, len_base_line);
    state->total = nb;

    long index_size = 0;
    if (index != nullptr) {
        clearIndexFiles();
        // Output index
        faiss::write_index(index, getIndexFp(ntrain).c_str());
        index_size = index->ntotal;
    }

    {
        wlock w{ state->rw_index };
        delete state->index;
        state->ntrain = ntrain;
        state->index = index;
    }

    faiss::Index* flat = new faiss::IndexFlat(dim, metric_type == 0 ? faiss::METRIC_INNER_PRODUCT : faiss::METRIC_L2);
    vector<float> base;
    readBase(state->data, nb, index_size, base);
    flat->add(base.size() / dim, &base[0]);

    wlock l{ state->rw_flat };
    delete state->flat;
    state->flat = flat;
    state->flat_start_num = index_size;
}

void VectoDB::GetIndexSize(long& ntrain, long& nsize) const
{
    rlock r{ state->rw_index };
    if (state->index == nullptr) {
        ntrain = 0;
        nsize = 0;
    } else {
        ntrain = state->ntrain;
        nsize = state->index->ntotal;
    }
}

long VectoDB::GetTotal()
{
    rlock l{ state->rw_flat };
    return state->total;
}

long VectoDB::GetFlatSize()
{
    rlock l{ state->rw_flat };
    long nflat = state->flat->ntotal;
    return nflat;
}

void VectoDB::AddWithIds(long nb, const float* xb, const long* xids)
{
    long len_buf = nb * len_base_line;
    std::vector<char> buf(len_buf);
    for (long i = 0; i < nb; i++) {
        *(long*)&buf[i * len_base_line] = xids[i];
        *(long*)&buf[i * len_base_line + sizeof(long)] = 1;
        memcpy(&buf[i * len_base_line + 2 * sizeof(long)], &xb[i * dim], len_vec);
    }
    mtxlock m{ state->m_base };
    state->fs_base.write(&buf[0], len_buf);
    long ntotal = state->total.fetch_add(nb);
    {
        wlock w1{ state->rw_flat };
        wlock w2{ state->rw_xids };
        state->flat->add(nb, xb);
        for (long i = 0; i < nb; i++) {
            state->xids.push_back(xids[i]);
        }
    }
}

long VectoDB::Search(long nq, long k, const float* xq, float* distances, long* xids)
{
    for (int i = 0; i < nq; i++) {
        xids[i] = long(-1);
    }
    long total = state->total;
    if (total <= 0)
        return total;
    /*
    // refers to https://blog.csdn.net/quyuan2009/article/details/50001679
    */

    long index_size = 0;
    bool searched_pq = false;
    bool searched_flat = false;
    vector<float> D(nq * k, -1.0);
    vector<faiss::Index::idx_t> I(nq * k, -1L);
    vector<float> D2(nq * k, -1.0);
    vector<faiss::Index::idx_t> I2(nq * k, -1L);
    {
        rlock r{ state->rw_index };
        if (state->index != nullptr) {
            index_size = state->index->ntotal;
            // Perform a search
            state->index->search(nq, xq, k, &D[0], &I[0]);
            searched_pq = true;
        }
    }

    {
        rlock r{ state->rw_flat };
        if (state->flat->ntotal != 0) {
            // Perform a search
            state->flat->search(nq, xq, k, &D2[0], &I2[0]);
            searched_flat = true;
        }
    }
    if (searched_pq && searched_flat) {
        for (int i = 0; i < nq; i++) {
            // merge D[i*k..(i+1)*k] and D2[i*k..(i+1)*k]
            int i1 = 0, i2 = 0;
            for (int j = 0; j < k; j++) {
                if (CompareDistance(metric_type, D[i * k + i1], D2[i * k + i2])) {
                    distances[i] = D[i * k + i1];
                    xids[i] = I[i * k + i1];
                    i1++;
                } else {
                    distances[i] = D2[i * k + i2];
                    xids[i] = I2[i * k + i2];
                    i2++;
                }
            }
        }
    } else if (searched_pq) {
        for (int i = 0; i < nq * k; i++) {
            distances[i] = D[i];
            xids[i] = I[i];
        }
    } else if (searched_flat) {
        for (int i = 0; i < nq * k; i++) {
            distances[i] = D2[i];
            xids[i] = I2[i];
        }
    }

    {
        rlock r{ state->rw_xids };
        for (int i = 0; i < nq; i++) {
            if (CompareDistance(metric_type, distances[i], dist_threshold)) {
                xids[i] = state->xids[xids[i]];
            } else {
                xids[i] = long(-1);
            }
        }
        //printf("\nmetric_type=%d, dist_threshold=%f\n", metric_type, dist_threshold);
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

void VectoDB::clearIndexFiles()
{
    fs::path fp_index;
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

void VectoDB::readBase(const uint8_t* data, long num_line, long start_num, vector<float>& base) const
{
    if (data == nullptr || num_line <= start_num)
        return;
    long nb = num_line - start_num;
    base.resize(nb * dim);
    for (long i = 0; i < nb; i++) {
        const uint8_t* start_pos = data + (i + start_num) * len_base_line;
        memcpy(&base[i * dim], (float*)(start_pos + 2 * sizeof(long)), len_vec);
    }
}

void VectoDB::readXids(const uint8_t* data, long num_line, long start_num, vector<long>& xids) const
{
    if (data == nullptr || num_line <= start_num)
        return;
    long nb = num_line - start_num;
    xids.resize(nb);
    for (long i = 0; i < nb; i++) {
        const uint8_t* start_pos = data + (i + start_num) * len_base_line;
        xids[i] = *(long*)start_pos;
    }
}

void VectoDB::ClearWorkDir(const char* work_dir)
{
    fs::create_directories(work_dir);
    ostringstream oss;
    oss << work_dir << "/base.fvecs";
    fs::remove(oss.str());
    oss.clear();
    oss << work_dir << "/update.fvecs";
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

void VectoDB::Normalize(std::vector<float>& vec)
{
    double l = 0;
    int dim = vec.size();
    for (int i = 0; i < dim; i++) {
        l += double(vec[i]) * double(vec[i]);
    }
    l = sqrt(l);
    for (int i = 0; i < dim; i++) {
        vec[i] = (float)(((double)vec[i]) / l);
    }
}

long VectoDB::getNumLines(long len_data, long len_base_line) const
{
    long nb = len_data / len_base_line;
    long remained = len_data - len_base_line * nb;
    if (remained != 0) {
        LOG(ERROR) << "file fize could end with partial line. file size " << len_data << ", len_base_line " << len_base_line << ", nb " << nb << ", remained " << remained << ".";
    }
    return nb;
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
    int rc = madvise(tmpd, len_f, MADV_RANDOM | MADV_DONTDUMP);
    if (rc < 0)
        LOG(ERROR) << "madvise failed with " << strerror(errno);
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

void* VectodbNew(char* work_dir, long dim, int metric_type, char* index_key, char* query_params, float dist_threshold)
{
    VectoDB* vdb = new VectoDB(work_dir, dim, metric_type, index_key, query_params, dist_threshold);
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
