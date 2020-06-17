#include "vectodb.hpp"
#include "vectodb.h"

#include "faiss/AutoTune.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexHNSW.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/index_io.h"
#include "faiss/index_factory.h"

#include <filesystem>
#include <system_error>
#include <shared_mutex>
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
#include <regex>

using namespace std;
namespace fs = std::filesystem;
using mtxlock = unique_lock<mutex>;
using rlock = unique_lock<shared_mutex>;
using wlock = shared_lock<shared_mutex>;

//the number of training points which IVF4096 needs for 1M dataset
const long DESIRED_NTRAIN = 200000L;
const long ALLOW_ADD_GAP  = 10000L;

struct DbState {
    DbState()
        : data_mut(nullptr)
        , refMutation(0L)
        , refNtrain(0L)
        , refFlat(nullptr)
        , initFlat(nullptr)
    {
    }
    ~DbState()
    {
    }

    mutex m_sync;
    mutex m_base;
    std::fstream fs_base_fvecs; //for append of base.fvecs
    std::fstream fs_base_xids; //for append of base.xids
    uint8_t* data_mut; // mapped base.mutation

    // Main activities in decreasing priority: insert, search, build and activate index.
    // Normally index is large, the read-lock (search) time is long(~26s for 10K searchs of sift),
    // the write-lock (activate index) just protects a pointer assignment.
    shared_mutex rw_index;
    long refMutation;
    long refNtrain; // the number of training points of the refFlat->base_index
    faiss::IndexRefineFlat* refFlat; //one of refFlat and initFlat is null
    faiss::IndexFlat* initFlat;
    vector<long> xids; //vector of xid of all vectors
    std::unordered_map<long, long> xid2num;
};

struct VecExt {
    long count;
    vector<float> vec;
};

VectoDB::VectoDB(const char* work_dir_in, long dim_in, const char* index_key_in, const char* query_params_in)
    : work_dir(work_dir_in)
    , dim(dim_in)
    , len_vec(dim * sizeof(float))
    , index_key(index_key_in)
    , query_params(query_params_in)
    , fp_base_xids(getBaseXidsFp())
    , fp_base_fvecs(getBaseFvecsFp())
    , fp_base_mutation(getBaseMutationFp())
    , fp_base_xids_tmp(fp_base_xids + ".tmp")
    , fp_base_fvecs_tmp(fp_base_fvecs + ".tmp")
    , fp_base_mutation_tmp(fp_base_mutation + ".tmp")
{
    static_assert(sizeof(float) == 4, "sizeof(float) must be 4");
    static_assert(sizeof(long) == 2 * sizeof(float), "sizeof(long) must be 8");

    fs::path dir{ fs::absolute(work_dir_in) };
    work_dir = dir.string().c_str();

    auto st{ std::make_unique<DbState>() }; //Make DbState be exception safe
    state = std::move(st); // equivalent to state.reset(st.release());
    state->fs_base_fvecs.exceptions(std::ios::failbit | std::ios::badbit);
    state->fs_base_xids.exceptions(std::ios::failbit | std::ios::badbit);

    fs::create_directories(dir);
    SyncIndex();
    google::FlushLogFiles(google::INFO);
}

VectoDB::~VectoDB()
{
    long len_mut = sizeof(uint64_t);
    munmapFile(fp_base_mutation, state->data_mut, len_mut);
    // There's no lock protection since I assume the object is idle.
    // Up layer could protect it with rwlock.
    if (state.get() != nullptr) {
        delete state->initFlat;
        delete state->refFlat;
    }
}

void VectoDB::AddWithIds(long nb, const float* xb, const long* xids)
{
    mtxlock m{ state->m_base };
    wlock w{ state->rw_index };
    state->fs_base_fvecs.write((const char*)xb, len_vec*nb);
    state->fs_base_xids.write((const char*)xids, sizeof(long)*nb);
    state->fs_base_fvecs.flush();
    state->fs_base_xids.flush();

    long cnt_xids = state->xids.size();
    for (long i = 0; i < nb; i++) {
        state->xids.push_back(xids[i]);
        state->xid2num[xids[i]] = cnt_xids + i;
    }
    if (state->initFlat)
        state->initFlat->add(nb, xb);
    else
        state->refFlat->add(nb, xb);
}

void VectoDB::RemoveIds(long nb, const long* xids)
{
    mtxlock m{ state->m_base };
    wlock w{ state->rw_index };
    bool seeked = false;
    for(long i=0; i<nb; i++){
        long xid = xids[i];
        auto it = state->xid2num.find(xid);
        if(it==state->xid2num.cend())
            continue;
        long num = it->second;
        state->xids[num] = -1L;
        state->xid2num.erase(it);
        xid = -1L;
        state->fs_base_xids.seekp(num, ios_base::beg);
        state->fs_base_xids.write((const char*)&xid, sizeof(long));
        seeked = true;
    }
    if(seeked) {
        state->fs_base_xids.seekp(0, ios_base::end);
        state->fs_base_xids.flush();
    }
    incBaseMutation();
}

void VectoDB::SyncIndex()
{
    mtxlock ms{ state->m_sync };
    {
        mtxlock m{ state->m_base };
        if(state->xids.size() < DESIRED_NTRAIN){
            LOG(INFO) << "Skipped sync since number of vectors " << state->xids.size() << " is less than " << DESIRED_NTRAIN;
            return;
        }
        if (state->refFlat != nullptr && state->refMutation==getBaseMutation()){
            if ((long)state->xids.size() < state->refFlat->base_index->ntotal + ALLOW_ADD_GAP){
                LOG(INFO) << "Skipped sync since gap is too little";
                return;
            }
            clearIndexFiles();
            // Output index
            const string& fp_index = getIndexFp(state->refMutation, state->refNtrain);
            faiss::write_index(state->refFlat->base_index, fp_index.c_str());
            LOG(INFO) << "Dumped index to " << fp_index;
            return;
        }
        if (!fs::is_regular_file(fp_base_xids) || !fs::is_regular_file(fp_base_fvecs) || !fs::is_regular_file(fp_base_mutation)) {
            std::ofstream out1(fp_base_xids);
            std::ofstream out2(fp_base_fvecs);
            std::ofstream out3(fp_base_mutation);
            if (!fs::is_regular_file(fp_base_mutation) || fs::file_size(fp_base_mutation)!=8) {
                std::ofstream ofs;
                ofs.open(fp_base_mutation, std::fstream::out);
                ofs << "00000000";
                ofs.close();
                fs::resize_file(fp_base_mutation, 8);
            }
            LOG(INFO) << "Initialized " << fp_base_xids << ", " << fp_base_fvecs << ", " << fp_base_mutation;
        }
        fs::copy(fp_base_xids, fp_base_xids_tmp);
        fs::copy(fp_base_fvecs, fp_base_fvecs_tmp);
        fs::copy(fp_base_mutation, fp_base_mutation_tmp);
        LOG(INFO) << "Created temp files " << fp_base_xids_tmp << ", " << fp_base_fvecs_tmp << ", " << fp_base_mutation_tmp;
    }
    long mutation = 0;
    uint8_t *data_mut;
    long len_mut = sizeof(uint64_t);
    mmapFile(fp_base_mutation_tmp, data_mut, len_mut);
    mutation = *(int64_t*)data_mut;
    munmapFile(fp_base_mutation_tmp, data_mut, len_mut);

    long orig_cnt_xids, cnt_xids;
    uint8_t *data_xids, *data_fvecs;
    long len_xids, len_fvecs;
    mmapFile(fp_base_xids_tmp, data_xids, len_xids);
    mmapFile(fp_base_fvecs_tmp, data_fvecs, len_fvecs);
    assert(len_xids/(long)sizeof(long) == len_fvecs/len_vec);
    orig_cnt_xids = cnt_xids = len_xids/sizeof(long);
    long i=0, j=0;
    // locate first i, elements[i]=-1
    for(; i<cnt_xids && *((long*)data_xids+i) != -1L; i++);
    if(i<cnt_xids) {
        j = i+1;
        while(1){
            // locate first j, elements[j]!=-1
            for(; j<cnt_xids && *((long*)data_xids+j) == -1L; j++);
            if(j>=cnt_xids)
                break;
            //move elements[j] to position i
            memmove(data_xids+i*sizeof(long), data_xids+j*sizeof(long), sizeof(long));
            memmove(data_fvecs+i*len_vec, data_fvecs+j*len_vec, len_vec);
            i++;
            j++;
        }
        cnt_xids = i;
        fs::resize_file(fp_base_xids_tmp, cnt_xids*sizeof(long));
        fs::resize_file(fp_base_fvecs_tmp, cnt_xids*len_vec);
        LOG(INFO) << "Compacted temp files " << fp_base_xids_tmp << ", " << fp_base_fvecs_tmp;
    }

    faiss::Index* base_index = nullptr;
    faiss::IndexRefineFlat* refFlat = new faiss::IndexRefineFlat(base_index);
    refFlat->own_fields = true;
    long nt = cnt_xids;
    if(nt>DESIRED_NTRAIN)
        nt = DESIRED_NTRAIN;
    LOG(INFO) << "Training on " << nt << " vectors of " << work_dir;
    base_index = faiss::index_factory(dim, index_key.c_str(), faiss::METRIC_INNER_PRODUCT);
    // according to faiss/benchs/bench_hnsw.py, ivf_hnsw_quantizer.
    auto index_ivf = dynamic_cast<faiss::IndexIVFFlat*>(base_index);
    if (index_ivf != nullptr) {
        index_ivf->cp.min_points_per_centroid = 5; //quiet warning
        index_ivf->quantizer_trains_alone = 2;
    }
    base_index->train(nt, (const float*)data_fvecs);
    faiss::ParameterSpace params;
    params.initialize(base_index);
    params.set_index_parameters(base_index, query_params.c_str());
    LOG(INFO) << "Indexing " << cnt_xids << " vectors of " << work_dir;
    refFlat->add(cnt_xids, (const float*)data_fvecs);
    vector<long> xids(cnt_xids);
    unordered_map<long, long> xid2num;
    memcpy(&xids[0], data_xids, len_xids);
    for(long i=0; i<cnt_xids; i++){
        long& xid = *(long *)(data_xids + i*sizeof(long));
        xid2num[xid] = i;
    }

    {
        wlock w{ state->rw_index };
        if((long)state->xids.size()>orig_cnt_xids){
            long gap = state->xids.size() - orig_cnt_xids;
            LOG(INFO) << "Indexing " << gap << " vectors of " << work_dir;
            for(long i=0, j=0; i<gap; i++){
                long xid = state->xids[orig_cnt_xids+i];
                if(xid==-1L)
                    continue;
                refFlat->add(1, (float *)data_fvecs+dim*(orig_cnt_xids+i));
                xids.push_back(xid);
                xid2num[xid] = cnt_xids + j;
                j++;
            }
        } else if(state->data_mut==nullptr || mutation>=getBaseMutation()) {
            long len_mut = sizeof(uint64_t);
            if(state->data_mut!=nullptr)
                munmapFile(fp_base_mutation, state->data_mut, len_mut);
            fs::remove(fp_base_xids);
            fs::remove(fp_base_fvecs);
            fs::remove(fp_base_mutation);
            fs::create_hard_link(fp_base_xids_tmp, fp_base_xids);
            fs::create_hard_link(fp_base_fvecs_tmp, fp_base_fvecs);
            fs::create_hard_link(fp_base_mutation_tmp, fp_base_mutation);
            //https://stackoverflow.com/questions/31483349/how-can-i-open-a-file-for-reading-writing-creating-it-if-it-does-not-exist-w
            state->fs_base_fvecs.close();
            state->fs_base_fvecs.open(fp_base_fvecs, std::fstream::in | std::fstream::out | std::fstream::binary);
            state->fs_base_fvecs.seekp(0, ios_base::end); //a particular libstdc++ implementation may use a single pointer for both seekg and seekp.
            state->fs_base_xids.close();
            state->fs_base_xids.open(fp_base_xids, std::fstream::in | std::fstream::out | std::fstream::binary);
            state->fs_base_xids.seekp(0, ios_base::end); //a particular libstdc++ implementation may use a single pointer for both seekg and seekp.
            mmapFile(fp_base_mutation, state->data_mut, len_mut);
            LOG(INFO) << "Copyied back temp files " << fp_base_xids_tmp << ", " << fp_base_fvecs_tmp << ", " << fp_base_mutation_tmp;
        }
        if(state->initFlat)
            delete state->initFlat;
        else if(state->refFlat){
            delete state->refFlat;
        }
        state->refMutation = mutation;
        state->refNtrain = nt;
        state->refFlat = refFlat;
        state->initFlat = nullptr;
        state->xids = std::move(xids);
        state->xid2num = std::move(xid2num);
        // Output index
        const string& fp_index = getIndexFp(mutation, nt);
        faiss::write_index(base_index, fp_index.c_str());
        LOG(INFO) << "Dumped index to " << fp_index;
    }

    munmapFile(fp_base_xids, data_xids, len_xids);
    munmapFile(fp_base_fvecs, data_fvecs, len_fvecs);
    fs::remove(fp_base_xids_tmp);
    fs::remove(fp_base_fvecs_tmp);
    fs::remove(fp_base_mutation_tmp);
    LOG(INFO) << "Destroyed temp files " << fp_base_xids_tmp << ", " << fp_base_fvecs_tmp << ", " << fp_base_mutation_tmp;
    LOG(INFO) << "SyncIndex of " << work_dir << " done";
    google::FlushLogFiles(google::INFO);
}

long VectoDB::GetTotal()
{
    rlock l{ state->rw_index };
    return state->xids.size();
}

void VectoDB::Search(long nq, long k, const float* xq, const long* /*uids*/, float* scores, long* xids)
{
    for (int i = 0; i < nq*k; i++) {
        xids[i] = -1L;
        scores[i] = -1.0;
    }
    rlock l{ state->rw_index };
    long total = state->xids.size();
    if (total <= 0)
        return;
    /*
    // refers to https://blog.csdn.net/quyuan2009/article/details/50001679
    */
    if (state->initFlat != nullptr) {
        state->initFlat->search(nq, xq, k, scores, xids);
    } else if(state->refFlat != nullptr) {
        state->refFlat->search(nq, xq, k, scores, xids);
    }
    //translate xids
    for(int i=0; i<nq*k; i++){
        if(xids[i] != -1L){
            long xid = state->xids[xids[i]];
            xids[i] = xid;
        }
    }
    //compact xids for each query
    for(int q=0; q<nq; q++){
        long i=0, j=0;
        // locate first i, elements[i]=-1
        for(; i<k && xids[q*k+i] != -1L; i++);
        if(i<k) {
            j = i+1;
            while(1){
                // locate first j, elements[j]!=-1
                for(; j<k && xids[q*k+j] == -1L; j++);
                if(j>=k)
                    break;
                //move elements[j] to position i
                xids[q*k+i] = xids[q*k+j];
                scores[q*k+i]=scores[q*k+j];
                i++;
                j++;
            }
            for(; i<k; i++){
                xids[q*k+i] = -1L;
                scores[q*k+i]=-1.0f;
            }
        }
    }
    return;
}

long VectoDB::getBaseMutation() const
{
    return *(uint64_t*)state->data_mut;
}

void VectoDB::incBaseMutation()
{
    *(uint64_t*)state->data_mut += 1;
    msync(state->data_mut, 8, MS_ASYNC);
}

std::string VectoDB::getBaseFvecsFp() const
{
    ostringstream oss;
    oss << work_dir << "/base.fvecs";
    return oss.str();
}

std::string VectoDB::getBaseXidsFp() const
{
    ostringstream oss;
    oss << work_dir << "/base.xids";
    return oss.str();
}

std::string VectoDB::getBaseMutationFp() const
{
    ostringstream oss;
    oss << work_dir << "/base.mutation";
    return oss.str();
}

std::string VectoDB::getIndexFp(long mutation, long ntrain) const
{
    ostringstream oss;
    oss << work_dir << "/" << index_key << "." << mutation << "." << ntrain << ".index";
    return oss.str();
}

void VectoDB::getIndexFpLatest(long& mutation, long& ntrain) const
{
    mutation = ntrain = 0;
    fs::path fp_index;
    const std::regex base_regex(index_key + R"(\.(\d+)\.(\d+)\.index)");
    std::smatch base_match;
    for (auto ent = fs::directory_iterator(work_dir); ent != fs::directory_iterator(); ent++) {
        const fs::path& p = ent->path();
        if (fs::is_regular_file(p)) {
            const string fn = p.filename().string();
            if (std::regex_match(fn, base_match, base_regex) && base_match.size() == 3) {
                long cur_mutation = std::stol(base_match[1].str());
                long cur_ntrain = std::stol(base_match[2].str());
                if (cur_mutation > mutation) {
                    mutation = cur_mutation;
                    ntrain = cur_ntrain;
                }
            }
        }
    }
}

void VectoDB::clearIndexFiles()
{
    fs::path fp_index;
    const std::regex base_regex(index_key + R"(\.(\d+)\.(\d+)\.index)");
    std::smatch base_match;
    for (auto ent = fs::directory_iterator(work_dir); ent != fs::directory_iterator(); ent++) {
        const fs::path& p = ent->path();
        if (fs::is_regular_file(p)) {
            const string fn = p.filename().string();
            if (std::regex_match(fn, base_match, base_regex)) {
                fs::remove(p);
            }
        }
    }
}

void VectoDB::ClearWorkDir(const char* work_dir)
{
    for (auto ent = fs::directory_iterator(work_dir); ent != fs::directory_iterator(); ent++) {
        const fs::path& p = ent->path();
        if (fs::is_regular_file(p)) {
            fs::remove(p);
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

void VectoDB::mmapFile(const string& fp, uint8_t*& data, long& len_data)
{
    munmapFile(fp, data, len_data);
    long len_f = fs::file_size(fp); //equivalent to "fs_base_fvecs.seekp(0, ios_base::end); long len_f = fs_base_fvecs.tellp();"
    if (len_f == 0)
        return;
    int f = open(fp.c_str(), O_RDONLY);
    void* tmpd = mmap(NULL, len_f, PROT_READ, MAP_SHARED, f, 0);
    if (tmpd == MAP_FAILED)
        throw fs::filesystem_error(fp, error_code(errno, generic_category()));
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
            throw fs::filesystem_error(fp, error_code(errno, generic_category()));
        data = nullptr;
        len_data = 0;
    }
}

/**
 * C wrappers
 */

void* VectodbNew(char* work_dir, long dim)
{
    VectoDB* vdb = new VectoDB(work_dir, dim);
    return vdb;
}

void VectodbDelete(void* vdb)
{
    delete static_cast<VectoDB*>(vdb);
}

void VectodbAddWithIds(void* vdb, long nb, float* xb, long* xids)
{
    static_cast<VectoDB*>(vdb)->AddWithIds(nb, xb, xids);
}

void VectodbRemoveIds(void* vdb, long nb, long* xids)
{
    static_cast<VectoDB*>(vdb)->RemoveIds(nb, xids);
}

void VectodbSyncIndex(void* vdb)
{
    static_cast<VectoDB*>(vdb)->SyncIndex();
}


long VectodbGetTotal(void* vdb)
{
    return static_cast<VectoDB*>(vdb)->GetTotal();
}

void VectodbSearch(void* vdb, long nq, long k, float* xq, long* uids, float* scores, long* xids)
{
    static_cast<VectoDB*>(vdb)->Search(nq, k, xq, uids, scores, xids);
}

void VectodbClearWorkDir(char* work_dir)
{
    VectoDB::ClearWorkDir(work_dir);
}
