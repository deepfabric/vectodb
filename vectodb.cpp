#include "vectodb.hpp"
#include "vectodb.h"

#include "faiss/AutoTune.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexHNSW.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/index_io.h"
#include "faiss/index_factory.h"
#include "faiss/roaring.h"

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

const int small_set_size = 32;

roaring_bitmap_t * RoaringBitmapWithSmallSet::CastAndReset()
{
    roaring_bitmap_t * ra;
    if(rb) {
        ra = rb;
        rb = nullptr;
    } else {
        ra = roaring_bitmap_of_ptr(small.size(), &small[0]);
        small.clear();
    }
    return ra;
}

void RoaringBitmapWithSmallSet::Add(uint32_t value)
{
    if(IsSmall()){
        for(size_t i = 0; i<small.size(); i++)
            if(small[i] == value)
                return;
        if(small.size() < small_set_size){
            small.push_back(value);
            return;
        }
        rb = roaring_bitmap_of_ptr(small.size(), &small[0]);
        small.clear();
    }
    roaring_bitmap_add(rb, value);
}

bool RoaringBitmapWithSmallSet::Contains(uint32_t value)
{
    if(IsSmall()){
        for(size_t i = 0; i<small.size(); i++)
            if(small[i] == value)
                return true;
        return false;
    } else {
        return roaring_bitmap_contains(rb, value);
    }
}

void RoaringBitmapWithSmallSet::Read(char *in)
{
    Reset();
    bool isSmall = (0 == *(in++));
    uint64_t num;
    int readed = ReadVarUInt(num, in);
    in += readed; 
    if(isSmall){
        for(int i=0; i<(int)num; i++){
            Add(*(uint32_t*)in);
            in += 4;
        }
    } else {
        rb = roaring_bitmap_portable_deserialize(in);
    }
}

int RoaringBitmapWithSmallSet::Write(char *out)
{
    int written = 0;
    if(IsSmall()){
        *(out++) = 0x00;
        written = WriteVarUInt((uint64_t)small.size(), out);
        out += written;
        memcpy(out, &small[0], 4*small.size());
        return 1 + written + 4*small.size();
    } else {
        *(out++) = 0x01;
        size_t bs = roaring_bitmap_portable_serialize(rb, out);
        written = GetLengthOfVarUInt(bs);
        memmove(out+written, out, bs);
        WriteVarUInt((uint64_t)bs, out);
        return 1 + written + bs;
    }
}

int RoaringBitmapWithSmallSet::SizeInBytes()
{
    int size;
    if(IsSmall()){
        size = 1 + GetLengthOfVarUInt(small.size()) + 4*small.size();
    } else {
        size = roaring_bitmap_portable_size_in_bytes(rb); 
        size += 1 + GetLengthOfVarUInt(size); 
    }
    return size;
}

struct DbState {
    DbState()
        : flat(nullptr)
    {
    }
    ~DbState()
    {
        delete flat;
    }

    faiss::IndexFlatDisk* flat;
};

VectoDB::VectoDB(const char* work_dir_in, long dim_in)
    : work_dir(work_dir_in)
    , dim(dim_in)
    , fp_flat(getFlatFp())
{
    fs::path dir{ fs::absolute(work_dir_in) };
    work_dir = dir.string().c_str();
    fs::create_directories(work_dir);

    faiss::IndexFlatDisk *flat = nullptr;
    flat = new faiss::IndexFlatDisk(fp_flat, dim_in, faiss::METRIC_INNER_PRODUCT);

    auto st{ std::make_unique<DbState>() }; //Make DbState be exception safe
    state = std::move(st); // equivalent to state.reset(st.release());
    state->flat = flat;
}

VectoDB::~VectoDB()
{
}

void VectoDB::AddWithIds(long nb, const float* xb, const long* xids)
{
    state->flat->add_with_ids(nb, xb, xids);
}

long VectoDB::RemoveIds(long nb, const long* xids)
{
    faiss::IDSelectorBatch sel(nb, xids);
    return state->flat->remove_ids(sel);
}

void VectoDB::Reset()
{
    state->flat->reset();
}

long VectoDB::GetTotal()
{
    return state->flat->ntotal;
}

void VectoDB::Search(long nq, const float* xq, long k, const long* uids, float* scores, long* xids)
{
    for (int i = 0; i < nq*k; i++) {
        xids[i] = -1L;
        scores[i] = -1.0;
    }
    if (uids==nullptr) {
        state->flat->search(nq, xq, k, scores, xids);
    } else {
        vector<roaring_bitmap_t *> rbs(nq);
        for (int i=0; i<nq; i++) {
            char *buf = (char *)uids[i];
            if(buf!=nullptr){
                RoaringBitmapWithSmallSet rbwss;
                rbwss.Read(buf);
                rbs[i] = rbwss.CastAndReset();
            }
            else
                rbs[i] = nullptr;
        }
        /*
        //dump rbs[0]
        if(rbs[0]!=nullptr){
            vector<uint32_t> bits;
            roaring_uint32_iterator_t *  it = roaring_create_iterator(rbs[0]);
            while(it->has_value) {
                bits.push_back(it->current_value);
                roaring_advance_uint32_iterator(it);
            }
            roaring_free_uint32_iterator(it);
            printf("rbs[0]:");
            for(size_t i=0; i<bits.size(); i++)
                printf(" %d", bits[i]);
            printf("\n");
        }*/

        state->flat->search(nq, xq, k, &rbs[0], scores, xids);
        for (int i=0; i<nq; i++) {
            if(rbs[i]!=nullptr)
                roaring_bitmap_free(rbs[i]);
        }
    }
    google::FlushLogFiles(google::INFO);
    return;
}

std::string VectoDB::getFlatFp() const
{
    ostringstream oss;
    oss << work_dir << "/flatdisk.index";
    return oss.str();
}

void ClearDir(const char* work_dir)
{
    fs::remove_all(work_dir);
    fs::create_directories(work_dir);
}

void NormVec(float* vec, int dim)
{
    double l = 0;
    for (int i = 0; i < dim; i++) {
        l += double(vec[i]) * double(vec[i]);
    }
    l = sqrt(l);
    for (int i = 0; i < dim; i++) {
        vec[i] = (float)(((double)vec[i]) / l);
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

long VectodbRemoveIds(void* vdb, long nb, long* xids)
{
    return static_cast<VectoDB*>(vdb)->RemoveIds(nb, xids);
}

void VectodbReset(void* vdb)
{
    static_cast<VectoDB*>(vdb)->Reset();
}

long VectodbGetTotal(void* vdb)
{
    return static_cast<VectoDB*>(vdb)->GetTotal();
}

void VectodbSearch(void* vdb, long nq, float* xq, long k, long* uids, float* scores, long* xids)
{
    static_cast<VectoDB*>(vdb)->Search(nq, xq, k, uids, scores, xids);
}

void VectodbClearDir(char* work_dir)
{
    ClearDir(work_dir);
}

void VectodbNormVec(float* vec, int dim)
{
    NormVec(vec, dim);
}
