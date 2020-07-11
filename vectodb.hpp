#pragma once

#include <memory> //std::shared_ptr
#include <string>
#include <vector>
#include "faiss/roaring.h"

class DbState;
namespace faiss {
class Index;
};
//class faiss::Index;

class VectoDB {
public:
    /** 
     * Construct a VectoDB, load base and index from work_dir.
     *
     * @param work_dir      input working direcotry. will load existing index if the directory is not empty.
     * @param dim           input dimension of vector
     */
    VectoDB(const char* work_dir, long dim);

    /** 
     * Deconstruct a VectoDB.
     */
    virtual ~VectoDB();

    /** 
     * Add n vectors of dimension d to the index.
     * The upper layer does memory management for xb, xids.
     *
     * @param xb     input matrix, size n * d
     * @param xids   ids to store for the vectors (size n). High 32bits uid, low 32bits pid.
     */
    void AddWithIds(long nb, const float* xb, const long* xids);

    /** removes IDs from the index. Returns the number of elements removed.
     */
    long RemoveIds(long nb, const long* xids);

    /**
     * removes all elements from the database.
    */
    void Reset();

    /** 
     * Get total number of vectors.
     *
     */
    long GetTotal();

    /** 
     * Query n vectors of dimension d to the index.
     * The upper layer does memory management for xq, uids, scores, xids.
     *
     * @param nq            input the number of vectors to search
     * @param xq            input vectors to search, size nq * d
     * @param k             input do kNN search
     * @param uids          input uid bitmap pointer array, size nq
     * @param scores        output pairwise scores, size nq * k
     * @param xids          output labels of the kNN, size nq * k
     */
    void Search(long nq, const float* xq, long k, const long* uids, float* scores, long* xids);

private:
    std::string getFlatFp() const;

private:
    std::string work_dir;
    long dim;
    std::string fp_flat;
    std::unique_ptr<DbState> state;
};


/** 
 * Remove all files under the given work directory.
 *
 * @param work_dir      input working direcotry
 */
void ClearDir(const char* work_dir);
void NormVec(float* vec, int dim);

// Keep sync with vectodb.go
inline uint64_t GetUid(uint64_t xid) {return xid>>34;}
inline uint64_t GetPid(uint64_t xid) {return xid&0x3FFFFFFFF;}
inline uint64_t GetXid(uint64_t uid, uint64_t pid) {return (uid<<34) + pid;}

// Keep sync with RoaringBitmapWithSmallSet in Clickhouse
void ChBitmapSerialize(const roaring_bitmap_t * rb, char *& buf, int& size);
roaring_bitmap_t * ChBitmapDeserialize(const char * buf);

// Compatible with ClickHouse readVarUInt
// Returns how many bytes readed.
inline int ReadVarUInt(uint64_t &x, const char *in)
{
    size_t i;
    x = 0;
    for (i = 0; i < 9; ++i)
    {
        x += uint64_t((*in) & 0x7F) << (i * 7);
        if (uint8_t(*in) <= 0x7F)
            break;
        in++;
    }
    return(int(i + 1));
}

// Returns how many bytes written.
inline int WriteVarUInt(uint64_t x, char *out)
{
    size_t i;
    for (i = 0; i < 9; ++i)
    {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F)
            byte |= 0x80;
        *out = byte;
        out++;
        x >>= 7;
        if (!x)
            break;
    }
    return(int(i + 1));
}

inline int GetLengthOfVarUInt(uint64_t x)
{
    return x < (1ULL << 7) ? 1
        : (x < (1ULL << 14) ? 2
        : (x < (1ULL << 21) ? 3
        : (x < (1ULL << 28) ? 4
        : (x < (1ULL << 35) ? 5
        : (x < (1ULL << 42) ? 6
        : (x < (1ULL << 49) ? 7
        : (x < (1ULL << 56) ? 8
        : 9)))))));
}
