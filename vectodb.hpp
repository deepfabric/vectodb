#pragma once

#include <memory> //std::shared_ptr
#include <string>
#include <vector>

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
