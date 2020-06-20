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
     * @param index_key     input faiss index_key
     * @param query_params  input faiss selected params of auto-tuning
     */
    VectoDB(const char* work_dir, long dim, const char* index_key = "IVF4096,PQ32", const char* query_params = "nprobe=256,ht=256");

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

    void RemoveIds(long nb, const long* xids);

    /** 
     * Get total number of vectors.
     *
     */
    long GetTotal();

    /** 
     * Upper layer shall invoke this regularly to let deletion & update take effect, and ensure all vectors be indexed.
     */
    void SyncIndex();

    /** 
     * Query n vectors of dimension d to the index.
     * The upper layer does memory management for xq, uids, scores, xids.
     *
     * @param nq            input the number of vectors to search
     * @param k             input do kNN search
     * @param xq            input vectors to search, size nq * d
     * @param uids          input uid bitmap pointer array, size nq
     * @param scores        output pairwise scores, size nq * k
     * @param xids          output labels of the kNN, size nq * k
     */
    void Search(long nq, long k, const float* xq, const long* uids, float* scores, long* xids);

public:
    /** 
     * Remove all files under the given work directory.
     *
     * @param work_dir      input working direcotry
     */
    static void ClearWorkDir(const char* work_dir);

    static void Normalize(std::vector<float>& vec);
    static void mmapFile(const std::string& fp, uint8_t*& data, long& len_data);
    static void munmapFile(const std::string& fp, uint8_t*& data, long& len_data);

private:
    std::string getBaseFvecsFp() const;
    std::string getBaseXidsFp() const;
    std::string getBaseMutationFp() const;
    std::string getIndexFp(long mutuation, long ntrain) const;
    long getBaseMutation() const;
    void incBaseMutation();
    void getIndexFpLatest(long& mutation, long& ntrain) const;
    void clearIndexFiles();
    void createBaseFilesIfNotExist();
    void openBaseFiles();
    void closeBaseFiles();

private:
    std::string work_dir;
    long dim;
    long len_vec;
    std::string index_key;
    std::string query_params;
    std::unique_ptr<DbState> state;
    std::string fp_base_xids;
    std::string fp_base_fvecs;
    std::string fp_base_mutation;
    std::string fp_base_xids_tmp;
    std::string fp_base_fvecs_tmp;
    std::string fp_base_mutation_tmp;
};
