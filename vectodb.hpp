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
     * @param work_dir      input working direcotry
     * @param dim           input dimension of vector
     * @param metric_type   input faiss metric, 0 - METRIC_INNER_PRODUCT, 1 - METRIC_L2
     * @param index_key     input faiss index_key
     * @param query_params  input faiss selected params of auto-tuning
     * @param dist_threshold   input distance threshold
     */
    VectoDB(const char* work_dir, long dim, int metric_type = 0, const char* index_key = "IVF4096,PQ32", const char* query_params = "nprobe=256,ht=256", float dist_threshold = 0.6f);

    /** 
     * Deconstruct a VectoDB.
     */
    virtual ~VectoDB();

    /** 
     * Build index.
     * @param index     output index
     * @param ntrain    output the number of train vectors
     */
    void BuildIndex(faiss::Index*& index, long& ntrain) const;

    /** 
     * Add n vectors of dimension d to the index.
     * The upper layer does memory management for xb, xids.
     *
     * @param xb     input matrix, size n * d
     * @param xids   ids to store for the vectors (size n). High 32bits uid, low 32bits pid.
     */
    void AddWithIds(long nb, const float* xb, const long* xids);

    /** 
     * Get total number of vectors.
     *
     */
    long GetTotal();

    /** 
     * Get flat size.
     *
     */
    long GetFlatSize();

    /**  
     * Activate index built with TryBuildIndex or BuildIndex.
     * If upper layer decide not to activate an index, it shall delete the index to reclaim resource.
     * If index_key is Flat, then TryBuildIndex, BuildIndex, ActivateIndex can be skipped.
     * @param index     input index
     * @param ntrain    input the number of training points of the index
     */
    void ActivateIndex(faiss::Index* index, long ntrain);

    /** 
     * Get index size.
     *
     * @param ntrain        output number of index train points
     * @param nsize         output number of index points
     */
    void GetIndexSize(long& ntrain, long& nsize) const;

    /** 
     * Query n vectors of dimension d to the index.
     * The upper layer does memory management for xq, distances, xids.
     *
     * @param nq            input the number of vectors to search
     * @param k             input do kNN search
     * @param xq            input vectors to search, size nq * d
     * @param uids          input uid bitmap pointer array, size nq
     * @param distances     output pairwise distances, size nq * k
     * @param xids          output labels of the kNN, size nq * k
     */
    long Search(long nq, long k, const float* xq, const char** uids, float* distances, long* xids);

public:
    /** 
     * Remove base and index files under the given work directory.
     *
     * @param work_dir      input working direcotry
     */
    static void ClearWorkDir(const char* work_dir);

    /** 
     * Compare distance. Return true if dis1 is closer then dis2.
     *
     */
    static bool CompareDistance(int metric_type, float dis1, float dis2)
    {
        return (metric_type == 0) == (dis1 > dis2);
    }
    static void Normalize(std::vector<float>& vec);
    static void mmapFile(const std::string& fp, uint8_t*& data, long& len_data);
    static void munmapFile(const std::string& fp, uint8_t*& data, long& len_data);

private:
    std::string getBaseFp() const;
    std::string getIndexFp(long ntrain) const;
    long getNumLines(long len_data, long len_base_line) const;
    long getIndexFpNtrain() const;
    void clearIndexFiles();
    void readBase(const uint8_t* data, long len_data, long start_num, std::vector<float>& base) const;
    void readXids(const uint8_t* data, long len_data, long start_num, std::vector<long>& xids) const;

private:
    std::string work_dir;
    long dim;
    long len_vec;
    long len_base_line;
    long len_upd_line;
    int metric_type;
    float dist_threshold;
    std::string index_key;
    std::string query_params;
    std::unique_ptr<DbState> state;
};
