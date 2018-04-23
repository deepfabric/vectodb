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
     * @param index_key     input faiss index_key
     * @param query_params  input faiss selected params of auto-tuning
     * @param metric_type   input faiss metric, 0 - METRIC_INNER_PRODUCT, 1 - METRIC_L2
     */
    VectoDB(const char* work_dir, long dim, int metric_type = 0, const char* index_key = "IVF4096,PQ32", const char* query_params = "nprobe=256,ht=256");

    /** 
     * Deconstruct a VectoDB.
     */
    virtual ~VectoDB();

    /** 
     * Build index.
     * @param index                 output index
     * @param index_size            output the number of vectors contained in index
     */
    void BuildIndex(long cur_ntrain, long cur_ntotal, faiss::Index*& index, long& ntrain) const;

    /** 
     * Add n vectors of dimension d to the index.
     * The upper layer does memory management for xb, xids.
     *
     * @param xb     input matrix, size n * d
     * @param xids if non-null, ids to store for the vectors (size n)
     */
    void AddWithIds(long nb, const float* xb, const long* xids);

    /**
     * Methods assuming Go write-lock already holded. There could be multiple writers.
     * Avoid rwlock intentionally since C++ locks interfere with goroutines scheduling.
     */

    /**  
     * Activate index built with TryBuildIndex or BuildIndex.
     * If upper layer decide not to activate an index, it shall delete the index to reclaim resource.
     * If index_key is Flat, then TryBuildIndex, BuildIndex, ActivateIndex can be skipped.
     * @param index     input index
     * @param ntrain    input the number of training points of the index
     */
    void ActivateIndex(faiss::Index* index, long ntrain);

    /** 
     * Update given vectors.
     * Assuming this operation is very rare, i.e. once a day.
     * This causes disagreement between database and index, so user shall invoke build_index later. 
     * The upper layer does memory management for xb, xids.
     *
     * @param xb        input matrix, size n * d
     * @param xids      if non-null, ids to store for the vectors (size n)
     */
    void UpdateWithIds(long nb, const float* xb, const long* xids);

    /**
     * Methods assuming Go read-lock already holded. There could be multiple readers.
     * Avoid rwlock intentionally since C++ locks interfere with goroutines scheduling.
     */

    /** 
     * Get index state.
     *
     * @param ntain         output number of index train points
     * @param ntotal        output number of index points
     * @param nflat         output number of points not indexed
     */
    void GetIndexState(long& ntrain, long& ntotal, long& nflat) const;

    /** 
     * Query n vectors of dimension d to the index.
     * The upper layer does memory management for xq, distances, xids.
     *
     * @param nq            input the number of vectors to search
     * @param xq            input vectors to search, size nq * d
     * @param xids          output labels of the 1-NNs, size nq
     * @param distances     output pairwise distances, size nq
     */
    void Search(long nq, const float* xq, float* distances, long* xids) const;

public:
    /** 
     * Remove base and index files under the given work directory.
     *
     * @param work_dir      input working direcotry
     */
    static void ClearWorkDir(const char* work_dir);
    static void mmapFile(const std::string& fp, uint8_t*& data, long& len_data);
    static void munmapFile(const std::string& fp, uint8_t*& data, long& len_data);

private:
    std::string getBaseFp() const;
    std::string getIndexFp(long ntrain) const;
    long getIndexFpNtrain() const;
    long getIndexSize() const;
    long getFlatSize() const;
    void readBase(const uint8_t* data, long len_data, long start_num, std::vector<float>& base) const;
    void readXids(const uint8_t* data, long len_data, long start_num, std::vector<long>& xids) const;
    void buildFlat();

private:
    std::string work_dir;
    long dim;
    long len_line;
    int metric_type;
    std::string index_key;
    std::string query_params;
    std::unique_ptr<DbState> state;
};
