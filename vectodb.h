#pragma once

#include <memory> //std::shared_ptr
#include <string>

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
     * Writer methods. There could be multiple writers.
     * Avoid rwlock intentionally since C++ locks interfere with goroutines scheduling.
     */

    /**  
     * Activate index built with TryBuildIndex or BuildIndex.
     * If upper layer decide not to activate an index, it shall delete the index to reclaim resource.
     * If index_key is Flat, then TryBuildIndex, BuildIndex, ActivateIndex can be skipped.
     * @param index                 input index
     * @param index_size            input the number of vectors contained in index
     */
    void ActivateIndex(faiss::Index* index);

    /** 
     * Add n vectors of dimension d to the index.
     * The upper layer does memory management for xb, xids.
     *
     * @param xb     input matrix, size n * d
     * @param xids if non-null, ids to store for the vectors (size n)
     */
    void AddWithIds(long nb, const float* xb, const long* xids);

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
     * Reader methods. There could be multiple readers.
     * Avoid rwlock intentionally since C++ locks interfere with goroutines scheduling.
     */

    /** Same as build_index but skip building if current number of exhaust vectors is under the given threshold.
     *
     * @param exhaust_threshold     input exhaust threshold
     */
    void TryBuildIndex(long exhaust_threshold, faiss::Index*& index) const;

    /** 
     * Build index.
     * @param index                 output index
     * @param index_size            output the number of vectors contained in index
     */
    void BuildIndex(faiss::Index*& index) const;

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

private:
    std::string getBaseFp() const;
    std::string getIndexFp() const;
    long getIndexSize() const;
    void buildFlatIndex(faiss::Index*& index, long nb, const float* xb);

private:
    std::string work_dir;
    long dim;
    int metric_type;
    const char* index_key;
    const char* query_params;
    std::unique_ptr<DbState> state;
};
