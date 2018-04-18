#pragma once

#ifdef __cplusplus
extern "C" {
#endif

struct VectoDB;

/**
 * Constructor and destructor methods.
 */
struct VectoDB* VectodbNew(char* work_dir, long dim, int metric_type, char* index_key, char* query_params);
void VectodbDelete(struct VectoDB* vdb);

/**
 * Writer methods. There could be multiple writers.
 */
void VectodbActivateIndex(struct VectoDB* vdb, void* index, long ntrain);
void VectodbAddWithIds(struct VectoDB* vdb, long nb, float* xb, long* xids);

/**
 * Reader methods. There could be multiple readers.
 */
void* VectodbTryBuildIndex(struct VectoDB* vdb, long exhaust_threshold, long* ntrain);
void* VectodbBuildIndex(struct VectoDB* vdb, long* ntrain);
void VectodbSearch(struct VectoDB* vdb, long nq, float* xq, float* distances, long* xids);

/**
 * Static methods.
 */
void VectodbClearWorkDir(char* work_dir);

#ifdef __cplusplus
}
#endif
