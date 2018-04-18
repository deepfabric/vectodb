#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Constructor and destructor methods.
 */
void* VectodbNew(char* work_dir, long dim, int metric_type, char* index_key, char* query_params);
void VectodbDelete(void* vdb);

/**
 * Writer methods. There could be multiple writers.
 */
void VectodbActivateIndex(void* vdb, void* index, long ntrain);
void VectodbAddWithIds(void* vdb, long nb, float* xb, long* xids);

/**
 * Reader methods. There could be multiple readers.
 */
void* VectodbTryBuildIndex(void* vdb, long exhaust_threshold, long* ntrain);
void* VectodbBuildIndex(void* vdb, long* ntrain);
void VectodbSearch(void* vdb, long nq, float* xq, float* distances, long* xids);

/**
 * Static methods.
 */
void VectodbClearWorkDir(char* work_dir);

#ifdef __cplusplus
}
#endif
