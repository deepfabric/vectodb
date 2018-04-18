#pragma once

class VectoDB;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Constructor and destructor methods.
 */
VectoDB* VectodbNew(char* work_dir, long dim, int metric_type, char* index_key, char* query_params);
void VectodbDelete(VectoDB* vdb);

/**
 * Writer methods. There could be multiple writers.
 */
void VectodbActivateIndex(VectoDB* vdb, void* index, long ntrain);
void VectodbAddWithIds(VectoDB* vdb, long nb, float* xb, long* xids);

/**
 * Reader methods. There could be multiple readers.
 */
void* VectodbTryBuildIndex(VectoDB* vdb, long exhaust_threshold, long* ntrain);
void* VectodbBuildIndex(VectoDB* vdb, long* ntrain);
void VectodbSearch(VectoDB* vdb, long nq, float* xq, float* distances, long* xids);

/**
 * Static methods.
 */
void VectodbClearWorkDir(char* work_dir);

#ifdef __cplusplus
}
#endif
