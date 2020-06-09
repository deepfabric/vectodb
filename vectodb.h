#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Constructor and destructor methods.
 */
void* VectodbNew(char* work_dir, long dim, int metric_type, char* index_key, char* query_params, float dist_threshold);
void VectodbDelete(void* vdb);

void* VectodbBuildIndex(void* vdb, long cur_ntrain, long cur_ntotal, long* ntrain);
void VectodbAddWithIds(void* vdb, long nb, float* xb, long* xids);
void VectodbUpdateWithIds(void* vdb, long nb, float* xb, long* xids);
long VectodbUpdateBase(void* vdb);
long VectodbGetTotal(void* vdb);
long VectodbGetFlatSize(void* vdb);

void VectodbActivateIndex(void* vdb, void* index, long ntrain);
void VectodbGetIndexSize(void* vdb, long* ntrain, long* nsize);
long VectodbSearch(void* vdb, long nq, long k, float* xq, float* distances, long* xids);

/**
 * Static methods.
 */
void VectodbClearWorkDir(char* work_dir);

#ifdef __cplusplus
}
#endif
