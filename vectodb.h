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
 * Methods don't need Go rwlock protection.
 */
void* VectodbBuildIndex(void* vdb, long cur_ntrain, long cur_ntotal, long* ntrain);
void VectodbAddWithIds(void* vdb, long nb, float* xb, long* xids);

/**
 * Methods assuming Go write-lock already holded. There could be multiple writers.
 */
void VectodbActivateIndex(void* vdb, void* index, long ntrain);

/**
 * Methods assuming Go read-lock already holded. There could be multiple readers.
 */
void VectodbGetIndexState(void* vdb, long* ntrain, long* ntotal, long* nflat);
void VectodbSearch(void* vdb, long nq, float* xq, float* distances, long* xids);

/**
 * Static methods.
 */
void VectodbClearWorkDir(char* work_dir);

#ifdef __cplusplus
}
#endif
