#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Constructor and destructor methods.
 */
void* VectodbNew(char* work_dir, long dim);
void VectodbDelete(void* vdb);
void VectodbAddWithIds(void* vdb, long nb, float* xb, long* xids);
void VectodbRemoveIds(long nb, long* xids);
void VectodbSearch(void* vdb, long nq, long k, float* xq, long* uids, float* scores, long* xids);
void VectodbSyncIndex(void* vdb);
long VectodbGetTotal(void* vdb);

/**
 * Static methods.
 */
void VectodbClearDir(char* work_dir);
void VectodbNormVec(float* vec, int dim);


#ifdef __cplusplus
}
#endif
