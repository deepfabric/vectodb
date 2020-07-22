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
long VectodbRemoveIds(void* vdb, long nb, long* xids);
void VectodbReset(void* vdb);
void VectodbSearch(void* vdb, long nq, float* xq, long k, int top_vectors, long* uids, float* scores, long* xids);
long VectodbGetTotal(void* vdb);

/**
 * Static methods.
 */
void VectodbClearDir(char* work_dir);
void VectodbNormVec(float* vec, int dim);


#ifdef __cplusplus
}
#endif
