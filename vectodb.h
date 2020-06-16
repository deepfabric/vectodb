#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Constructor and destructor methods.
 */
void* VectodbNew(char* work_dir, long dim);
void VectodbDelete(void* vdb);

void* VectodbBuildIndex(void* vdb, long* ntrain);
void VectodbAddWithIds(void* vdb, long nb, float* xb, long* xids);
long VectodbGetTotal(void* vdb);
long VectodbGetFlatSize(void* vdb);

void VectodbActivateIndex(void* vdb, void* index, long ntrain);
void VectodbGetIndexSize(void* vdb, long* ntrain, long* nsize);
long VectodbSearch(void* vdb, long nq, float* xq, long *ks, float* scores, long* xids);

/**
 * Static methods.
 */
void VectodbClearWorkDir(char* work_dir);

#ifdef __cplusplus
}
#endif
