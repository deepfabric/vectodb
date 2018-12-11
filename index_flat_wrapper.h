#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// IndexFlatWrapper is a thin wrapper of faiss::IndexFlat. Only supports metric type 0 - METRIC_INNER_PRODUCT.
void* IndexFlatNew(long dim, float dist_threshold);
void IndexFlatDelete(void* ifw);
void IndexFlatAddWithIds(void* ifw, long nb, float* xb, long* xids);
void IndexFlatSearch(void* ifw, long nq, float* xq, float* distances, long* xids);

#ifdef __cplusplus
}
#endif
