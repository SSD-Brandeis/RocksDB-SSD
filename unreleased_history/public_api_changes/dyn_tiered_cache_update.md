The `NewTieredCache` API has been changed to take the total cache capacity (inclusive of both the primary and the compressed secondary cache) and the ratio of total capacity to allocate to the compressed cache. These are specified in `TieredCacheOptions`. Any capacity specified in `LRUCacheOptions`, `HyperClockCacheOptions` and `CompressedSecondaryCacheOptions` is ignored. A new API, `UpdateTieredCache` is provided to dynamically update the total capacity, ratio of compressed cache, and admission policy.