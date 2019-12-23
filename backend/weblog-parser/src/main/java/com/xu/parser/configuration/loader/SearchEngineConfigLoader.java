package com.xu.parser.configuration.loader;

import com.xu.parser.configuration.SearchEngineConfig;

import java.util.List;

/**
 *  搜索引擎配置的加载接口
 */
public interface SearchEngineConfigLoader {
    /**
     * 获取所有的搜索引擎配置信息
     * @return
     */
     List<SearchEngineConfig> getSearchEngineConfigs();
}
