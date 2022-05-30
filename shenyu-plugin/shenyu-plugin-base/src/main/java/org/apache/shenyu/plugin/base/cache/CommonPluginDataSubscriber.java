/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shenyu.plugin.base.cache;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.enums.DataEventTypeEnum;
import org.apache.shenyu.common.enums.PluginHandlerEventEnum;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The type Common plugin data subscriber.
 */
public class CommonPluginDataSubscriber implements PluginDataSubscriber {

    private static final Logger LOG = LoggerFactory.getLogger(CommonPluginDataSubscriber.class);

    private final Map<String, PluginDataHandler> handlerMap;

    private ApplicationEventPublisher eventPublisher;

    /**
     * Instantiates a new Common plugin data subscriber.
     *
     * @param pluginDataHandlerList the plugin data handler list
     */
    public CommonPluginDataSubscriber(final List<PluginDataHandler> pluginDataHandlerList) {
        this.handlerMap = pluginDataHandlerList.stream().collect(Collectors.toConcurrentMap(PluginDataHandler::pluginNamed, e -> e));
    }

    /**
     * Instantiates a new Common plugin data subscriber.
     *
     * @param pluginDataHandlerList the plugin data handler list
     * @param eventPublisher        eventPublisher is used to publish sort plugin event
     */
    public CommonPluginDataSubscriber(final List<PluginDataHandler> pluginDataHandlerList,
                                      final ApplicationEventPublisher eventPublisher) {
        this.handlerMap = pluginDataHandlerList.stream().collect(Collectors.toConcurrentMap(PluginDataHandler::pluginNamed, e -> e));
        this.eventPublisher = eventPublisher;
    }

    /**
     * Put extend plugin data handler.
     *
     * @param handlers the handlers
     */
    public void putExtendPluginDataHandler(final List<PluginDataHandler> handlers) {
        if (CollectionUtils.isEmpty(handlers)) {
            return;
        }
        for (PluginDataHandler handler : handlers) {
            String pluginNamed = handler.pluginNamed();
            handlerMap.computeIfAbsent(pluginNamed, name -> {
                LOG.info("shenyu auto add extends plugin data handler name is :{}", pluginNamed);
                return handler;
            });
        }
    }

    @Override
    public void onSubscribe(final PluginData pluginData) {
        subscribeDataHandler(pluginData, DataEventTypeEnum.UPDATE);
    }

    @Override
    public void unSubscribe(final PluginData pluginData) {
        subscribeDataHandler(pluginData, DataEventTypeEnum.DELETE);
    }

    @Override
    public void refreshPluginDataAll() {
        BaseDataCache.getInstance().cleanPluginData();
    }

    @Override
    public void refreshPluginDataSelf(final List<PluginData> pluginDataList) {
        if (CollectionUtils.isEmpty(pluginDataList)) {
            return;
        }
        BaseDataCache.getInstance().cleanPluginDataSelf(pluginDataList);
    }

    @Override
    public void onSelectorSubscribe(final SelectorData selectorData) {
        subscribeDataHandler(selectorData, DataEventTypeEnum.UPDATE);
    }

    @Override
    public void unSelectorSubscribe(final SelectorData selectorData) {
        subscribeDataHandler(selectorData, DataEventTypeEnum.DELETE);
    }

    @Override
    public void refreshSelectorDataAll() {
        BaseDataCache.getInstance().cleanSelectorData();
    }

    @Override
    public void refreshSelectorDataSelf(final List<SelectorData> selectorDataList) {
        if (CollectionUtils.isEmpty(selectorDataList)) {
            return;
        }
        BaseDataCache.getInstance().cleanSelectorDataSelf(selectorDataList);
    }

    @Override
    public void onRuleSubscribe(final RuleData ruleData) {
        // 调用处理 规则数据 的方法，更新数据
        subscribeDataHandler(ruleData, DataEventTypeEnum.UPDATE);
    }

    @Override
    public void unRuleSubscribe(final RuleData ruleData) {
        subscribeDataHandler(ruleData, DataEventTypeEnum.DELETE);
    }

    @Override
    public void refreshRuleDataAll() {
        BaseDataCache.getInstance().cleanRuleData();
    }

    @Override
    public void refreshRuleDataSelf(final List<RuleData> ruleDataList) {
        if (CollectionUtils.isEmpty(ruleDataList)) {
            return;
        }
        BaseDataCache.getInstance().cleanRuleDataSelf(ruleDataList);
    }

    private <T> void subscribeDataHandler(final T classData, final DataEventTypeEnum dataType) {
        // 如果 事件类型 为 UPDATE 则 更新本地缓存
        if (dataType == DataEventTypeEnum.UPDATE) {
            Optional.ofNullable(classData)
                    .ifPresent(data -> updateCacheData(classData));
        }
        // 如果 事件类型 为 DELETE 则 删除本地缓存
        else if (dataType == DataEventTypeEnum.DELETE) {
            Optional.ofNullable(classData)
                    .ifPresent(data -> removeCacheData(classData));
        }
    }

    /**
     * update cache data.
     *
     * @param data data is plugin mate data, data is not null
     * @param <T>  data type, support is [{@link PluginData},{@link SelectorData},{@link RuleData}]
     */
    private <T> void updateCacheData(@NonNull final T data) {

        // 如果 事件数据 是 PluginData 类型
        if (data instanceof PluginData) {
            PluginData pluginData = (PluginData) data;
            final PluginData oldPluginData = BaseDataCache.getInstance().obtainPluginData(pluginData.getName());

            // 更新 BaseDataCache 中的缓存
            BaseDataCache.getInstance().cachePluginData(pluginData);

            // 从 handlerMap 中获取 PluginDataHandler，再调用 handlerPlugin 方法 更新缓存
            Optional.ofNullable(handlerMap.get(pluginData.getName()))
                    .ifPresent(handler -> handler.handlerPlugin(pluginData));

            // 通知 插件 启用状态
            PluginHandlerEventEnum state = Boolean.TRUE.equals(pluginData.getEnabled())
                    ? PluginHandlerEventEnum.ENABLED : PluginHandlerEventEnum.DISABLED;
            eventPublisher.publishEvent(new PluginHandlerEvent(state, pluginData));

            // 对 插件 进行排序
            sortPluginIfOrderChange(oldPluginData, pluginData);
        }
        // 如果 事件数据 是 SelectorData 类型
        else if (data instanceof SelectorData) {
            SelectorData selectorData = (SelectorData) data;

            // 更新 BaseDataCache 中的缓存
            BaseDataCache.getInstance().cacheSelectData(selectorData);

            // 从 handlerMap 中获取 PluginDataHandler，再调用 handlerSelector 方法 更新缓存
            Optional.ofNullable(handlerMap.get(selectorData.getPluginName()))
                    .ifPresent(handler -> handler.handlerSelector(selectorData));

        }
        // 如果 事件数据 是 RuleData 类型
        else if (data instanceof RuleData) {
            RuleData ruleData = (RuleData) data;

            // 更新 BaseDataCache 中的缓存
            BaseDataCache.getInstance().cacheRuleData(ruleData);

            // 从 handlerMap 中获取 PluginDataHandler，再调用 handlerRule 方法 更新缓存
            Optional.ofNullable(handlerMap.get(ruleData.getPluginName()))
                    .ifPresent(handler -> handler.handlerRule(ruleData));

        }
    }

    /**
     * judge need update plugin order.
     *
     * @param oldPluginData old pluginData
     * @param pluginData    current pluginData
     */
    private void sortPluginIfOrderChange(final PluginData oldPluginData, final PluginData pluginData) {
        if (Objects.isNull(eventPublisher) || Objects.isNull(pluginData.getSort())) {
            return;
        }
        if (Objects.isNull(oldPluginData) || Objects.isNull(oldPluginData.getSort())
                || (!Objects.equals(oldPluginData.getSort(), pluginData.getSort()))) {
            eventPublisher.publishEvent(new PluginHandlerEvent(PluginHandlerEventEnum.SORTED, pluginData));
        }
    }

    /**
     * remove cache data.
     *
     * @param data data is plugin mate data, data is not null
     * @param <T>  data type, support is [{@link PluginData},{@link SelectorData},{@link RuleData}]
     */
    private <T> void removeCacheData(@NonNull final T data) {
        if (data instanceof PluginData) {
            PluginData pluginData = (PluginData) data;
            BaseDataCache.getInstance().removePluginData(pluginData);
            Optional.ofNullable(handlerMap.get(pluginData.getName()))
                    .ifPresent(handler -> handler.removePlugin(pluginData));
            eventPublisher.publishEvent(new PluginHandlerEvent(PluginHandlerEventEnum.DELETE, pluginData));
        } else if (data instanceof SelectorData) {
            SelectorData selectorData = (SelectorData) data;
            BaseDataCache.getInstance().removeSelectData(selectorData);
            Optional.ofNullable(handlerMap.get(selectorData.getPluginName()))
                    .ifPresent(handler -> handler.removeSelector(selectorData));

        } else if (data instanceof RuleData) {
            RuleData ruleData = (RuleData) data;
            BaseDataCache.getInstance().removeRuleData(ruleData);
            Optional.ofNullable(handlerMap.get(ruleData.getPluginName()))
                    .ifPresent(handler -> handler.removeRule(ruleData));

        }
    }
}
