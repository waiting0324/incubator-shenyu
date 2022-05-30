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

package org.apache.shenyu.plugin.divide.handler;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.dto.convert.selector.DivideUpstream;
import org.apache.shenyu.common.dto.convert.rule.impl.DivideRuleHandle;
import org.apache.shenyu.common.enums.PluginEnum;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.loadbalancer.cache.UpstreamCacheManager;
import org.apache.shenyu.loadbalancer.entity.Upstream;
import org.apache.shenyu.plugin.base.cache.CommonHandleCache;
import org.apache.shenyu.plugin.base.cache.MetaDataCache;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.plugin.base.utils.BeanHolder;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The type Divide plugin data handler.
 */
public class DividePluginDataHandler implements PluginDataHandler {

    public static final Supplier<CommonHandleCache<String, DivideRuleHandle>> CACHED_HANDLE = new BeanHolder<>(CommonHandleCache::new);

    @Override
    public void handlerRule(final RuleData ruleData) {

        // 获取 RuleData 的 handle 数据
        Optional.ofNullable(ruleData.getHandle()).ifPresent(s -> {

            // 将 handle 数据 转换成 DivideRuleHandle 对象
            DivideRuleHandle divideRuleHandle = GsonUtils.getInstance().fromJson(s, DivideRuleHandle.class);

            // 根据 Rule 对象，获取 Rule id 后，更新专属于 DividePlugin 的缓存
            CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), divideRuleHandle);

            // 删除 MetaDataCache 的 DIVIDE_CACHE 缓存
            MetaDataCache.getInstance().clean();
        });
    }

    @Override
    public void handlerSelector(final SelectorData selectorData) {
        List<DivideUpstream> upstreamList = GsonUtils.getInstance().fromList(selectorData.getHandle(), DivideUpstream.class);
        if (CollectionUtils.isEmpty(upstreamList)) {
            return;
        }
        UpstreamCacheManager.getInstance().submit(selectorData.getId(), convertUpstreamList(upstreamList));
        // the update is also need to clean, but there is no way to
        // distinguish between crate and update, so it is always clean
        MetaDataCache.getInstance().clean();
    }

    @Override
    public void removeSelector(final SelectorData selectorData) {
        UpstreamCacheManager.getInstance().removeByKey(selectorData.getId());
        MetaDataCache.getInstance().clean();
    }

    @Override
    public void removeRule(final RuleData ruleData) {
        Optional.ofNullable(ruleData.getHandle()).ifPresent(s -> CACHED_HANDLE.get().removeHandle(CacheKeyUtils.INST.getKey(ruleData)));
        MetaDataCache.getInstance().clean();
    }

    @Override
    public String pluginNamed() {
        return PluginEnum.DIVIDE.getName();
    }

    private List<Upstream> convertUpstreamList(final List<DivideUpstream> upstreamList) {
        return upstreamList.stream().map(u -> Upstream.builder()
                .protocol(u.getProtocol())
                .url(u.getUpstreamUrl())
                .weight(u.getWeight())
                .status(u.isStatus())
                .timestamp(u.getTimestamp())
                .warmup(u.getWarmup())
                .build()).collect(Collectors.toList());
    }
}
