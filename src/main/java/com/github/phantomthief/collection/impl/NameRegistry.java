package com.github.phantomthief.collection.impl;

import static com.github.phantomthief.util.MoreReflection.getCallerPlace;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.github.phantomthief.collection.BufferTrigger;

/**
 * @author w.vela
 * Created on 2021-02-04.
 */
public interface NameRegistry {

    /**
     * 注意，当前还只支持 {@link BufferTrigger#simple()} 方式构建的命名获取
     */
    static NameRegistry autoRegistry() {
        return () -> {
            StackTraceElement callerPlace = getCallerPlace(SimpleBufferTriggerBuilder.class);
            if (callerPlace != null && !StringUtils.equals("ReflectionUtils.java", callerPlace.getFileName())) {
                return callerPlace.getFileName() + ":" + callerPlace.getLineNumber();
            } else {
                return null;
            }
        };
    }

    @Nullable
    String name();
}
