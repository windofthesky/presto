/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.connector.meta;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class FeatureUtil
{
    private FeatureUtil()
    {
    }

    public static Optional<Set<ConnectorFeature>> supportedFeatures(Class<?> connector)
    {
        SupportedFeatures supported = connector.getAnnotation(SupportedFeatures.class);
        return supported == null ? Optional.empty() : Optional.of(ImmutableSet.copyOf(supported.value()));
    }

    public static Set<ConnectorFeature> requiredFeatures(AnnotatedElement test)
    {
        RequiredFeatures requiredFeatures = test.getAnnotation(RequiredFeatures.class);
        return requiredFeatures == null ? ImmutableSet.of() : ImmutableSet.copyOf(requiredFeatures.value());
    }

    public static boolean connectorSupportsTestMethod(Class<?> connector, Class<?> test, Method testMethod)
    {
        Optional<Set<ConnectorFeature>> supportedFeatures = supportedFeatures(connector);
        Set<ConnectorFeature> requiredFeatures = Sets.union(requiredFeatures(test), requiredFeatures(testMethod));

        return !supportedFeatures.isPresent() || supportedFeatures.get().containsAll(requiredFeatures);
    }

    public static Stream<ConnectorFeature> streamOfRequired(Optional<RequiredFeatures> requiredFeatures)
    {
        return requiredFeatures.map(required -> Arrays.stream(required.value()))
                .orElse(Stream.empty());
    }

    public static Stream<ConnectorFeature> streamOfSupported(Optional<SupportedFeatures> supportedFeatures)
    {
        return supportedFeatures.map(supported -> Arrays.stream(supported.value()))
                .orElse(Stream.empty());
    }
}
