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

import com.google.common.base.Joiner;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.connector.meta.FeatureUtil.streamOfRequired;
import static com.facebook.presto.connector.meta.FeatureUtil.streamOfSupported;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;
import static org.junit.platform.commons.util.AnnotationUtils.findRepeatableAnnotations;

public class SupportedTestCondition
        implements ExecutionCondition
{
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext)
    {
        Optional<Method> testMethod = extensionContext.getTestMethod();

        if (!testMethod.isPresent()) {
            return enabled("");
        }

        Class<?> testClass = extensionContext.getRequiredTestClass();

        Optional<RequiredFeatures> methodRequires = findAnnotation(testMethod, RequiredFeatures.class);
        List<RequiredFeatures> classRequires = findRepeatableAnnotations(testClass, RequiredFeatures.class);
        Optional<SupportedFeatures> classSupports = findAnnotation(testClass, SupportedFeatures.class);

        Set<ConnectorFeature> requiredFeatures = Stream.concat(
                streamOfRequired(methodRequires),
                classRequires.stream().flatMap(r -> Arrays.stream(r.value())))
                .distinct()
                .collect(Collectors.toCollection(HashSet::new));

        Set<ConnectorFeature> supportedFeatures = streamOfSupported(classSupports)
                .distinct()
                .collect(toImmutableSet());

        requiredFeatures.removeAll(supportedFeatures);

        return requiredFeatures.isEmpty() ?
                enabled("All required features present") :
                disabled("Missing required features: " + Joiner.on(", ").join(requiredFeatures));
    }
}
