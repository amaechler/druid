/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.kafka;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Verifies that the {@code aws-msk-iam-auth} plugin loads and configures using only the
 * dependencies this extension declares, so that a supervisor spec can select the
 * {@code AWS_MSK_IAM} or {@code OAUTHBEARER} mechanism against an IAM-authenticated Amazon MSK
 * cluster.
 *
 * <p>The plugin is declared at {@code runtime} scope with its AWS SDK v2 transitives excluded,
 * because {@code druid-aws-common} already places those artifacts on the core classpath and
 * extensions resolve against it parent-first. None of that is visible to the compiler, so only
 * an actual class load will catch it if the SDK stops satisfying the plugin — after an SDK
 * upgrade, say, or if {@code druid-aws-common} drops an artifact. The test classpath reaches
 * the same SDK jars through {@code druid-server}, so it mirrors what a Druid installation puts
 * in {@code lib/}.
 *
 * <p>These tests use reflection for two reasons: a {@code runtime} dependency is not on the
 * compile classpath, and going through {@link Class#forName} takes the same load path the Kafka
 * client takes when it resolves the login module named in {@code sasl.jaas.config}.
 */
public class AwsMskIamAuthTest
{
  private static final String LOGIN_MODULE = "software.amazon.msk.auth.iam.IAMLoginModule";
  private static final String SASL_CALLBACK_HANDLER = "software.amazon.msk.auth.iam.IAMClientCallbackHandler";
  private static final String OAUTH_CALLBACK_HANDLER =
      "software.amazon.msk.auth.iam.IAMOAuthBearerLoginCallbackHandler";

  private static final String BOOTSTRAP_SERVER = "b-1.example.c1.kafka.us-east-1.amazonaws.com:9098";

  @Test
  public void testSaslCallbackHandlerConfigures()
  {
    configureCallbackHandler(
        SASL_CALLBACK_HANDLER,
        "AWS_MSK_IAM",
        Collections.emptyMap(),
        Collections.emptyMap()
    );
  }

  @Test
  public void testOAuthBearerCallbackHandlerConfigures()
  {
    configureCallbackHandler(
        OAUTH_CALLBACK_HANDLER,
        "OAUTHBEARER",
        Map.of("bootstrap.servers", List.of(BOOTSTRAP_SERVER)),
        Collections.emptyMap()
    );
  }

  /**
   * The assume-role options make the plugin build an {@code StsClient} while the callback handler
   * is being configured. That eager construction reaches a wider slice of the AWS SDK than the
   * default credential chain does, including the ServiceLoader lookup for a synchronous HTTP
   * client, so it is the case most likely to expose a missing artifact.
   */
  @Test
  public void testAssumeRoleCallbackHandlerConfigures()
  {
    configureCallbackHandler(
        SASL_CALLBACK_HANDLER,
        "AWS_MSK_IAM",
        Collections.emptyMap(),
        Map.of(
            "awsRoleArn", "arn:aws:iam::123456789012:role/druid-msk-test",
            "awsStsRegion", "us-east-1"
        )
    );
  }

  /**
   * Loading the login module registers the {@code AWS_MSK_IAM} SASL mechanism as a JCA provider.
   * Both Kafka extensions may do this from their own classloaders within a single JVM, which is
   * what the plugin's classloader-aware SASL client factory exists to support.
   */
  @Test
  public void testSaslMechanismIsRegistered() throws Exception
  {
    Class.forName(LOGIN_MODULE);

    final SaslClient saslClient = Sasl.createSaslClient(
        new String[]{"AWS_MSK_IAM"},
        null,
        "kafka",
        BOOTSTRAP_SERVER,
        Collections.emptyMap(),
        callbacks -> {
        }
    );

    Assertions.assertNotNull(saslClient, "AWS_MSK_IAM SASL mechanism is not registered");
  }

  private void configureCallbackHandler(
      String handlerClassName,
      String saslMechanism,
      Map<String, ?> configs,
      Map<String, String> loginModuleOptions
  )
  {
    final AppConfigurationEntry entry = new AppConfigurationEntry(
        LOGIN_MODULE,
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
        loginModuleOptions
    );

    try {
      final Object handler = Class.forName(handlerClassName).getDeclaredConstructor().newInstance();
      handler.getClass()
             .getMethod("configure", Map.class, String.class, List.class)
             .invoke(handler, configs, saslMechanism, List.of(entry));
    }
    catch (InvocationTargetException e) {
      // A missing AWS SDK artifact surfaces here as a LinkageError wrapped by reflection.
      throw new AssertionError(
          "Failed to configure " + handlerClassName + " for " + saslMechanism
          + ". The AWS SDK v2 artifacts this extension declares as provided are no longer"
          + " sufficient for aws-msk-iam-auth.",
          e.getCause()
      );
    }
    catch (ReflectiveOperationException | LinkageError e) {
      throw new AssertionError("Failed to load " + handlerClassName, e);
    }
  }
}
