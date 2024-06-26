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

package org.apache.druid.storage.azure;

import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AzureDataSegmentKillerTest extends EasyMockSupport
{
  private static final String CONTAINER_NAME = "container";
  private static final String CONTAINER = "test";
  private static final String PREFIX = "test/log";
  private static final String BLOB_PATH = "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";
  private static final String BLOB_PATH_2 = "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/2/0/index.zip";

  private static final int MAX_KEYS = 1;
  private static final int MAX_TRIES = 3;

  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final URI PREFIX_URI = URI.create(StringUtils.format("azure://%s/%s", CONTAINER, PREFIX));
  // BlobStorageException is not recoverable since the client attempts retries on it internally
  private static final Exception NON_RECOVERABLE_EXCEPTION = new BlobStorageException("", null, null);

  private static final DataSegment DATA_SEGMENT = new DataSegment(
      "test",
      Intervals.of("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.of("containerName", CONTAINER_NAME, "blobPath", BLOB_PATH),
      null,
      null,
      new LinearShardSpec(0),
      0,
      1
  );

  private static final DataSegment DATA_SEGMENT_2 = new DataSegment(
      "test",
      Intervals.of("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.of("containerName", CONTAINER_NAME, "blobPath", BLOB_PATH_2),
      null,
      null,
      new LinearShardSpec(0),
      0,
      1
  );

  private AzureDataSegmentConfig segmentConfig;
  private AzureInputDataConfig inputDataConfig;
  private AzureAccountConfig accountConfig;
  private AzureStorage azureStorage;
  private AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;

  @BeforeEach
  public void before()
  {
    segmentConfig = createMock(AzureDataSegmentConfig.class);
    inputDataConfig = createMock(AzureInputDataConfig.class);
    accountConfig = createMock(AzureAccountConfig.class);
    azureStorage = createMock(AzureStorage.class);
    azureCloudBlobIterableFactory = createMock(AzureCloudBlobIterableFactory.class);
  }

  @Test
  public void killTest() throws SegmentLoadingException, BlobStorageException
  {
    List<String> deletedFiles = new ArrayList<>();
    final String dirPath = Paths.get(BLOB_PATH).getParent().toString();

    EasyMock.expect(azureStorage.emptyCloudBlobDirectory(CONTAINER_NAME, dirPath)).andReturn(deletedFiles);

    replayAll();

    final AzureDataSegmentKiller killer = new AzureDataSegmentKiller(
        segmentConfig,
        inputDataConfig,
        accountConfig,
        azureStorage,
        azureCloudBlobIterableFactory
    );

    killer.kill(DATA_SEGMENT);

    verifyAll();
  }

  @Test
  public void test_kill_StorageExceptionExtendedErrorInformationNull_throwsException()
  {
    String dirPath = Paths.get(BLOB_PATH).getParent().toString();

    EasyMock.expect(azureStorage.emptyCloudBlobDirectory(CONTAINER_NAME, dirPath))
            .andThrow(new BlobStorageException("", null, null));

    replayAll();

    final AzureDataSegmentKiller killer = new AzureDataSegmentKiller(
        segmentConfig,
        inputDataConfig,
        accountConfig,
        azureStorage,
        azureCloudBlobIterableFactory
    );

    assertThrows(
        SegmentLoadingException.class,
        () -> killer.kill(DATA_SEGMENT)
    );

    verifyAll();
  }

  @Test
  public void test_kill_runtimeException_throwsException()
  {
    final String dirPath = Paths.get(BLOB_PATH).getParent().toString();

    EasyMock.expect(azureStorage.emptyCloudBlobDirectory(CONTAINER_NAME, dirPath))
            .andThrow(new RuntimeException(""));

    replayAll();

    final AzureDataSegmentKiller killer = new AzureDataSegmentKiller(
        segmentConfig,
        inputDataConfig,
        accountConfig,
        azureStorage,
        azureCloudBlobIterableFactory
    );

    assertThrows(
        RuntimeException.class,
        () -> killer.kill(DATA_SEGMENT)
    );

    verifyAll();
  }

  @Test
  public void test_killAll_segmentConfigWithNullContainerAndPrefix_throwsISEException() throws Exception
  {
    EasyMock.expect(segmentConfig.getContainer()).andReturn(null).atLeastOnce();
    EasyMock.expect(segmentConfig.getPrefix()).andReturn(null).anyTimes();

    boolean thrownISEException = false;

    try {
      AzureDataSegmentKiller killer = new AzureDataSegmentKiller(
          segmentConfig,
          inputDataConfig,
          accountConfig,
          azureStorage,
          azureCloudBlobIterableFactory
      );
      EasyMock.replay(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);
      killer.killAll();
    }
    catch (ISE e) {
      thrownISEException = true;
    }

    assertTrue(thrownISEException);
    EasyMock.verify(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);
  }

  @Test
  public void test_killAll_noException_deletesAllSegments() throws Exception
  {
    EasyMock.expect(segmentConfig.getContainer()).andReturn(CONTAINER).atLeastOnce();
    EasyMock.expect(segmentConfig.getPrefix()).andReturn(PREFIX).atLeastOnce();
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();

    CloudBlobHolder blob1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);
    CloudBlobHolder blob2 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_2, TIME_1);

    AzureCloudBlobIterable azureCloudBlobIterable = AzureTestUtils.expectListObjects(
        azureCloudBlobIterableFactory,
        MAX_KEYS,
        PREFIX_URI,
        ImmutableList.of(blob1, blob2),
        azureStorage
    );

    EasyMock.replay(blob1, blob2);
    AzureTestUtils.expectDeleteObjects(
        azureStorage,
        ImmutableList.of(blob1, blob2),
        ImmutableMap.of(),
        MAX_TRIES
    );
    EasyMock.replay(segmentConfig, inputDataConfig, accountConfig, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);
    killer.killAll();
    EasyMock.verify(segmentConfig, inputDataConfig, accountConfig, blob1, blob2, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_deletesAllSegments()
  {
    boolean ioExceptionThrown = false;
    CloudBlobHolder cloudBlob = null;
    AzureCloudBlobIterable azureCloudBlobIterable = null;
    try {
      EasyMock.expect(segmentConfig.getContainer()).andReturn(CONTAINER).atLeastOnce();
      EasyMock.expect(segmentConfig.getPrefix()).andReturn(PREFIX).atLeastOnce();
      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).anyTimes();

      cloudBlob = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);

      azureCloudBlobIterable = AzureTestUtils.expectListObjects(
          azureCloudBlobIterableFactory,
          MAX_KEYS,
          PREFIX_URI,
          ImmutableList.of(cloudBlob),
          azureStorage
      );

      EasyMock.replay(cloudBlob);
      AzureTestUtils.expectDeleteObjects(
          azureStorage,
          ImmutableList.of(),
          ImmutableMap.of(cloudBlob, NON_RECOVERABLE_EXCEPTION),
          MAX_TRIES
      );
      EasyMock.replay(
          segmentConfig,
          inputDataConfig,
          accountConfig,
          azureCloudBlobIterable,
          azureCloudBlobIterableFactory,
          azureStorage
      );
      AzureDataSegmentKiller killer = new AzureDataSegmentKiller(
          segmentConfig,
          inputDataConfig,
          accountConfig,
          azureStorage,
          azureCloudBlobIterableFactory
      );
      killer.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    assertTrue(ioExceptionThrown);

    EasyMock.verify(
        segmentConfig,
        inputDataConfig,
        accountConfig,
        cloudBlob,
        azureCloudBlobIterable,
        azureCloudBlobIterableFactory,
        azureStorage
    );
  }

  @Test
  public void killBatchTest() throws SegmentLoadingException, BlobStorageException
  {
    Capture<List<String>> deletedFilesCapture = Capture.newInstance();
    EasyMock.expect(azureStorage.batchDeleteFiles(
        EasyMock.eq(CONTAINER_NAME),
        EasyMock.capture(deletedFilesCapture),
        EasyMock.eq(null)
    )).andReturn(true);

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);

    killer.kill(ImmutableList.of(DATA_SEGMENT, DATA_SEGMENT_2));

    verifyAll();

    assertEquals(ImmutableSet.of(BLOB_PATH, BLOB_PATH_2), new HashSet<>(deletedFilesCapture.getValue()));
  }

  @Test
  public void test_killBatch_runtimeException()
  {
    EasyMock.expect(azureStorage.batchDeleteFiles(CONTAINER_NAME, ImmutableList.of(BLOB_PATH, BLOB_PATH_2), null))
            .andThrow(new RuntimeException(""));

    replayAll();

    final AzureDataSegmentKiller killer = new AzureDataSegmentKiller(
        segmentConfig,
        inputDataConfig,
        accountConfig,
        azureStorage,
        azureCloudBlobIterableFactory
    );

    assertThrows(
        RuntimeException.class,
        () -> killer.kill(ImmutableList.of(DATA_SEGMENT, DATA_SEGMENT_2))
    );

    verifyAll();
  }

  @Test
  public void test_killBatch_SegmentLoadingExceptionOnError()
  {
    EasyMock.expect(azureStorage.batchDeleteFiles(CONTAINER_NAME, ImmutableList.of(BLOB_PATH, BLOB_PATH_2), null))
            .andReturn(false);

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(
        segmentConfig,
        inputDataConfig,
        accountConfig,
        azureStorage,
        azureCloudBlobIterableFactory
    );

    assertThrows(
        SegmentLoadingException.class,
        () -> killer.kill(ImmutableList.of(DATA_SEGMENT, DATA_SEGMENT_2))
    );

    verifyAll();
  }

  @Test
  public void killBatch_emptyList() throws SegmentLoadingException, BlobStorageException
  {
    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);
    killer.kill(ImmutableList.of());
  }

  @Test
  public void killBatch_singleSegment() throws SegmentLoadingException, BlobStorageException
  {
    List<String> deletedFiles = new ArrayList<>();
    final String dirPath = Paths.get(BLOB_PATH).getParent().toString();

    // For a single segment, fall back to regular kill(DataSegment) logic
    EasyMock.expect(azureStorage.emptyCloudBlobDirectory(CONTAINER_NAME, dirPath)).andReturn(deletedFiles);

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);

    killer.kill(ImmutableList.of(DATA_SEGMENT));

    verifyAll();
  }
}
