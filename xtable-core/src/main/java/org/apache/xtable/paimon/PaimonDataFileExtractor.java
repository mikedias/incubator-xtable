/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.paimon;

import static org.apache.xtable.paimon.PaimonSourceConfig.PaimonEmitFilesMode.CHANGELOG;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import lombok.extern.log4j.Log4j2;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;

import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;

@Log4j2
public class PaimonDataFileExtractor {

  private final PaimonPartitionExtractor partitionExtractor =
      PaimonPartitionExtractor.getInstance();

  private final PaimonStatsExtractor statsExtractor = PaimonStatsExtractor.getInstance();

  private static final PaimonDataFileExtractor INSTANCE = new PaimonDataFileExtractor();

  public static PaimonDataFileExtractor getInstance() {
    return INSTANCE;
  }

  public List<InternalDataFile> toInternalDataFiles(
      FileStoreTable table,
      Snapshot snapshot,
      InternalSchema internalSchema,
      PaimonSourceConfig sourceConfig) {
    List<InternalDataFile> result = new ArrayList<>();
    Iterator<ManifestEntry> manifestEntryIterator =
        manifestEntryIterator(table, snapshot, sourceConfig);
    while (manifestEntryIterator.hasNext()) {
      result.add(toInternalDataFile(table, manifestEntryIterator.next(), internalSchema));
    }
    return result;
  }

  /**
   * Converts a Paimon ManifestEntry to an InternalDataFile. This method is used for both full
   * snapshot reads and incremental sync.
   *
   * @param table the Paimon table
   * @param entry the manifest entry representing a data file
   * @param internalSchema the internal schema for partition value extraction
   * @return InternalDataFile representation
   */
  public InternalDataFile toInternalDataFile(
      FileStoreTable table, ManifestEntry entry, InternalSchema internalSchema) {
    return InternalDataFile.builder()
        .physicalPath(toFullPhysicalPath(table, entry))
        .fileSizeBytes(entry.file().fileSize())
        .lastModified(entry.file().creationTimeEpochMillis())
        .recordCount(entry.file().rowCount())
        .partitionValues(
            partitionExtractor.toPartitionValues(table, entry.partition(), internalSchema))
        .columnStats(statsExtractor.extractColumnStats(entry.file(), internalSchema))
        .build();
  }

  private String toFullPhysicalPath(FileStoreTable table, ManifestEntry entry) {
    String basePath = table.location().toString();
    String bucketPath = "bucket-" + entry.bucket();
    String filePath = entry.file().fileName();

    Optional<String> partitionPath = partitionExtractor.toPartitionPath(table, entry.partition());
    if (partitionPath.isPresent()) {
      return String.join("/", basePath, partitionPath.get(), bucketPath, filePath);
    } else {
      return String.join("/", basePath, bucketPath, filePath);
    }
  }

  /**
   * Extracts file changes (added and removed files) from delta manifests for a given snapshot. This
   * method reads only the delta manifests which contain the changes introduced in this specific
   * snapshot, making it efficient for incremental sync.
   *
   * @param table the Paimon table
   * @param snapshot the snapshot to extract changes from
   * @param internalSchema the internal schema for partition value extraction
   * @return InternalFilesDiff containing added and removed files
   */
  public InternalFilesDiff extractFilesDiff(
      FileStoreTable table, Snapshot snapshot, InternalSchema internalSchema) {

    ManifestList manifestList = table.store().manifestListFactory().create();
    ManifestFile manifestFile = table.store().manifestFileFactory().create();

    // Read delta manifests - these contain only the changes in this snapshot
    List<ManifestFileMeta> deltaManifests = manifestList.readDeltaManifests(snapshot);
    log.debug("Found {} delta manifests for snapshot {}", deltaManifests.size(), snapshot.id());

    Set<InternalDataFile> addedFiles = new HashSet<>();
    Set<InternalDataFile> removedFiles = new HashSet<>();

    // For primary key tables, only consider top-level files (fully compacted)
    int topLevel = table.coreOptions().numLevels() - 1;
    boolean hasPrimaryKeys = !table.schema().primaryKeys().isEmpty();

    for (ManifestFileMeta manifestMeta : deltaManifests) {
      List<ManifestEntry> entries = manifestFile.read(manifestMeta.fileName());
      log.debug("Processing {} manifest entries from {}", entries.size(), manifestMeta.fileName());

      for (ManifestEntry entry : entries) {
        if (hasPrimaryKeys && entry.file().level() != topLevel) {
          continue;
        }

        InternalDataFile dataFile = toInternalDataFile(table, entry, internalSchema);
        if (entry.kind() == FileKind.ADD) {
          addedFiles.add(dataFile);
        } else if (entry.kind() == FileKind.DELETE) {
          removedFiles.add(dataFile);
        }
      }
    }

    log.info(
        "Snapshot {} has {} files added and {} files removed",
        snapshot.id(),
        addedFiles.size(),
        removedFiles.size());

    return InternalFilesDiff.builder().filesAdded(addedFiles).filesRemoved(removedFiles).build();
  }

  private Iterator<ManifestEntry> manifestEntryIterator(
      FileStoreTable table, Snapshot snapshot, PaimonSourceConfig sourceConfig) {
    if (sourceConfig.getEmitFilesMode().equals(CHANGELOG)) {
      return readAllChangelogEntries(table, snapshot);
    } else if (!table.schema().primaryKeys().isEmpty()) {
      // If the table has primary keys, we read only the top level files
      // which means we can only consider fully compacted files.
      return table
          .newSnapshotReader()
          .withSnapshot(snapshot)
          .withLevel(table.coreOptions().numLevels() - 1)
          .readFileIterator();
    } else {
      return table.newSnapshotReader().withSnapshot(snapshot).readFileIterator();
    }
  }

  /**
   * Reads all changelog entries from all snapshots up to and including the given snapshot. This
   * method iterates through all snapshots and collects changelog manifests from those that have
   * them (typically APPEND snapshots), regardless of compaction.
   *
   * @param table the Paimon table
   * @param upToSnapshot the snapshot up to which to read changelogs
   * @return Iterator of ManifestEntry containing all changelog entries
   */
  private Iterator<ManifestEntry> readAllChangelogEntries(
      FileStoreTable table, Snapshot upToSnapshot) {
    ManifestList manifestList = table.store().manifestListFactory().create();
    ManifestFile manifestFile = table.store().manifestFileFactory().create();

    List<ManifestEntry> allChangelogEntries = new ArrayList<>();

    try {
      // Iterate through all snapshots up to the given snapshot
      Iterator<Snapshot> snapshotIterator = table.snapshotManager().snapshots();
      while (snapshotIterator.hasNext()) {
        Snapshot snapshot = snapshotIterator.next();

        // Only process snapshots up to the requested snapshot
        if (snapshot.id() > upToSnapshot.id()) {
          break;
        }

        // Check if this snapshot has changelog manifests
        if (snapshot.changelogManifestList() != null) {
          List<ManifestFileMeta> changelogManifests = manifestList.readChangelogManifests(snapshot);
          log.debug(
              "Snapshot {} has {} changelog manifests", snapshot.id(), changelogManifests.size());

          for (ManifestFileMeta manifestMeta : changelogManifests) {
            List<ManifestEntry> entries = manifestFile.read(manifestMeta.fileName());
            allChangelogEntries.addAll(entries);
          }
        }
      }
    } catch (Exception e) {
      log.error("Failed to read changelog entries", e);
      throw new RuntimeException("Failed to read changelog entries from Paimon table", e);
    }

    log.info(
        "Read {} total changelog entries up to snapshot {}",
        allChangelogEntries.size(),
        upToSnapshot.id());
    return allChangelogEntries.iterator();
  }
}
