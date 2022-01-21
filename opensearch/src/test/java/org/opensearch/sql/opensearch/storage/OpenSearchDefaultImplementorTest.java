/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

@ExtendWith(MockitoExtension.class)
public class OpenSearchDefaultImplementorTest {

  @Mock
  OpenSearchIndexScan indexScan;
  @Mock
  OpenSearchClient client;

  /**
   * For test coverage.
   */
  @Test
  public void visitInvalidTypeShouldThrowException() {
    final OpenSearchIndex.OpenSearchDefaultImplementor implementor =
        new OpenSearchIndex.OpenSearchDefaultImplementor(indexScan, client);

    final IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> implementor.visitNode(relation("index"),
            indexScan));
    ;
    assertEquals(
        "unexpected plan node type "
            + "class org.opensearch.sql.planner.logical.LogicalRelation",
        exception.getMessage());
  }
}
