/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.search.scan;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class SearchScanScrollingTests extends AbstractSharedClusterTest {
    // TODO RANDOMIZE
    @Test
    public void shard1docs100size3() throws Exception {
        testScroll(1, 100, 3);
    }

    @Test
    public void shard1docs100size7() throws Exception {
        testScroll(1, 100, 7);
    }

    @Test
    public void shard1docs100size13() throws Exception {
        testScroll(1, 100, 13);
    }

    @Test
    public void shard1docs100size24() throws Exception {
        testScroll(1, 100, 24);
    }

    @Test
    public void shard1docs100size45() throws Exception {
        testScroll(1, 100, 45);
    }

    @Test
    public void shard1docs100size63() throws Exception {
        testScroll(1, 100, 63);
    }

    @Test
    public void shard1docs100size89() throws Exception {
        testScroll(1, 100, 89);
    }

    @Test
    public void shard1docs100size99() throws Exception {
        testScroll(1, 100, 99);
    }

    @Test
    public void shard1docs100size100() throws Exception {
        testScroll(1, 100, 100);
    }

    @Test
    public void shard1docs100size101() throws Exception {
        testScroll(1, 100, 101);
    }

    @Test
    public void shard1docs100size120() throws Exception {
        testScroll(1, 100, 120);
    }

    @Test
    public void shard3docs100size3() throws Exception {
        testScroll(3, 100, 3);
    }

    @Test
    public void shard3docs100size7() throws Exception {
        testScroll(3, 100, 7);
    }

    @Test
    public void shard3docs100size13() throws Exception {
        testScroll(3, 100, 13);
    }

    @Test
    public void shard3docs100size24() throws Exception {
        testScroll(3, 100, 24);
    }

    @Test
    public void shard3docs100size45() throws Exception {
        testScroll(3, 100, 45);
    }

    @Test
    public void shard3docs100size63() throws Exception {
        testScroll(3, 100, 63);
    }

    @Test
    public void shard3docs100size89() throws Exception {
        testScroll(3, 100, 89);
    }

    @Test
    public void shard3docs100size120() throws Exception {
        testScroll(3, 100, 120);
    }

    @Test
    public void shard3docs100size3Unbalanced() throws Exception {
        testScroll(3, 100, 3, true);
    }

    @Test
    public void shard3docs100size7Unbalanced() throws Exception {
        testScroll(3, 100, 7, true);
    }

    @Test
    public void shard3docs100size13Unbalanced() throws Exception {
        testScroll(3, 100, 13, true);
    }

    @Test
    public void shard3docs100size24Unbalanced() throws Exception {
        testScroll(3, 100, 24, true);
    }

    @Test
    public void shard3docs100size45Unbalanced() throws Exception {
        testScroll(3, 100, 45, true);
    }

    @Test
    public void shard3docs100size63Unbalanced() throws Exception {
        testScroll(3, 100, 63, true);
    }

    @Test
    public void shard3docs100size89Unbalanced() throws Exception {
        testScroll(3, 100, 89, true);
    }

    @Test
    public void shard3docs100size120Unbalanced() throws Exception {
        testScroll(3, 100, 120);
    }

    private void testScroll(int numberOfShards, long numberOfDocs, int size) throws Exception {
        testScroll(numberOfShards, numberOfDocs, size, false);
    }

    private void testScroll(int numberOfShards, long numberOfDocs, int size, boolean unbalanced) throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", numberOfShards)).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        Set<String> ids = Sets.newHashSet();
        Set<String> expectedIds = Sets.newHashSet();
        for (int i = 0; i < numberOfDocs; i++) {
            String id = Integer.toString(i);
            expectedIds.add(id);
            String routing = null;
            if (unbalanced) {
                if (i < (numberOfDocs * 0.6)) {
                    routing = "0";
                } else if (i < (numberOfDocs * 0.9)) {
                    routing = "1";
                } else {
                    routing = "2";
                }
            }
            client().prepareIndex("test", "type1", id).setRouting(routing).setSource("field", i).execute().actionGet();
            // make some segments
            if (i % 10 == 0) {
                client().admin().indices().prepareFlush().execute().actionGet();
            }
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.SCAN)
                .setQuery(matchAllQuery())
                .setSize(size)
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();

        assertThat(searchResponse.getHits().totalHits(), equalTo(numberOfDocs));

        // start scrolling, until we get not results
        while (true) {
            searchResponse = client().prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueMinutes(2)).execute().actionGet();
            assertThat(searchResponse.getHits().totalHits(), equalTo(numberOfDocs));
            assertThat(searchResponse.getFailedShards(), equalTo(0));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(hit.id() + "should not exists in the result set", ids.contains(hit.id()), equalTo(false));
                ids.add(hit.id());
            }
            if (searchResponse.getHits().hits().length == 0) {
                break;
            }
        }

        assertThat(expectedIds, equalTo(ids));
    }
}