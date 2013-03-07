/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.indices.template;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.cluster.metadata.AliasMetaData.newAliasMetaDataBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Arrays;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 */
public class IndexTemplateAliasTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node2");
    }

    @Test
    public void indexTemplateAliasTest() throws Exception {
        clean();
        
        client.admin().indices().preparePutTemplate("template_with_aliases")
        		.setTemplate("te*")
                .addAlias("simple_alias", newAliasMetaDataBuilder("simple_alias").build())
                .addAlias("filter_alias", newAliasMetaDataBuilder("filter_alias")
                		.filter("{\"type\":{\"value\":\"type2\"}}")
                		.build())
                .execute().actionGet();

        // index something into test_index, will match on template
        client.index(indexRequest("test_index").type("type1").id("1").source("A", "A value").refresh(true)).actionGet();
        client.index(indexRequest("test_index").type("type2").id("2").source("B", "B value").refresh(true)).actionGet();
        
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        
        AliasMetaData aliasMetaData = ((InternalNode) node("node1")).injector().getInstance(ClusterService.class).state().metaData().aliases().get("filter_alias").get("test_index");
        assertThat(aliasMetaData.alias(), equalTo("filter_alias"));
        assertThat(aliasMetaData.filter(), notNullValue(CompressedString.class));
        
        // Search the simple alias
        SearchResponse searchResponse = client.prepareSearch("simple_alias")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("failed search " + Arrays.toString(searchResponse.getShardFailures()));
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
        
        // Search the filter alias expecting only one result of "type2"
        searchResponse = client.prepareSearch("filter_alias")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        if (searchResponse.getFailedShards() > 0) {
            logger.warn("failed search " + Arrays.toString(searchResponse.getShardFailures()));
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).type(), equalTo("type2"));
      }

    private void clean() {
        try {
            client.admin().indices().prepareDelete("test_index").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        try {
            client.admin().indices().prepareDeleteTemplate("template_1").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
    }
}
