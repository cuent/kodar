/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucuenca.test.client;

import junit.framework.TestCase;

/**
 * Test web service client and REST service KODAR. Test it's gonna pass if the
 * WS is executed or not. If WS it isn't executed It'll throw an exception.
 *
 * @author Xavier Sumba
 */
public class TestWebService extends TestCase {

    public TestWebService(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testWebService() {
        WebServiceClientTest ws = new WebServiceClientTest();
        ws.execute("/Users/cuent/NetBeansProjects/KODAR/Job/datos_para_cluster.csv");
        ws.getFile("rdf", "/Users/cuent/Downloads");
    }
}
