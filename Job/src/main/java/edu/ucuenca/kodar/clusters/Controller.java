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
package edu.ucuenca.kodar.clusters;

import java.io.IOException;

/**
 *
 * @author Xavier Sumba <xavier.sumba93@ucuenca.ec>
 */
public interface Controller {

    public void seqDirectoryToText(String inputFile, String outputFile) throws IOException;

    public void seqDirectoryToLong(String inputFile, String outputFile) throws IOException;

    public void seq2Sparse(String[] seq2SparseArgs) throws Exception;

    public void kmeans(String[] kmeansArgs) throws Exception;

    public void clusterDumper(String[] clusterDumperArgs) throws Exception;

    public void collapsedVariationalBayes(String[] args) throws Exception;

    public void rowId(String[] rowIdArgs) throws Exception;

    public void vectorDump(String[] vectorDumpArgs) throws Exception;
}
