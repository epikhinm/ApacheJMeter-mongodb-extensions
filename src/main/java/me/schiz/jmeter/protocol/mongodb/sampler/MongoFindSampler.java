/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package me.schiz.jmeter.protocol.mongodb.sampler;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.apache.jmeter.protocol.mongodb.config.MongoSourceElement;
import org.apache.jmeter.protocol.mongodb.mongo.MongoDB;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: schizophrenia
 * Date: 5/23/13
 * Time: 9:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class MongoFindSampler
        extends AbstractSampler
        implements TestBean {
    private static final Logger log =   LoggingManager.getLoggerForClass();

    public final static String SOURCE = "MongoFindSampler.source"; //$NON-NLS-1$

    public final static String DATABASE = "MongoFindSampler.database"; //$NON-NLS-1$
    public final static String USERNAME = "MongoFindSampler.username"; //$NON-NLS-1$
    public final static String PASSWORD = "MongoFindSampler.password"; //$NON-NLS-1$
    public final static String COLLECTION = "MongoFindSampler.collectiton"; //$NON-NLS-1$
    public final static String DOCUMENT = "MongoFindSampler.document"; //$NON-NLS-1$

    @Override
    public SampleResult sample(Entry e) {
        SampleResult res = new SampleResult();
        //String data = getScript();

        BasicDBObject document = (BasicDBObject) JSON.parse(getDocument());

        res.setSampleLabel(getTitle());
        res.setResponseCodeOK();
        res.setSuccessful(true);
        res.setResponseMessageOK();
        res.setSamplerData("db." + getCollection() + ".find(" + document+")");
        res.setDataType(SampleResult.TEXT);
        res.setContentType("text/plain"); // $NON-NLS-1$
        res.sampleStart();

        try {
            MongoDB mongoDB = MongoSourceElement.getMongoDB(getSource());
            DB db = mongoDB.getDB(getDatabase(), getUsername(), getPassword());
            res.latencyEnd();
            DBCursor cursor = db.getCollection(getCollection()).find(document);
            String response = new String();
            List<DBObject> resultList = cursor.toArray();
            if(resultList != null || resultList.isEmpty()) {
                for(DBObject o : resultList) {
                    response += o.toString() + "\n";
                }
                res.setResponseData(response.getBytes());
            } else {
                    res.setResponseCode("404");
                    res.setSuccessful(false);
            }
        } catch (Exception ex) {
            res.setResponseCode("500"); // $NON-NLS-1$
            res.setSuccessful(false);
            res.setResponseMessage(ex.toString());
            res.setResponseData(ex.getMessage().getBytes());
        } finally {
            res.sampleEnd();
        }
        return res;
    }

    public String getTitle() {
        return this.getName();
    }

    public String getDocument() {
        return getPropertyAsString(DOCUMENT);
    }

    public void setDocument(String document) {
        setProperty(DOCUMENT, document);
    }

    public String getCollection() {
        return getPropertyAsString(COLLECTION);
    }

    public void setCollection(String collection) {
        setProperty(COLLECTION, collection);
    }

    public String getDatabase() {
        return getPropertyAsString(DATABASE);
    }

    public void setDatabase(String database) {
        setProperty(DATABASE, database);
    }

    public String getUsername() {
        return getPropertyAsString(USERNAME);
    }

    public void setUsername(String username) {
        setProperty(USERNAME, username);
    }

    public String getPassword() {
        return getPropertyAsString(PASSWORD);
    }

    public void setPassword(String password) {
        setProperty(PASSWORD, password);
    }

    public String getSource() {
        return getPropertyAsString(SOURCE);
    }

    public void setSource(String source) {
        setProperty(SOURCE, source);
    }
}
