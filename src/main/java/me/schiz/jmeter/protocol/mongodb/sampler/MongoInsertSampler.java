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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import org.apache.jmeter.protocol.mongodb.config.MongoSourceElement;
import org.apache.jmeter.protocol.mongodb.mongo.MongoDB;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class MongoInsertSampler
        extends AbstractSampler
        implements TestBean {
    private static final Logger log =   LoggingManager.getLoggerForClass();

    public final static String SOURCE = "MongoInsertSampler.source"; //$NON-NLS-1$

    public final static String DATABASE = "MongoInsertSampler.database"; //$NON-NLS-1$
    public final static String USERNAME = "MongoInsertSampler.username"; //$NON-NLS-1$
    public final static String PASSWORD = "MongoInsertSampler.password"; //$NON-NLS-1$
    public final static String COLLECTION = "MongoInsertSampler.collectiton"; //$NON-NLS-1$
    public final static String DOCUMENT = "MongoInsertSampler.document"; //$NON-NLS-1$

    @Override
    public SampleResult sample(Entry e) {
        SampleResult res = new SampleResult();
        //String data = getScript();

        res.setSampleLabel(getTitle());
        res.setResponseCodeOK();
        res.setSuccessful(true);
        res.setResponseMessageOK();
        res.setSamplerData("db." + getCollection() + ".insert(<json>)");
        res.setDataType(SampleResult.TEXT);
        res.setContentType("text/plain"); // $NON-NLS-1$
        res.sampleStart();

        try {
            MongoDB mongoDB = MongoSourceElement.getMongoDB(getSource());
            //MongoScriptRunner runner = new MongoScriptRunner();
            DB db = mongoDB.getDB(getDatabase(), getUsername(), getPassword());
            res.latencyEnd();
            BasicDBObject document = (BasicDBObject) JSON.parse(getDocument());
            WriteResult wResult = db.getCollection(getCollection()).insert(document);
//            latencyEnd result = runner.evaluate(db, data);
//            EvalResultHandler handler = new EvalResultHandler();
//            String resultAsString = handler.handle(result);
//            res.setResponseData(resultAsString.getBytes());
            res.setResponseData(wResult.toString().getBytes());
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
