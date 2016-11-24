/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.ycsb.db;

import com.mongodb.*;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.netty.NettyStreamFactoryFactory;
import org.bson.Document;
import com.mongodb.client.model.Filters;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.descending;

/**
 * The QuickTour code example see: https://mongodb.github.io/mongo-java-driver/3.0/getting-started
 */
public class AsyncMongoDbClient extends DB {
    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = Integer.valueOf(1);

    /** A singleton MongoClient instance. */
    private static MongoClient mongo;

    private static MongoDatabase db;

    private static int serverCounter = 0;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    /** The default read preference for the test */
    private static ReadPreference readPreference;

    /** Allow inserting batches to save time during load */
    private static Integer BATCHSIZE;


    /** The database to access. */
    private static String database;

    /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    /** Measure of how compressible the data is, compressibility=10 means the data can compress tenfold.
     *  The default is 1, which is uncompressible */
    private static float compressibility = (float) 1.0;

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongo != null) {
                return;
            }
            // use netty nio
            System.setProperty("org.mongodb.async.type", "netty");

            // initialize MongoDb driver
            Properties props = getProperties();
            String urls = props.getProperty("mongodb.url", "localhost:27017");

            database = props.getProperty("mongodb.database", "ycsb");

            // Set insert batchsize, default 1 - to be YCSB-original equivalent
            final String batchSizeString = props.getProperty("batchsize", "1");
            BATCHSIZE = Integer.parseInt(batchSizeString);

            final String compressibilityString = props.getProperty("compressibility", "1");
            this.compressibility = Float.parseFloat(compressibilityString);


            try {
                if (urls.startsWith("mongodb://")) {
                    mongo = MongoClients.create(new ConnectionString(urls));
                } else {
                    System.err.println("error: start with monogdb:// ");
                    return;
                }
                db = mongo.getDatabase(database);
                System.out.println("mongo connection created with " + urls);

                final CountDownLatch dropLatch = new CountDownLatch(1);
                db.drop(new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        dropLatch.countDown();
                    }
                });
                dropLatch.await();

            } catch (Exception e1) {
                System.err.println("Could not initialize MongoDB connection pool for Loader: "
                        + e1.toString());
                e1.printStackTrace();
                return;
            }

        }
    }

    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            try {
                mongo.close();
            } catch (Exception e1) { /* ignore */ }
        }
    }

    private byte[] applyCompressibility(byte[] data){
        if (Float.compare(compressibility, 1.0f) == 0) {
            return data;
        }
        long string_length = data.length;

        long random_string_length = (int) Math.round(string_length /compressibility);
        long compressible_len = string_length - random_string_length;
        for(int i=0;i<compressible_len;i++)
            data[i] = 0;
        return data;
    }

    @Override
    public int read(String table, String key, Set<String> field,
                    HashMap<String, ByteIterator> result) {
        final CountDownLatch dropLatch = new CountDownLatch(1);
        // find first
        SingleResultCallback<Document> printDocument = new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document document, final Throwable t) {
                //System.out.println(document.toJson());
                //document.putAll(result);
                dropLatch.countDown();
            }
        };

        MongoCollection<Document> collection = db.getCollection(table);
        if (field != null) {
            List<String> fieldNames = new ArrayList<String>(field);
            collection.find(Filters.eq("_id", key))
                      .projection(include(fieldNames))
                      .first(printDocument);
        } else {
            collection.find(Filters.eq("_id", key))
                      .first(printDocument);
        }

        try {
            dropLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
                    Set<String> field, Vector<HashMap<String, ByteIterator>> result) {
        try {
            final CountDownLatch dropLatch = new CountDownLatch(1);
            Block<Document> printDocumentBlock = new Block<Document>() {
                @Override
                public void apply(final Document document) {
//                    HashMap<String, ByteIterator> item = new HashMap<String, ByteIterator>();
//                    document.putAll(item);
                    //result.add(item);
                }
            };
            SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    dropLatch.countDown();
                }
            };
            MongoCollection<Document> collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            List<String> fieldNames = new ArrayList<String>(field);
            collection.find(gte("_id", startkey))
                    .projection(include(fieldNames))
                    .sort(descending("_id"))
                    .limit(recordcount)
                    .forEach(printDocumentBlock,callbackWhenFinished);


            dropLatch.await();
            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {

        }
    }

    @Override
    public int update(String table, String key,
                      HashMap<String, ByteIterator> values) {
        try {
            final CountDownLatch dropLatch = new CountDownLatch(1);
            SingleResultCallback<UpdateResult> printDocument = new SingleResultCallback<UpdateResult>() {
                @Override
                public void onResult(final UpdateResult result, final Throwable t) {
                    dropLatch.countDown();
                }
            };
            MongoCollection<Document> collection = db.getCollection(table);
            Document fieldsToSet = new Document();
            Iterator<String> keys = values.keySet().iterator();
            while (keys.hasNext()) {
                String tmpKey = keys.next();
                byte[] data = values.get(tmpKey).toArray();
                fieldsToSet.append(tmpKey, applyCompressibility(data));
            }

            collection.updateOne(eq("_id", key), fieldsToSet, printDocument);
            dropLatch.await();
        } catch (Exception e) {
            System.err.println(e.toString());
            e.printStackTrace();
        }
        return 0;

    }

    @Override
    public int insert(String table, String key,
                      HashMap<String, ByteIterator> values) {
        final CountDownLatch dropLatch = new CountDownLatch(1);
        MongoCollection<Document> collection = db.getCollection(table);
        Document r = new Document("_id", key);
        for (String k : values.keySet()) {
            byte[] data = values.get(k).toArray();
            r.append(k, applyCompressibility(data));
        }

        collection.insertOne(r, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    System.err.println("Couldn't insert key ");
                    t.printStackTrace();
                    return;
                }
                dropLatch.countDown();
            }
        });
        try {
            dropLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 0;

    }

    @Override
    public int delete(String table, String key) {
        final CountDownLatch dropLatch = new CountDownLatch(1);
        MongoCollection<Document> collection = db.getCollection(table);
        collection.deleteOne(Filters.eq("_id", key), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                dropLatch.countDown();
            }
        });
        try {
            dropLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 0;
    }
}

