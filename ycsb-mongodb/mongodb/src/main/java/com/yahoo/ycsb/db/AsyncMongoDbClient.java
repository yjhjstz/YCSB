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
import org.bson.Document;
import com.mongodb.client.model.Filters;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.measurements.Measurements;

import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.descending;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
/**
 * The QuickTour code example see: https://mongodb.github.io/mongo-java-driver/3.0/getting-started
 */
public class AsyncMongoDbClient extends DB {
    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = Integer.valueOf(1);

    /** A singleton MongoClient instance. */
    private static MongoClient mongo;

    private static MongoDatabase db;

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

    private static final Measurements _measurements = Measurements.getMeasurements();

    private static Integer dropData;

    private static Semaphore _semaphore = null;
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

            final String dropDataBase = props.getProperty("mongodb.drop", "0");
            dropData = Integer.parseInt(dropDataBase);

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
                // drop database
                if (dropData == 1) {
                    final CountDownLatch dropLatch = new CountDownLatch(1);
                    db.drop(new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final Throwable t) {
                            dropLatch.countDown();
                        }
                    });
                    dropLatch.await();
                }
                // init semaphore count, control insert rate
                int MaxWaitQueueSize = mongo.getSettings().getConnectionPoolSettings().getMaxWaitQueueSize();
                System.out.println("max wait queue: " + MaxWaitQueueSize);
                _semaphore = new Semaphore(MaxWaitQueueSize);

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
    // control speed
    private int acquireTicket() {
        try {
            _semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
    }

    private void releaseTicket() {
        _semaphore.release();
    }

    @Override
    public int read(String table, String key, Set<String> field,
                    HashMap<String, ByteIterator> result) {
        acquireTicket();
        final long st = System.nanoTime();
        SingleResultCallback<Document> printDocument = new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document document, final Throwable t) {
                int ret = 0;
                if (t != null) {
                    System.err.println("Couldn't read key ");
                    ret = 1;
                }
                releaseTicket();
                long en = System.nanoTime();
                _measurements.measure("READ", (int)((en-st)/1000));
                _measurements.reportReturnCode("READ", ret);

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

        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
                    Set<String> field, Vector<HashMap<String, ByteIterator>> result) {
        try {
            acquireTicket();
            final long st=System.nanoTime();
            Block<Document> printDocumentBlock = new Block<Document>() {
                @Override
                public void apply(final Document document) {

                }
            };
            SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    int ret = 0;
                    if (t != null) {
                        System.err.println("Couldn't scan key ");
                        t.printStackTrace();
                        ret = 1;
                    }
                    releaseTicket();
                    long en=System.nanoTime();
                    _measurements.measure("SCAN", (int)((en-st)/1000));
                    _measurements.reportReturnCode("SCAN", ret);

                }
            };
            MongoCollection<Document> collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            List<String> fieldNames = new ArrayList<String>(field);
            collection.find(gte("_id", startkey))
                    .projection(include(fieldNames))
                    .sort(descending("_id"))
                    .limit(recordcount)
                    .forEach(printDocumentBlock, callbackWhenFinished);

            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }

    }

    @Override
    public int update(String table, String key,
                      HashMap<String, ByteIterator> values) {
        try {
            acquireTicket();
            final long st = System.nanoTime();
            SingleResultCallback<UpdateResult> printDocument = new SingleResultCallback<UpdateResult>() {
                @Override
                public void onResult(final UpdateResult result, final Throwable t) {
                    int ret = 0;
                    if (t != null) {
                        System.err.println("Couldn't update key ");
                        t.printStackTrace();
                        ret = 1;
                    }
                    releaseTicket();
                    long en=System.nanoTime();
                    _measurements.measure("UPDATE", (int)((en-st)/1000));
                    _measurements.reportReturnCode("UPDATE", ret);

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

            collection.replaceOne(eq("_id", key), fieldsToSet, printDocument);

        } catch (Exception e) {
            System.err.println(e.toString());
            e.printStackTrace();
        }
        return 0;

    }

    @Override
    public int insert(String table, String key,
                      HashMap<String, ByteIterator> values) {
        acquireTicket();
        final long st = System.nanoTime();

        MongoCollection<Document> collection = db.getCollection(table);
        Document r = new Document("_id", key);
        for (String k : values.keySet()) {
            byte[] data = values.get(k).toArray();
            r.append(k, applyCompressibility(data));
        }

        collection.insertOne(r, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                int ret = 0;
                if (t != null) {
                    System.err.println("Couldn't insert key ");
                    ret = 1;
                }
                releaseTicket();
                long en = System.nanoTime();
                _measurements.measure("INSERT", (int)((en-st)/1000));
                _measurements.reportReturnCode("INSERT", ret);

            }
        });

        return 0;

    }

    @Override
    public int delete(String table, String key) {
        acquireTicket();
        final long st = System.nanoTime();
        MongoCollection<Document> collection = db.getCollection(table);
        collection.deleteOne(Filters.eq("_id", key), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                int ret = 0;
                if (t != null) {
                    ret = 1;
                }
                releaseTicket();
                long en = System.nanoTime();
                _measurements.measure("DELETE", (int) ((en - st) / 1000));
                _measurements.reportReturnCode("DELETE", ret);

            }
        });
        return 0;
    }

    @Override
    public void runCommand(String cmd) {
        acquireTicket();
        final long st = System.nanoTime();
        db.runCommand(new Document("ping", 1), new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                int ret = 0;
                if (t != null) {
                    ret = 1;
                }
                releaseTicket();
                long en = System.nanoTime();
                _measurements.measure("CMD", (int) ((en - st) / 1000));
                _measurements.reportReturnCode("CMD", ret);
            }
        });
    }

    @Override
    public void warmUp() {
        acquireTicket();
        db.runCommand(new Document("ping", 1), new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                releaseTicket();
            }
        });

    }

    @Override
    public boolean ready() {
        acquireTicket();
        final CountDownLatch dropLatch = new CountDownLatch(1);
        final long[] cc = {0};
        db.runCommand(new Document("serverStatus", 1), new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {

                if (t == null) {
                    final Document conn =  (Document)result.get("connections");
                    JSONObject jsonObject = (JSONObject)JSONValue.parse(conn.toJson());
                    cc[0] = (Long)jsonObject.get("current");
                }
                releaseTicket();
                dropLatch.countDown();
            }
        });
        try {
            dropLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long maxC = mongo.getSettings().getConnectionPoolSettings().getMaxWaitQueueSize();
        boolean ready = cc[0] >= maxC - 10;
        System.err.println("current conn: " + cc[0] + ", ready:" + ready);
        return ready ;
    }

}

