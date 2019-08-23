package com.couchbase.assignment;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.*;
import lombok.NonNull;
import rx.Observable;
import rx.functions.Func1;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DocumentLoader {

    private final Cluster cluster;
    private final String userName;

    public DocumentLoader(final @NonNull String userName, final @NonNull String password) {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment
                .builder()
                .mutationTokensEnabled(true)
                .computationPoolSize(5)
                .build();
        //Assuming this setup in on the local host, i have hard-code it. Else could use nodes as parameter
        this.cluster = CouchbaseCluster.create(env, "localhost");
        this.cluster.authenticate(userName, password);
        this.userName = userName;
    }

    public void upsertDocuments(final @NonNull String bucketName, final int documentCount) throws Exception {
        if (documentCount < 1 ){
            throw new Exception("'documentCount' should be greater than 0!");
        }

        final Bucket bucket = cluster.openBucket(bucketName);

        List<JsonDocument> documents = new ArrayList<JsonDocument>();
        for(int i = 0; i < documentCount; i++) {
            /**
             * For this assignment just generating some random values for the documents.
             * In real-world scenario, it could be from a file or another user defined input source.
             */
            JsonObject content = JsonObject.create()
                    .put("id", UUID.randomUUID().toString())
                    .put("creationTime", System.currentTimeMillis())
                    .put("author", userName);
            documents.add(JsonDocument.create("document-"+i, content));
        }

        // Insert them in a single batch, waiting until the last one is done.
        Observable
                .from(documents)
                .flatMap(new Func1<JsonDocument, Observable<?>>() {
                    public Observable<JsonDocument> call(final JsonDocument docToInsert) {
                        /**
                         * For this assignment purpose using upsert instead of insert as we're just inserting
                         * random arbitrary values with the same document id's, easy to clean up for this testing
                         */

                        return bucket.async().upsert(docToInsert);
                    }
                })
                .last()
                .toBlocking()
                .single();
    }

    public void getAllDocuments(@NonNull final String bucketName) {
        System.out.println("Fetching all the documents from Bucket - "+bucketName);
        final Bucket bucket = cluster.openBucket(bucketName);
        // Create a N1QL Primary Index (but ignore if it exists)
        bucket.bucketManager().createN1qlPrimaryIndex(true, false);

        // Perform a N1QL Query
        N1qlQueryResult result = bucket.query(
                N1qlQuery.simple("SELECT * FROM `default`")
                /*N1qlQuery.parameterized("SELECT name FROM `default` WHERE $1 IN interests",
                        JsonArray.from("African Swallows"))*/
        );

        // Print each found Row
        for (N1qlQueryRow row : result) {
            // Prints {"name":"Arthur"}
            System.out.println(row);
        }


        /*// Perform a N1QL Query
        /*N1qlQueryResult result = bucket.query(
                N1qlQuery.simple("SELECT name FROM `default`")
                /*N1qlQuery.parameterized("SELECT name FROM `default` WHERE $1 IN interests",
                        JsonArray.from("African Swallows"))*
        //);
        bucket.async()
                .query(Select.select("*").from(bucketName).limit(10))
                .flatMap(result ->
                        result.errors()
                                .flatMap(e -> Observable.<AsyncN1qlQueryRow>error(new CouchbaseException("N1QL Error/Warning: " + e)))
                                .switchIfEmpty(result.rows())
                )
                .map(AsyncN1qlQueryRow::value)
                .subscribe(
                        rowContent -> System.out.println(rowContent),
                        runtimeError -> runtimeError.printStackTrace()
                );*/
    }

    public JsonDocument getDocumentById(@NonNull final String bucketName, @NonNull final String documentId) {
        final Bucket bucket = cluster.openBucket(bucketName);
        return bucket.get(documentId);
    }
}
