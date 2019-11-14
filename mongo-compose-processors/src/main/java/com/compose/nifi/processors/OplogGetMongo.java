package com.compose.nifi.processors;


import com.google.gson.JsonArray;
import com.mongodb.CursorType;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.ne;

/**
 * Created by liuguanxiong on 8/25/16.
 */
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"4.0","oplog","compose", "mongodb", "get"})
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "This is the content-type for the content."),
        @WritesAttribute(attribute = "mongo.id", description = "The MongoDB object_id for the document in hex format or the 'h' from the oplog document."),
        @WritesAttribute(attribute = "mongo.ts", description = "Timestamp of operation from oplog or timestamp of query prior to tailing."),
        @WritesAttribute(attribute = "mongo.op", description = "The Mongo operation. `i' for insert, 'd' for delete, 'u' for update, 'q' which is a placeholder for query result when not an oplog operation"),
        @WritesAttribute(attribute = "mongo.db", description = "The Mongo database name"),
        @WritesAttribute(attribute = "mongo.collection", description = "The Mongo collection name")
})
@CapabilityDescription("Dumps documents from a MongoDB and then dumps operations from the oplog in soft real time. The FlowFile content is the document itself from the find or the `o` attribute from the oplog. It keeps a connection open and waits on new oplog entries. Restart does the full dump again and then oplog tailing.")
public class OplogGetMongo extends AbstractProcessor {
  private static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("the happy path for mongo documents and operations").build();
  private static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("the unhappy path. check for appropriate attributes among other things.").build();

  private static final Set<Relationship> relationships;

  private static final List<PropertyDescriptor> propertyDescriptors;

  static {
    List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
    _propertyDescriptors.addAll(MongoWrapper.oplogDescriptors);
    propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

    Set<Relationship> _relationships = new HashSet<>();
    _relationships.add(REL_SUCCESS);
    relationships = Collections.unmodifiableSet(_relationships);
  }

  private MongoWrapper mongoWrapper;

  @Override
  public final Set<Relationship> getRelationships() {
    return relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return propertyDescriptors;
  }

  @OnScheduled
  public final void createClient(ProcessContext context) throws IOException {
    mongoWrapper = new MongoWrapper();
    mongoWrapper.createClient(context);
  }

  @OnStopped
  public final void closeClient() {
    mongoWrapper.closeClient();
  }

  @Override
  public final void onTrigger(final ProcessContext context, final ProcessSession session) {
    FlowFile flowFile = session.get();
    if(flowFile == null) {
      session.getProvenanceReporter().receive(flowFile, mongoWrapper.getURI(context));
      session.transfer(flowFile, REL_FAILURE);
      return;
    }

    getLogger().info("ts key: " + mongoWrapper.getTSKey(context));
    getLogger().info(flowFile.getAttribute(mongoWrapper.getTSKey(context)));

    String tsKey = mongoWrapper.getTSKey(context);
    int tsValue = Integer.parseInt(flowFile.getAttribute(tsKey));

    MongoCollection<Document> oplog = mongoWrapper.getLocalDatabase().getCollection("oplog.rs");
    try {
      BsonTimestamp givenTs = new BsonTimestamp( tsValue, 0);
      FindIterable<Document> it = oplog.find(and(gt("ts", givenTs),ne("op","n")))
              .cursorType(CursorType.NonTailable).oplogReplay(true).noCursorTimeout(true);
      MongoCursor<Document> cursor = it.iterator();
      try {
        JsonArray jsonArray = new JsonArray();
        while(cursor.hasNext()){
          Document currentDoc = cursor.next();
          getLogger().info("currentDoc: " + currentDoc);
          jsonArray.add(currentDoc.toJson());
        }

        String endTsValue = null;

        if (jsonArray.size() > 0) {
          endTsValue = jsonArray.get(jsonArray.size()-1).getAsJsonObject()
                  .getAsJsonObject("ts")
                  .getAsJsonObject("$timestamp")
                  .get("t").toString();
        }

        getLogger().info("jsonArray: " + jsonArray.toString());
        flowFile = session.putAttribute(flowFile, tsKey, endTsValue);
        flowFile = session.write(flowFile, new OutputStreamCallback() {
          @Override
          public void process(OutputStream outputStream) throws IOException {
            outputStream.write(jsonArray.toString().getBytes());
          }
        });
        session.transfer(flowFile, REL_SUCCESS);
      } finally {
        cursor.close();
      }
    } catch (Throwable t) {
      getLogger().error("{} failed to process due to {}; rolling back", new Object[] {this, t});
    }
  }

}