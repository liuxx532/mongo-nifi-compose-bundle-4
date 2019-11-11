package com.compose.nifi.processors;


import com.mongodb.CursorType;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.gt;

/**
 * Created by liuguanxiong.
 */
@EventDriven
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
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
public class ComposeOplogGetMongo extends AbstractProcessor {
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
    _relationships.add(REL_FAILURE);
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
      transferToRelationship(session, flowFile, REL_FAILURE);
      return;
    }

    int tsValue = Integer.parseInt(flowFile.getAttribute(mongoWrapper.getTSKey(context)));

    BsonTimestamp bts = new BsonTimestamp(tsValue, 0);
    String dbName = mongoWrapper.getDatabase(context).getName();

    MongoCollection<Document> oplog = mongoWrapper.getLocalDatabase().getCollection("oplog.rs");
    try {
      final byte[] content = new byte[(int) flowFile.getSize()];
      session.read(flowFile, new InputStreamCallback() {
        @Override
        public void process(final InputStream in) throws IOException {
          StreamUtils.fillBuffer(in, content, true);
        }
      });


      FindIterable<Document> it = oplog.find(gt("ts", bts)).cursorType(CursorType.TailableAwait).oplogReplay(true).noCursorTimeout(true);
      MongoCursor<Document> cursor = it.iterator();
      Document currentDoc = cursor.next();
      getLogger().info("currentDoc: " + currentDoc);
      String[] namespace = currentDoc.getString("ns").split(Pattern.quote("."));

      if ( namespace[1] == null || namespace[1].equals("$cmd") || namespace[1].equals("system")) {
        transferToRelationship(session, flowFile, REL_FAILURE);
        return;
      }

      try {
        while(cursor.hasNext()){

          flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
          flowFile = session.putAttribute(flowFile, "mongo.id", getId(currentDoc));
          flowFile = session.putAttribute(flowFile, "mongo.ts", currentDoc.get("ts", BsonTimestamp.class).getTime() + "");
          flowFile = session.putAttribute(flowFile, "mongo.op", currentDoc.getString("op"));
          flowFile = session.putAttribute(flowFile, "mongo.db", dbName);
          flowFile = session.putAttribute(flowFile, "mongo.collection", namespace[1]);

          flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream outputStream) throws IOException {
              IOUtils.write(currentDoc.toJson().toString(), outputStream);
            }
          });
          session.getProvenanceReporter().receive(flowFile, mongoWrapper.getURI(context));
          session.transfer(flowFile, REL_SUCCESS);
          session.commit();
        }
      } finally {
        cursor.close();
      }
    } catch (Throwable t) {
      getLogger().error("{} failed to process due to {}; rolling back", new Object[] {this, t});
    }
  }

  //createIndex -> op=i
  private String getId(Document doc) {
    switch(doc.getString("op")) {
      case "i":
      case "d":
      case "u":
         return doc.get("o2", Document.class) == null ?
                 doc.get("o", Document.class).getObjectId("_id").toHexString() :
                 doc.get("o2", Document.class).getObjectId("_id").toHexString();
      case "n":
      case "c":
        return Long.toString(doc.getLong("h"));
      default:
        return "NA";
    }
  }

  private void transferToRelationship(ProcessSession session,
                                      FlowFile flowFile,
                                      Relationship relationship) {
    session.getProvenanceReporter().route(flowFile, relationship);
    session.transfer(flowFile, relationship);
  }
}
