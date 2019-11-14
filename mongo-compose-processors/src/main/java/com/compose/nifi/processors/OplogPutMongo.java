package com.compose.nifi.processors;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by hayshutton on 8/26/16.
 */
@EventDriven
@TriggerSerially
@Tags({"compose", "mongodb", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Puts Documents and Operations into a MongoDB. Relies on attributes to decide on type of operation such as insert, update, or delete.")
public class OplogPutMongo extends AbstractProcessor {
  private static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("the unhappy path. check for appropriate attributes among other things.").build();
  private static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("the happy path. probably auto terminate it.").build();

  private static final Set<Relationship> relationships;

  private static final List<PropertyDescriptor> propertyDescriptors;

  static {
    List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
    _propertyDescriptors.addAll(MongoWrapper.descriptors);
    _propertyDescriptors.add(MongoWrapper.WRITE_CONCERN);
    propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

    Set<Relationship> _relationships = new HashSet<>();
    _relationships.add(REL_FAILURE);
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
      transferToRelationship(session, flowFile, REL_FAILURE);
      return;
    }
    String dbName = flowFile.getAttribute("mongo.db");
    String collectionName = flowFile.getAttribute("mongo.collection");

    WriteConcern writeConcern = mongoWrapper.getWriteConcern(context);
    MongoCollection<Document> collection = mongoWrapper.getDatabase(dbName).getCollection(collectionName).withWriteConcern(writeConcern);

    try {
                  // Read the contents of the FlowFile into a byte array
        final byte[] content = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, content, true);
            }
        });
        JSONArray jsonArray = (JSONArray)(new JSONParser().parse(new String(content)));
        getLogger().info("jsonArray: " + jsonArray);

        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject obj = (JSONObject)(new JSONParser().parse(jsonArray.get(i).toString()));
            obj.get("");
        }

        collection.insertOne(doc);

        session.transfer(flowFile, REL_SUCCESS);
    } catch (Exception e) {
        getLogger().error("Failed to insert {} into MongoDB due to {}", new Object[] {flowFile, e}, e);
        session.transfer(flowFile, REL_FAILURE);
        context.yield();
    }
  }

  private void transferToRelationship(ProcessSession session,
                                      FlowFile flowFile,
                                      Relationship relationship) {
    session.getProvenanceReporter().route(flowFile, relationship);
    session.transfer(flowFile, relationship);
  }

}
