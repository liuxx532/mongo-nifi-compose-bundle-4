package com.compose.nifi.processors;

import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
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
import org.bson.*;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import sun.awt.image.BadDepthException;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by liuguanxiong on 04/26/2020.
 */
@EventDriven
@TriggerSerially
@Tags({"compose", "mongodb", "put","bulkWrite"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Puts Documents and Operations into a MongoDB. Relies on attributes to decide on type of operation such as insert, update, or delete.")
public class OplogBulkWriteMongo extends AbstractProcessor {
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
  private String db;

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
    db =  mongoWrapper.getDatabaseName(context);
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
        getLogger().info("jsonArray size: " + jsonArray.size());
        excuteOperation(collection,jsonArray);

        session.transfer(flowFile, REL_SUCCESS);
    } catch (Exception e) {
        getLogger().error("Failed to insert {} into MongoDB due to {}", new Object[] {flowFile, e}, e);
        session.transfer(flowFile, REL_FAILURE);
        context.yield();
    }
  }

  //处理 oplog 中的 operation
  private void excuteOperation(MongoCollection<Document> collection,JSONArray jsonArray) throws Exception{
      Iterator records = jsonArray.iterator();
      List<WriteModel<Document>> documentList = new ArrayList<>();

      while (records.hasNext()) {
          JSONObject record = (JSONObject)(new JSONParser().parse(records.next().toString()));
          Document currentDoc = Document.parse(record.toJSONString());
          getLogger().info("currentDoc: " + currentDoc);
          Document oDoc = currentDoc.get("o", Document.class);
          String ns = currentDoc.get("ns", String.class);
          getLogger().info("ns: " + ns);
          String nsPrefix = currentDoc.get("ns", String.class).split("\\.")[0];

          getLogger().info("db: " + db);
          getLogger().info("nsPrefix: " + nsPrefix);

          //如果不等于配置的数据库 而且不等于admin.$cmd，则跳过
          if (!db.equals(nsPrefix) && !("admin.$cmd").equals(ns)) {
              continue;
          }

          String operation = currentDoc.getString("op");
          ObjectId id;
          Object lsid = currentDoc.get("lsid");
          getLogger().info("oDoc: " + oDoc);

          if (!("admin.$cmd").equals(ns)) {
              id = currentDoc.get("o2", Document.class) == null ?
                      currentDoc.get("o", Document.class).getObjectId("_id") :
                      currentDoc.get("o2", Document.class).getObjectId("_id");
              oDoc.remove("ui");
              documentList.add(processOpt(operation,oDoc,id));
          } else {
              ArrayList applyOps = oDoc.get("applyOps",ArrayList.class);
              Iterator transOps = applyOps.iterator();
              while (transOps.hasNext()) {
                  Document transOp = (Document)transOps.next();
                  getLogger().info("transOp: " + transOp);
                  transOp.remove("ui");
                  String op = transOp.getString("op");
                  Document transDoc = transOp.get("o",Document.class);
                  id = transDoc.getObjectId("_id");

                  documentList.add(processOpt(op,transDoc,id));
              }
          }

      }

      getLogger().info("documentList size:" + documentList.size());
      getLogger().info("documentList: " + documentList);

      if (!documentList.isEmpty()) {
          BulkWriteResult bulkWriteResult = collection.bulkWrite(documentList);
          if (bulkWriteResult.wasAcknowledged()) {
              getLogger().info(
                      "Wrote batch with {} inserts, {} updates and {} deletes",
                      new Object[]{bulkWriteResult.getInsertedCount(),
                      bulkWriteResult.getModifiedCount(),
                      bulkWriteResult.getDeletedCount()}
              );
          }
      }
  }

  private WriteModel<Document> processOpt(String operation,Document oDoc,ObjectId id)
          throws BadOperationException {
      Document updateKey = new Document();
      updateKey.put("_id", id);

      switch(operation) {
          case "i":
              return new InsertOneModel<>(oDoc);
          case "d":
              return new DeleteOneModel<>(oDoc);
          case "u":
              oDoc.remove("_id");
              oDoc.remove("$v");
              return new UpdateOneModel(updateKey, oDoc,new UpdateOptions().upsert(true));
          default:
              throw new BadOperationException("Unhandled operation");
      }
  }

  private void transferToRelationship(ProcessSession session,
                                      FlowFile flowFile,
                                      Relationship relationship) {
    session.getProvenanceReporter().route(flowFile, relationship);
    session.transfer(flowFile, relationship);
  }

}
